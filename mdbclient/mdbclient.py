import urllib.parse

import backoff
from aiohttp import ClientSession

from mdbclient.relations import REL_ITEMS, REL_DOCUMENTS


class AggregateGoneException(Exception):
    pass


class BadRequest(Exception):
    def __init__(self, uri, request_payload, response):
        self.message = response
        self.uri = uri
        self.request_payload = request_payload


class HttpReqException(Exception):
    def __init__(self, uri, request_payload, message, status):
        self.uri = uri
        self.request_payload = request_payload
        self.message = message
        self.status = status


class Http404(Exception):
    def __init__(self, uri, message):
        self.uri = uri
        self.message = message


class Conflict(Exception):
    def __init__(self, uri, request_payload, message):
        self.uri = uri
        self.request_payload = request_payload
        self.message = message


def _reference_values(meo, ref_type):
    references = meo.get("references", [])
    return [x for x in references if x.get("type") == ref_type]


def _links_of_sub_type(links_list, sub_type):
    return [l for l in links_list if l.get("subType") == sub_type]


def _child_links_of_sub_type(owner, child_name, sub_type):
    links_list = owner.get(child_name, [])
    return _links_of_sub_type(links_list, sub_type)


def _link(owner, rel):
    links = owner.get("links", [])
    rel_ = [l for l in links if l["rel"] == rel]
    rel_item = next(iter(rel_), None)
    if not rel_item:
        raise Exception(f"could not find {rel} in {owner}")
    return rel_item["href"]


def _self_link(owner):
    return _link(owner, "self")


class Timeline(dict):
    TIMELINE_ITEMTYPE_EXTRACTEDVERSIONTIMELINEITEM = \
        'http://id.nrk.no/2017/mdb/timelineitem/ExtractedVersionTimelineItem'
    TIMELINE_ITEMTYPE_EXPLOITATIONISSUETIMELINEITEM = \
        'http://id.nrk.no/2017/mdb/timelineitem/ExploitationIssueTimelineItem'
    TIMELINE_ITEMTYPE_GENERALRIGHTS = 'http://id.nrk.no/2017/mdb/timelineitem/GeneralRightsTimelineItem'
    TIMELINE_ITEMTYPE_INDEXPOINTTIMELINEITEM = 'http://id.nrk.no/2017/mdb/timelineitem/IndexpointTimelineItem'
    TIMELINE_ITEMTYPE_INTERNALTIMELINEITEM = 'http://id.nrk.no/2017/mdb/timelineitem/InternalTimelineItem'
    TIMELINE_ITEMTYPE_TECHNICALTIMELINEITEM = 'http://id.nrk.no/2017/mdb/timelineitem/TechnicalTimelineItem'

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def filter_items(self, predicate):
        return [x for x in self.get("items", []) if predicate(x)]

    def find_item(self, resid):
        resid_ = [x for x in self.get("items", []) if x.get("resId") == resid]
        if len(resid_) == 1:
            return resid_[0]
        if len(resid_) > 1:
            raise Exception(f"Multiple elements found for {resid}")

    def find_index_points_by_title_and_offset(self, title, offset):
        return [x for x in self.get("items", []) if x["title"] == title and x["offset"] == offset]

    def find_index_point_by_title_and_offset(self, title, offset):
        matching = self.find_index_points_by_title_and_offset(title, offset)
        if len(matching) > 1:
            raise Exception(f"More than one index point found for title={title} offset={offset}")
        if matching:
            return matching[0]

    def find_index_points_by_offset_and_duration(self, offset, duration):
        return [x for x in self.get('items', []) if x['offset'] == offset and x['duration'] == duration]

    def find_index_point_by_offset_and_duration(self, offset, duration):
        matching = self.find_index_points_by_offset_and_duration(offset, duration)
        if len(matching) > 1:
            raise Exception(f"More than one index point found for offset={offset} duration={duration}")
        if matching:
            return matching[0]


class RightsTimeline(Timeline):
    TYPE = "http://id.nrk.no/2017/mdb/timelinetype/Rights"

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self["type"] = self.TYPE


class IndexpointTimeline(Timeline):
    TYPE = "http://id.nrk.no/2017/mdb/timelinetype/IndexPoints"

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self["type"] = self.TYPE


class GenealogyTimeline(Timeline):
    TYPE = "http://id.nrk.no/2017/mdb/timelinetype/Genealogy"

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self["type"] = self.TYPE


class GenealogyRightsTimeline(Timeline):
    TYPE = "http://id.nrk.no/2017/mdb/timelinetype/GenealogyRights"

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self["type"] = self.TYPE


class TechnicalTimeline(Timeline):
    TYPE = "http://id.nrk.no/2017/mdb/timelinetype/Technical"

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self["type"] = self.TYPE


class InternalTimeline(Timeline):
    TYPE = "http://id.nrk.no/2017/mdb/timelinetype/Internal"

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self["type"] = self.TYPE


class EditorialObject(dict):
    def reference_values(self, ref_type):
        return _reference_values(self, ref_type)

    def reference_value(self, ref_type):
        found = self.reference_values(ref_type)
        if not found:
            return
        if len(found) > 1:
            raise Exception(f"Multiple refs of type {ref_type} in {_self_link(self)}")
        return found[0]["reference"]

    def self_link(self):
        return _self_link(self)

    def link(self, rel):
        return _link(self, rel)


class MasterEO(EditorialObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def _timeline_of_sub_type(self, sub_type):
        rel_ = [l for l in self["timelines"] if
                l.get("type") == "http://id.nrk.no/2017/mdb/types/Timeline" and l.get("subType") == sub_type]
        return next(iter(rel_), None)

    def timeline_of_sub_type(self, sub_type):
        return self._timeline_of_sub_type(sub_type)

    def media_objects_of_sub_type(self, sub_type):
        return _child_links_of_sub_type(self, "mediaObjects", sub_type)


class MediaObject(dict):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)


class PublicationMediaObject(dict):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)


class MediaResource(dict):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)


class Essence(dict):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)


class PublicationEvent(EditorialObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)


def create_response(response):
    type_ = response.get("type")
    if type_ == "http://id.nrk.no/2016/mdb/types/MasterEditorialObject":
        return MasterEO(response)
    if type_ == "http://id.nrk.no/2016/mdb/types/MediaObject":
        return MediaObject(response)
    if type_ == "http://id.nrk.no/2016/mdb/types/PublicationMediaObject":
        return PublicationMediaObject(response)
    if type_ == "http://id.nrk.no/2016/mdb/types/MediaResource":
        return MediaResource(response)
    if type_ == "http://id.nrk.no/2016/mdb/types/Essence":
        return Essence(response)
    if type_ == "http://id.nrk.no/2016/mdb/types/PublicationEvent":
        return PublicationEvent(response)
    if type_ == InternalTimeline.TYPE:
        return InternalTimeline(response)
    if type_ == GenealogyTimeline.TYPE:
        return GenealogyTimeline(response)
    if type_ == IndexpointTimeline.TYPE:
        return IndexpointTimeline(response)
    if type_ == TechnicalTimeline.TYPE:
        return TechnicalTimeline(response)
    if type_ == RightsTimeline.TYPE:
        return RightsTimeline(response)
    if type_ == GenealogyRightsTimeline.TYPE:
        return GenealogyRightsTimeline(response)
    raise Exception(f"Dont know how to create response for {type_}")


class StandardResponse(object):
    def __init__(self, response: dict, status, location=None):
        self.response = response
        self.status = status
        self.location = location

    def __iter__(self):
        for i in [self.response, self.status]:
            yield i

    def is_successful(self):
        return self.status < 400


class FollowedResponse(StandardResponse):
    def __init__(self, response, status, followed):
        StandardResponse.__init__(self, response, status)
        self.followed = followed

    def __iter__(self):
        for i in [self.response, self.status, self.followed]:
            yield i


# server scope. Has no request specific state
# Use https://pypi.org/project/backoff-async/ to handle retries
class RestApiUtil(object):

    def __init__(self, session: ClientSession):
        self.session = session

    @staticmethod
    async def __unpack_response_content(response):
        if response.content_type == "application/json":
            return await response.json()
        return await response.text()

    @staticmethod
    async def __raise_errors(response, uri, request_payload):
        if response.status == 400:
            raise BadRequest(uri, request_payload, await RestApiUtil.__unpack_response_content(response))
        if response.status == 409:
            raise Conflict(uri, request_payload, await RestApiUtil.__unpack_response_content(response))
        if response.status == 404:
            raise Http404(uri, None)
        if response.status == 410:
            raise AggregateGoneException
        if response.status >= 400:
            raise HttpReqException(uri, request_payload, await RestApiUtil.__unpack_response_content(response),
                                   response.status)

    @staticmethod
    async def __unpack_json_response(response, request_uri, request_payload=None) -> StandardResponse:
        await RestApiUtil.__raise_errors(response, request_uri, request_payload)
        return StandardResponse(await RestApiUtil.__unpack_response_content(response), response.status)

    async def http_get(self, uri, headers=None, uri_params=None) -> StandardResponse:
        async with self.session.get(uri, params=uri_params, headers=headers) as response:
            return await RestApiUtil.__unpack_json_response(response, uri)

    async def delete(self, uri, headers) -> StandardResponse:
        async with self.session.delete(uri, headers=headers) as response:
            return await RestApiUtil.__unpack_json_response(response, uri)

    # @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
    async def http_post_follow(self, uri, json_payload, headers=None) -> StandardResponse:
        async with self.session.post(uri, json=json_payload, headers=headers) as response:
            await RestApiUtil.__raise_errors(response, uri, json_payload)
            reloaded = await self.follow(response, headers)
            return reloaded

    # @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
    async def http_post(self, uri, json_payload, headers=None) -> StandardResponse:
        async with self.session.post(uri, json=json_payload, headers=headers) as response:
            firstlevel_response = await RestApiUtil.__unpack_json_response(response, uri, json_payload)
            return firstlevel_response

    async def http_post_form(self, uri, dict_payload, headers=None) -> StandardResponse:
        async with self.session.post(uri, data=dict_payload, headers=headers) as response:
            firstlevel_response = await RestApiUtil.__unpack_json_response(response, uri, dict_payload)
            return firstlevel_response

    async def put(self, uri, json_payload, headers=None) -> StandardResponse:
        async with self.session.put(uri, json=json_payload, headers=headers) as response:
            result = await RestApiUtil.__unpack_json_response(response, uri, json_payload)
            return result

    async def follow(self, response, headers=None) -> StandardResponse:
        loc = response.headers["Location"]
        return await self.http_get(loc, headers)


def _res_id(mdb_object):
    return {"resId": mdb_object["resId"]}


def _check_if_lock(exc: HttpReqException):
    if not isinstance(exc.message, dict):
        return False
    type_ = exc.message.get("type")
    return type_ == 'LockAcquisitionFailedException' or type_ == 'DeadlockException'


def _check_if_not_lock(exc: HttpReqException):
    return not _check_if_lock(exc)


class MdbJsonApi(object):
    """
    Knows how to work with mdb hyperlinked json objects. Calls mdbclient in a responsible manner with
    all the correct headers.

    Knows how to rewrite all urls to use a host different from the one in the supplied url, using param force_host

    Does not handle http error codes, clients that need to interpret error codes will need to
    use a custom response_unpacker with this class. This response_unpacker should probably raise exceptions.

    'owner' in method names in this class refer to some json object with a "links" collection.

    This class is unaware of the url of the remote system since it's all in the hyperlinked payloads.
    """

    def __init__(self, user_id, correlation_id, session: ClientSession = None, source_system=None, batch_id=None,
                 force_host=None, force_scheme=None):
        self._global_headers = {}
        if source_system:
            if not isinstance(source_system, str):
                raise TypeError("source_system is not a string: " + type(source_system))
            self._global_headers["X-Source-System"] = source_system
        if user_id:
            if not isinstance(user_id, str):
                raise TypeError("user_id is not a string: " + type(user_id))
            self._global_headers["X-userId"] = user_id
        if correlation_id:
            if not isinstance(correlation_id, str):
                raise TypeError("correlation_id is not a string: " + type(correlation_id))
            self._global_headers["X-transactionId"] = correlation_id
        if batch_id:
            if not isinstance(batch_id, str):
                raise TypeError("batch_id is not a string: " + type(batch_id))
            self._global_headers["X-Batch-Identifier"] = batch_id

        self.force_host = force_host
        self.force_scheme = force_scheme
        self.rest_api_util = RestApiUtil(session)

    def add_global_header(self, key, value):
        if not isinstance(value, str):
            raise TypeError(f"header value for {key} value is not a string: {type(value)}")

        self._global_headers[key] = value

    def _merged_headers(self, request_headers):
        return {**self._global_headers, **request_headers} if request_headers else self._global_headers

    async def _do_post(self, link, payload, headers=None) -> {}:
        posted = await self.rest_api_util.http_post(link, payload, self._merged_headers(headers))
        return posted.response

    async def _do_put(self, link, payload, headers=None) -> {}:
        resp = await self.rest_api_util.put(link, payload, self._merged_headers(headers))
        return resp.response

    async def _do_delete(self, link, headers=None) -> {}:
        deleted = await self.rest_api_util.delete(link, self._merged_headers(headers))
        return deleted.response

    async def _do_get(self, link, headers=None) -> {}:
        reloaded = await self.rest_api_util.http_get(link, self._merged_headers(headers))
        return reloaded.response

    async def _do_post_follow(self, link, updates, headers=None) -> {}:
        updated = await self.rest_api_util.http_post_follow(link, updates, self._merged_headers(headers))
        return updated.response

    def _rewritten_link(self, link):
        if not self.force_host:
            return link
        parsed = urllib.parse.urlparse(link)
        # noinspection PyProtectedMember
        replaced = parsed._replace(netloc=self.force_host, scheme="http")
        return replaced.geturl()

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def open_url(self, url, headers=None):
        response = await self.rest_api_util.http_get(self._rewritten_link(url), self._merged_headers(headers))
        if not response.is_successful():
            raise Exception(f"Http {response.status} for {url}:\n{response}")
        return response


class MdbJsonMethodApi(MdbJsonApi):
    """
    A jason api that knows how to invoke method calls directly by name. Requires a server addreess (api_base)
    """

    def __init__(self, api_base, user_id, correlation_id, session: ClientSession = None, source_system=None,
                 batch_id=None,
                 force_host=None, force_scheme=None):
        MdbJsonApi.__init__(self, user_id, correlation_id, session, source_system, batch_id, force_host, force_scheme)
        parsed = urllib.parse.urlparse(api_base)
        self.api_base = parsed.scheme + "://" + parsed.netloc + "/api"

    def __api_method(self, sub_path):
        return self.api_base + "/" + sub_path

    def _api_method(self, sub_path):
        return self.api_base + "/" + sub_path

    async def _invoke_get_method(self, name, parameters, headers=None) -> dict:
        real_method = self.__api_method(name)
        parameters_ = await self.rest_api_util.http_get(real_method, self._merged_headers(headers), parameters)
        return parameters_.response

    async def _invoke_create_method(self, method_name, payload, headers=None) -> {}:
        real_method = self.__api_method(method_name)
        stdresponse = await self.rest_api_util.http_post_follow(real_method, payload, self._merged_headers(headers))
        return stdresponse.response


class MdbClient(MdbJsonMethodApi):
    def __init__(self, api_base, user_id, correlation_id, session: ClientSession = None, source_system=None):
        MdbJsonMethodApi.__init__(self, api_base, user_id, correlation_id, session, source_system)

    @staticmethod
    def localhost(user_id, session: ClientSession = None, correlation_id=None):
        return MdbClient("http://localhost:22338", user_id, correlation_id, session)

    @staticmethod
    def dev(user_id, session: ClientSession = None, correlation_id=None):
        return MdbClient("http://mdbklippdev.felles.ds.nrk.no", user_id, correlation_id, session)

    @staticmethod
    def stage(user_id, session: ClientSession = None, correlation_id=None):
        return MdbClient("http://mdbklippstage.felles.ds.nrk.no", user_id, correlation_id, session)

    @staticmethod
    def prod(user_id, session: ClientSession = None, correlation_id=None):
        return MdbClient("http://mdbklipp.felles.ds.nrk.no", user_id, correlation_id, session)

    async def __add_on_rel(self, owner, rel, payload, headers=None):
        link = self._rewritten_link(_link(owner, rel))
        return await self._do_post(link, payload, headers)

    async def __replace_content(self, owner, payload, headers=None) -> dict:
        link = self._rewritten_link(_self_link(owner))
        return await self._do_put(link, payload, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_master_eo(self, master_eo, headers=None) -> MasterEO:
        return create_response(await self._invoke_create_method("masterEO", master_eo, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_media_object(self, master_eo, media_object, headers=None) -> MediaObject:
        media_object["masterEO"] = _res_id(master_eo)
        return create_response(await self._invoke_create_method("mediaObject", media_object, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_media_resource(self, media_object, media_resource, headers=None) -> MediaResource:
        media_resource["mediaObject"] = _res_id(media_object)
        return create_response(await self._invoke_create_method("mediaResource", media_resource, headers=headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_essence(self, publication_media_object, media_resource, essence, headers=None) -> Essence:
        essence["composedOf"] = _res_id(media_resource)
        essence["playoutOf"] = _res_id(publication_media_object)
        return create_response(await self._invoke_create_method("essence", essence, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_timeline(self, master_eo, timeline, headers=None, shallow=False) -> Timeline:
        timeline["masterEO"] = _res_id(master_eo)
        items = None
        if shallow:
            items = timeline.get("items")
            del timeline["items"]
        try:
            return create_response(await self._invoke_create_method("timeline", timeline, headers))
        finally:
            if shallow and items:
                timeline["items"] = items

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def replace_timeline(self, master_eo, existing_timeline, timeline, headers=None) -> Timeline:
        timeline["masterEO"] = _res_id(master_eo)
        return create_response(await self.__replace_content(existing_timeline, timeline, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_subject(self, owner, subjects, headers=None):
        return await self.__add_on_rel(owner, "http://id.nrk.no/2016/mdb/relation/subjects", subjects, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def broadcast_change(self, destination, resid, type_, headers=None):
        payload = {
            "destination": destination,
            "resId": resid,
            "type": type_
        }
        '''
        {
          "resId" : "http://id.nrk.no/2017/mdb/timeline/963df4ef-d884-410b-bdf4-efd884a10bf3",
          "changeType" : "NOOP",
          "aggregateType" : "http://id.nrk.no/2017/mdb/types/Timeline",
          "resolve" : "http://mdbklippstage.felles.ds.nrk.no/api/resolve/http%3A%2F%2Fid.blabla"
        }'''
        real_method = self._api_method("changes/by-resid")
        stdresponse = await self.rest_api_util.http_post_form(real_method, payload, self._merged_headers(headers))
        return stdresponse.response

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_or_replace_timeline(self, master_eo, timeline, headers=None) -> Timeline:
        type_of_timeline = timeline["Type"]
        existing_timeline_of_same_type = self._timelines_of_subtype(master_eo, type_of_timeline)
        if existing_timeline_of_same_type:
            return create_response(
                await self.replace_timeline(master_eo, existing_timeline_of_same_type, timeline, headers))
        else:
            return create_response(await self._invoke_create_method("timeline", timeline, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_timeline_item(self, timeline, item, headers=None):
        return await self.__add_on_rel(timeline, REL_ITEMS, item, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_stored_document(self, master_eo, stored_document, headers=None):
        return await self.__add_on_rel(master_eo, REL_DOCUMENTS, stored_document,
                                       headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_publication_event(self, master_eo, publication_event, headers=None) -> PublicationEvent:
        if not publication_event:
            raise Exception("Cannot create an empty publication event")
        publication_event["publishes"] = _res_id(master_eo)
        publication_event["subType"] = "http://authority.nrk.no/datadictionary/broadcast"
        return create_response(await self._invoke_create_method("publicationEvent", publication_event, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_publication_media_object(self, publication_event, media_object, publication_media_object,
                                              headers=None) -> PublicationMediaObject:
        publication_media_object["publicationEvent"] = _res_id(publication_event)
        publication_media_object["publishedVersionOf"] = _res_id(media_object)
        return create_response(
            await self._invoke_create_method("publicationMediaObject", publication_media_object, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def resolve(self, res_id, headers=None) -> dict:
        if not res_id:
            return {}
        return create_response(await self._invoke_get_method("resolve", {'resId': res_id}, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def find_media_object(self, name, headers=None) -> dict:
        try:
            return await self._invoke_get_method("mediaObject/by-name", {"name": name}, headers)
        except Http404:
            pass

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def reference(self, ref_type, value, headers=None) -> dict:
        return await self._invoke_get_method("references", {'type': ref_type, 'reference': value}, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def reference_single(self, ref_type, value, headers=None) -> dict:
        resp = await self._invoke_get_method("references", {'type': ref_type, 'reference': value}, headers)
        if resp:
            if len(resp) > 1:
                raise Exception(f"Multiple elements found when resolving {ref_type}={value}:{resp}")
            return await self.open(resp[0], headers)
        return {}

    @staticmethod
    def _timelines_of_subtype(master_eo, sub_type):
        item = (tl for tl in master_eo["timelines"] if tl["subType"] == sub_type)
        return next(item, None)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def find_serie(self, title, master_system, headers=None):
        response = await self._invoke_get_method("serie/by_title", {'title': title, 'masterSystem': master_system},
                                                 headers)
        return response.get("serie")[0] if response.get("serie") else None

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_serie(self, title, master_system, headers=None):
        payload = {"title": title, "masterSystem": master_system}
        return await self._invoke_create_method("serie", payload, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_serie_2(self, payload, headers=None):
        return await self._invoke_create_method("serie", payload, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_season(self, season, headers=None):
        return await self._invoke_create_method("season", season, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_episode(self, season_id, episode, headers=None):
        return await self._invoke_create_method(f"serie/{season_id}/episode", episode, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def delete(self, owner, headers=None):
        link = self._rewritten_link(_self_link(owner))
        return await self._do_delete(link, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def open(self, owner, headers=None):
        link = self._rewritten_link(_self_link(owner))
        return create_response(await self._do_get(link, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def update(self, owner, updates, headers=None):
        link = self._rewritten_link(_self_link(owner))
        return await self._do_post_follow(link, updates, headers)
