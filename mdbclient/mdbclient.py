import json
import urllib.parse

import aiohttp


class AggregateGoneException(Exception):
    pass


class BadRequest(Exception):
    def __init__(self, message):
        self.message = message


class HttpRequestException(Exception):
    def __init__(self, message, status):
        self.message = message
        self.status = status


class Http404(Exception):
    def __init__(self, message):
        self.message = message


class ApiResponseParser:
    @staticmethod
    def is_successful(http_status):
        return http_status < 400

    @staticmethod
    def reference_value(meo, ref_type):
        found = ApiResponseParser.reference_values(meo, ref_type)
        if not found:
            return
        if len(found) > 1:
            raise Exception(f"Multiple refs of type {ref_type} in {ApiResponseParser.self_link(meo)}")
        return found[0]["reference"]

    @staticmethod
    def reference_values(meo, ref_type):
        references = meo.get("references", [])
        return [x for x in references if x.get("type") == ref_type]

    @staticmethod
    def link(owner, rel):
        links = owner.get("links", [])
        rel_ = ApiResponseParser.find_link(links, rel)
        rel_item = next(iter(rel_), None)
        if not rel_item:
            raise Exception(f"could not find {rel} in {owner}")
        return rel_item["href"]

    @staticmethod
    def links_of_sub_type(links_list, sub_type):
        return [l for l in links_list if l.get("subType") == sub_type]

    @staticmethod
    def child_links_of_sub_type(owner, child_name, sub_type):
        links_list = owner.get(child_name, [])
        return ApiResponseParser.links_of_sub_type(links_list, sub_type)

    @staticmethod
    def link_of_sub_type(links, sub_type):
        rel_ = ApiResponseParser.links_of_sub_type(links, sub_type)
        rel_item = next(iter(rel_), None)
        if not rel_item:
            raise Exception(f"could not find link of subType={sub_type}")
        return ApiResponseParser.self_link(rel_item)

    @staticmethod
    def find_link(links_list, link_type, sub_type=None):
        rel_ = [l for l in links_list if l["rel"] == link_type and (not sub_type or l["subType"] == sub_type)]
        return rel_

    @staticmethod
    def timeline_of_sub_type(master_eo, sub_type):
        rel_ = [l for l in master_eo["timelines"] if
                l.get("type") == "http://id.nrk.no/2017/mdb/types/Timeline" and l.get("subType") == sub_type]
        return next(iter(rel_), None)

    @staticmethod
    def self_link(owner):
        return ApiResponseParser.link(owner, "self")


class StandardResponse(object):
    def __init__(self, response, status, location=None):
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

    def __init__(self):
        self.session = None
        self.traffic = []

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
        return self

    async def __aexit__(self, *err):
        await self.close()

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    @staticmethod
    async def __unpack_ct(response):
        if response.content_type == "application/json":
            return await response.json()
        return await response.text()

    @staticmethod
    async def __unpack_json_response(response) -> StandardResponse:
        if response.status == 400:
            raise BadRequest(await RestApiUtil.__unpack_ct(response))
        if response.status == 404:
            raise Http404(None)
        if response.status == 410:
            raise AggregateGoneException
        if response.status >= 400:
            raise HttpRequestException(await RestApiUtil.__unpack_ct(response), response.status)

        try:
            return StandardResponse(await RestApiUtil.__unpack_ct(response), response.status)
        except Exception as e:
            print(e)
            raise e

    async def http_get(self, uri, headers=None, uri_params=None):
        async with self.session.get(uri, params=uri_params, headers=headers) as response:
            return await RestApiUtil.__unpack_json_response(response)

    async def delete(self, uri, headers):
        async with self.session.delete(uri, headers=headers) as response:
            return await RestApiUtil.__unpack_json_response(response)

    # @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
    async def http_post_follow(self, uri, json_payload, headers=None):
        log_msg = f"POST TO {uri}\n{json.dumps(json_payload, indent=4, sort_keys=True)}"
        self.traffic.append(log_msg)
        async with self.session.post(uri, json=json_payload, headers=headers) as response:
            if response.status >= 400:
                raise Exception(
                    f"Status was {response.status}: {await response.text()}, request to {uri} was {json_payload}")
            reloaded = await self.follow(response, headers)
            self.traffic.append(reloaded)
            return reloaded

    # @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
    async def http_post(self, uri, json_payload, headers=None):
        log_msg = f"POST TO {uri}\n{json.dumps(json_payload, indent=4, sort_keys=True)}"
        self.traffic.append(log_msg)
        async with self.session.post(uri, json=json_payload, headers=headers) as response:
            if response.status >= 400:
                raise Exception(
                    f"Status was {response.status}: {await response.text()}, request to {uri} was {json_payload}")
            firstlevel_response = await RestApiUtil.__unpack_json_response(response)
            self.traffic.append(firstlevel_response)
            return firstlevel_response

    async def put(self, uri, json_payload, headers=None):
        async with self.session.put(uri, json=json_payload, headers=headers) as response:
            result = await RestApiUtil.__unpack_json_response(response)
            return result

    async def follow(self, response, headers=None):
        loc = response.headers["Location"]
        return await self.http_get(loc, headers)


def _res_id(mdb_object):
    return {"resId": mdb_object["resId"]}


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

    def __init__(self, api_base, user_id, correlation_id, source_system=None,
                 batch_id=None, force_host=None, force_scheme=None):
        self._headers = {}
        if source_system:
            self._headers["X-Source-System"] = source_system
        if user_id:
            self._headers["X-userId"] = user_id
        if correlation_id:
            self._headers["X-transactionId"] = correlation_id
        if batch_id:
            self._headers["X-Batch-Identifier"] = batch_id

        self.force_host = force_host
        self.force_scheme = force_scheme
        parsed = urllib.parse.urlparse(api_base)
        self.api_base = parsed.scheme + "://" + parsed.netloc + "/api"

    async def __aenter__(self):
        self.rest_api_util = RestApiUtil()
        await self.rest_api_util.__aenter__()
        return self

    async def __aexit__(self, *err):
        await self.rest_api_util.__aexit__()
        self.rest_api_util = None

    def __api_method(self, sub_path):
        return self.api_base + "/" + sub_path

    def add_header(self, key, value):
        self._headers[key] = value

    async def _invoke_get_method(self, name, parameters) -> StandardResponse:
        real_method = self.__api_method(name)
        return await self.rest_api_util.http_get(real_method, self._headers, parameters)

    async def _invoke_create_method(self, method_name, payload) -> StandardResponse:
        real_method = self.__api_method(method_name)
        stdresponse = await self.rest_api_util.http_post_follow(real_method, payload, self._headers)
        return stdresponse.response

    def _rewritten_link(self, link):
        if not self.force_host:
            return link
        parsed = urllib.parse.urlparse(link)
        # noinspection PyProtectedMember
        replaced = parsed._replace(netloc=self.force_host, scheme="http")
        return replaced.geturl()


    async def _add_on_rel(self, owner, rel, payload):
        link = self._rewritten_link(ApiResponseParser.link(owner, rel))
        posted = await self.rest_api_util.http_post(link, payload, self._headers)
        return posted.response

    async def open_url(self, url, additional_headers=None):
        headers_to_use = {**self._headers, **additional_headers} if additional_headers else self._headers
        response = await self.rest_api_util.http_get(self._rewritten_link(url), headers_to_use)
        if not response.is_successful():
            raise Exception(f"Http {response.status} for {url}:\n{response}")
        return response


class MdbClient(MdbJsonApi):
    def __init__(self, api_base, user_id, correlation_id=None, source_system=None):
        super().__init__(api_base, correlation_id, user_id, source_system)

    @staticmethod
    def localhost(user_id, correlation_id=None):
        return MdbClient("http://localhost:22338", user_id, correlation_id)

    @staticmethod
    def dev(user_id, correlation_id=None):
        return MdbClient("http://mdbklippdev.felles.ds.nrk.no", user_id, correlation_id)

    @staticmethod
    def stage(user_id, correlation_id=None):
        return MdbClient("http://mdbklippstage.felles.ds.nrk.no", user_id, correlation_id)

    @staticmethod
    def prod(user_id, correlation_id=None):
        return MdbClient("http://mdbklipp.felles.ds.nrk.no", user_id, correlation_id)

    async def __aenter__(self):
        self.rest_api_util = RestApiUtil()
        await super().__aenter__()
        return self

    async def create_master_eo(self, master_eo):
        return await self._invoke_create_method("masterEO", master_eo)

    async def create_media_object(self, master_eo, media_object):
        media_object["masterEO"] = _res_id(master_eo)
        return await self._invoke_create_method("mediaObject", media_object)

    async def create_media_resource(self, media_object, media_resource):
        media_resource["mediaObject"] = _res_id(media_object)
        return await self._invoke_create_method("mediaResource", media_resource)

    async def create_essence(self, publication_media_object, media_resource, essence):
        essence["composedOf"] = _res_id(media_resource)
        essence["playoutOf"] = _res_id(publication_media_object)
        return await self._invoke_create_method("essence", essence)

    async def create_timeline(self, master_eo, timeline):
        timeline["masterEO"] = _res_id(master_eo)
        return await self._invoke_create_method("timeline", timeline)

    async def replace_timeline(self, master_eo, existing_timeline, timeline):
        timeline["masterEO"] = _res_id(master_eo)
        self_link = ApiResponseParser.self_link(existing_timeline)
        resp, status = await self.rest_api_util.put(self_link, timeline,
                                                    self._headers)  # ApiResponseParser.verbatim_response
        return resp

    async def add_subject(self, owner, subjects):
        return await self._add_on_rel(owner, "http://id.nrk.no/2016/mdb/relation/subjects", subjects)

    async def create_or_replace_timeline(self, master_eo, timeline):
        type_of_timeline = timeline["Type"]
        existing_timeline_of_same_type = self._timelines_of_subtype(master_eo, type_of_timeline)
        if existing_timeline_of_same_type:
            return await self.replace_timeline(master_eo, existing_timeline_of_same_type, timeline)
        else:
            return await self._invoke_create_method("timeline", timeline)

    async def add_timeline_item(self, timeline, item):
        return await self._add_on_rel(timeline, "http://id.nrk.no/2016/mdb/relation/items", item)

    async def create_publication_event(self, master_eo, publication_event):
        if not publication_event:
            raise Exception("Cannot create an empty publication event")
        publication_event["publishes"] = _res_id(master_eo)
        publication_event["subType"] = "http://authority.nrk.no/datadictionary/broadcast"
        return await self._invoke_create_method("publicationEvent", publication_event)

    async def create_publication_media_object(self, publication_event, media_object, publication_media_object):
        publication_media_object["publicationEvent"] = _res_id(publication_event)
        publication_media_object["publishedVersionOf"] = _res_id(media_object)
        return await self._invoke_create_method("publicationMediaObject", publication_media_object)

    async def resolve(self, res_id):
        if not res_id:
            return
        return await self._invoke_get_method("resolve", {'resId': res_id})

    async def reference(self, ref_type, value):
        return await self._invoke_get_method("references", {'type': ref_type, 'reference': value})

    async def reference_single(self, ref_type, value):
        resp = await self._invoke_get_method("references", {'type': ref_type, 'reference': value})
        if resp.response:
            if len(resp.response) > 1:
                raise Exception(f"Multiple elements found when resolving {ref_type}={value}:{resp}")
            return await self.open(resp.response[0])
        return resp

    @staticmethod
    def _timelines_of_subtype(master_eo, sub_type):
        item = (tl for tl in master_eo["timelines"] if tl["subType"] == sub_type)
        return next(item, None)

    async def find_serie(self, title, master_system):
        resp = await self._invoke_get_method("serie/by_title", {'title': title, 'masterSystem': master_system})
        return resp.response.serie[0] if resp.response.serie else None

    async def create_serie(self, title, master_system):
        payload = {"title": title, "masterSystem": master_system}
        return await self._invoke_create_method("serie", payload)

    async def create_serie_2(self, payload):
        return await self._invoke_create_method("serie", payload)

    async def create_season(self, season):
        return await self._invoke_create_method("season", season)

    async def create_episode(self, season_id, episode):
        return await self._invoke_create_method(f"serie/{season_id}/episode", episode)

    async def delete(self, owner, additional_headers=None):
        link = self._rewritten_link(ApiResponseParser.self_link(owner))
        headers_to_use = {**self._headers, **additional_headers} if additional_headers else self._headers
        deleted = await self.rest_api_util.delete(link, headers_to_use)
        return deleted.response

    async def open(self, owner, additional_headers=None):
        link = self._rewritten_link(ApiResponseParser.self_link(owner))
        headers_to_use = {**self._headers, **additional_headers} if additional_headers else self._headers
        reloaded = await self.rest_api_util.http_get(link, headers_to_use)
        return reloaded.response


    async def update(self, owner, updates):
        link = self._rewritten_link(ApiResponseParser.self_link(owner))
        updated = await self.rest_api_util.http_post_follow(link, updates, self._headers)
        return updated.response

