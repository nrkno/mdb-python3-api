import copy
import datetime
import urllib.parse
from abc import abstractmethod
from enum import Enum
from typing import Optional, Union, List, TypeVar, Generic

import backoff
from aiohttp import ClientSession, ClientResponse, ClientPayloadError, ServerDisconnectedError, ClientOSError

from mdbclient.relations import REL_ITEMS, REL_DOCUMENTS, REL_FORMATS


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
    def __init__(self, uri, message, headers=None, uri_params=None):
        self.uri_params = uri_params
        self.headers = headers
        self.uri = uri
        self.message = message


class Conflict(Exception):
    def __init__(self, uri, request_payload, message):
        self.uri = uri
        self.request_payload = request_payload
        self.message = message


def _links_of_sub_type(links_list, sub_type):
    return [x for x in links_list if x.get("subType") == sub_type]


def _child_links_of_sub_type(owner, child_name, sub_type):
    links_list = owner.get(child_name, [])
    return _links_of_sub_type(links_list, sub_type)


def _link(owner, rel):
    links = owner.get("links", [])
    rel_ = [x for x in links if x["rel"] == rel]
    rel_item = next(iter(rel_), None)
    if not rel_item:
        raise Exception(f"could not find {rel} in {owner}")
    return rel_item["href"]


def _self_link(owner):
    return _link(owner, "self")


class MdbLink:
    def __init__(self, link_node):
        self.link = link_node

    def __getitem__(self, key):
        return self.link[key]

    def rel(self):
        return self.link.get("rel")

    def type(self):
        return self.link.get("type")

    def href(self):
        return self.link.get("href")

    @staticmethod
    def create(link_node) -> 'MdbLink':
        if link_node:
            return MdbLink(link_node)


class MdbLinks:
    def __init__(self, links_node):
        self.links_node = links_node

    def __len__(self):
        return len(self.links_node)

    def select_single(self, rel):
        matching = [x for x in self.links_node if x.get("rel") == rel]
        if len(matching) > 1:
            raise Exception(f"Multiple links match rel={rel}")
        if len(matching) == 0:
            raise Exception(f"No links match rel={rel}")
        return MdbLink.create(matching[0])

    def self_link(self):
        return self.select_single(rel="self")

    @staticmethod
    def create(links_node) -> 'MdbLinks':
        if links_node:
            return MdbLinks(links_node)


T = TypeVar('T')


class ResourceReference(Generic[T]):
    def __init__(self, resource_reference):
        self.resource_reference = resource_reference

    def __getitem__(self, key):
        return self.resource_reference[key]

    def get(self, key, default=None):
        return self.resource_reference.get(key, default)

    def links(self) -> MdbLinks:
        return MdbLinks.create(self.resource_reference.get("links"))

    def is_type(self, main_type):
        return self.resource_reference.get("type") == main_type

    def is_subtype(self, sub_type):
        return self.resource_reference.get("subType") == sub_type

    @staticmethod
    def create(node) -> 'ResourceReference[T]':
        if node:
            return ResourceReference(node)


X = TypeVar('X')


class ResourceReferenceCollection(Generic[X]):
    def __init__(self, children, owner, collection_name):
        self.children = children
        self.owner = owner
        self.collection_name = collection_name

    def of_type(self, main_type) -> 'ResourceReferenceCollection[X]':
        return ResourceReferenceCollection([x for x in self.children if x.get("type") == main_type], self.owner,
                                           self.collection_name)

    def of_subtype(self, sub_type) -> 'ResourceReferenceCollection[X]':
        return ResourceReferenceCollection([x for x in self.children if x.get("subType") == sub_type], self.owner,
                                           self.collection_name)

    def first(self) -> ResourceReference[X]:
        return ResourceReference.create(self.children[0]) if self.children else None

    def __getitem__(self, key) -> ResourceReference[X]:
        return ResourceReference.create(self.children[key])

    def single(self) -> ResourceReference[X]:
        if len(self.children) > 1:
            raise Exception(f"Requested single element of {self.collection_name} from {self.owner.self_link()} "
                            f"which has multiple elements")
        if len(self.children) == 0:
            raise Exception(f"Requested single element of an empty linkcollection")
        return self.children[0]

    def single_or_none(self) -> Optional[ResourceReference[X]]:
        if len(self.children) > 1:
            raise Exception(f"Requested single element of {self.collection_name} from {self.owner.self_link()} "
                            f"which has multiple elements")
        return self.first()

    def __len__(self):
        return len(self.children)


def clone_for_create(item):
    copy_ = copy.copy(item)
    if "resId" in copy_:
        del copy_["resId"]
    if "links" in copy_:
        del copy_["links"]
    return copy_


class BasicMdbObject(dict):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self.resid = self.get("resId")

    def self_link(self):
        return _self_link(self)

    def link(self, rel):
        return _link(self, rel)

    def links(self) -> MdbLinks:
        return MdbLinks.create(self.get("links"))

    def type(self) -> str:
        return self.get("type")

    def sub_type(self) -> str:
        return self.get("subType")


class Reference(BasicMdbObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self.type: str = self.get("type")
        self.reference: str = self.get("reference")

    def int_value(self) -> Optional[int]:
        if self.reference:
            return int(self.reference)


def _reference_values(meo, ref_type) -> List[Reference]:
    references = meo.get("references", [])
    return [Reference(x) for x in references if x.get("type") == ref_type]


class Timeline(BasicMdbObject):
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
        self.timeline_items = self.get("items", [])

    def self_link(self):
        return _self_link(self)

    def filter_items(self, predicate):
        return [x for x in self.get("items", []) if predicate(x)]

    def select_items(self, *keyvalue_tuples):
        def matches_field_exps(item):
            for exp in keyvalue_tuples:
                if not item.get(exp[0]) == exp[1]:
                    return False
            return True

        res = [x for x in self.get("items", []) if matches_field_exps(x)]
        return res

    def select_single_item(self, *keyvalue_tuples):
        items = self.select_items(*keyvalue_tuples)
        if len(items) == 1:
            return items[0]
        if len(items) > 1:
            msg = ",".join([f"{x[0]}={x[1]}]" for x in keyvalue_tuples])
            raise Exception(f"Multiple elements found for {msg} in {self.resid}")

    def find_item(self, res_id):
        return self.select_single_item(("resId", res_id))

    def find_by_title(self, title):
        return self.select_single_item(("title", title))

    def find_by_description(self, description):
        return self.select_single_item(("description", description))

    def find_index_points_by_title_and_offset(self, title, offset):
        return self.select_items(("title", title), ("offset", offset))

    def find_index_point_by_title_and_offset(self, title, offset):
        return self.select_single_item(("title", title), ("offset", offset))

    def find_index_points_by_offset_and_duration(self, offset, duration):
        return self.select_items(("offset", offset), ("duration", duration))

    def find_index_point_by_offset_and_duration(self, offset, duration):
        return self.select_single_item(("offset", offset), ("duration", duration))

    def find_index_point_by_offset(self, offset):
        return self.select_single_item(("offset", offset))

    def stabilize_order(self):
        """
        Provides a guaranteed stable order of values
        """

        self.get("subjects", []).sort(key=lambda x: x["title"])
        self.get("spatials", []).sort(key=lambda x: x["name"])
        self.get("contributors", []).sort(key=lambda x: x["contact"]["title"] + x["role"]["resId"])

    def master_eo(self) -> ResourceReference['MasterEO']:
        return ResourceReference.create(self.get("masterEO"))


class RightsTimeline(Timeline):
    TYPE = "http://id.nrk.no/2017/mdb/timelinetype/Rights"

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self["type"] = self.TYPE

    def fulltimeline_item(self, type_):
        return self.select_single_item(("appliesToFullTimeline", True), ("type", type_))

    @staticmethod
    def create(items) -> 'RightsTimeline':
        return RightsTimeline({"items": items})


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

    def find_index_points_by_event(self, event_):
        return self.select_items(("event", event_))

    def find_index_points_by_event_and_offset(self, event_, offset):
        return self.select_items(("event", event_), ("offset", offset))

    def find_index_point_by_event_and_offset(self, event, offset):
        matching = self.find_index_points_by_event_and_offset(event, offset)
        if len(matching) > 1:
            raise Exception(f"More than one index point found for event={event} offset={offset}")
        if matching:
            return matching[0]


class InternalTimeline(Timeline):
    TYPE = "http://id.nrk.no/2017/mdb/timelinetype/Internal"

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self["type"] = self.TYPE

    def find_index_points_by_subtype_offset_duration(self, subtype, offset, duration):
        return self.select_items(("subType", subtype), ("offset", offset), ("duration", duration))

    def find_index_point_by_sybtype_offset_duration(self, subtype, offset, duration):
        matching = self.find_index_points_by_subtype_offset_duration(subtype, offset, duration)
        if len(matching) > 1:
            raise Exception(
                f"More than one index point found for subtype={subtype}, offset={offset} "
                f"duration={duration} in {self.self_link()}")
        if matching:
            return matching[0]


class Contributor(dict):
    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self.resId = dict_.get("resId")
        self.contact = dict_.get("contact", {})
        self.role = dict_.get("role", {})
        self.characterName = dict_.get("characterName")
        self.capacity = dict_.get("capacity")
        self.comment = dict_.get("comment")

    def key(self):
        return f"CT={self.contact.get('title')},T={self.role.get('title')},R={self.role.get('resId')},C={self.contact.get('resId')},CAP={self.capacity}"

    @staticmethod
    def unique(contributors: List['Contributor']) -> List['Contributor']:
        res = []
        seen = set()
        for x in contributors:
            if (key := x.key()) not in seen:
                seen.add(key)
                res.append(x)
        return res


class EditorialObject(BasicMdbObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)
        self.resid = self.get("resId")

    def contributors(self) -> List[Contributor]:
        contrs = self.get("contributors", [])
        return [Contributor(x) for x in contrs]

    def references(self, ref_type) -> List[Reference]:
        return _reference_values(self, ref_type)

    def reference(self, ref_type) -> Optional[Reference]:
        found = self.references(ref_type)
        if not found:
            return
        if len(found) > 1:
            raise Exception(f"Multiple refs of type {ref_type} in {_self_link(self)}")
        return found[0]

    def reference_value(self, ref_type) -> Optional[str]:
        found = self.reference(ref_type)
        if not found:
            return
        return found.reference

    def _reference_collection(self, collection_name) -> ResourceReferenceCollection:
        result = self.get(collection_name, [])
        return ResourceReferenceCollection(result, self, collection_name)


class VersionGroup(BasicMdbObject):
    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def metadata_meo(self) -> ResourceReference['MasterEO']:
        return ResourceReference.create(self.get("metadataMeo"))


class MasterEO(EditorialObject):
    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def media_objects(self) -> ResourceReferenceCollection['MediaObject']:
        return self._reference_collection("mediaObjects")

    def publications(self) -> ResourceReferenceCollection['PublicationEvent']:
        return self._reference_collection("publications")

    def timelines(self) -> ResourceReferenceCollection[Timeline]:
        return self._reference_collection("timelines")

    def version_group(self) -> ResourceReference[VersionGroup]:
        return ResourceReference.create(self.get("versionGroup"))

    def has_subject_with_title(self, subject: str, case_sensitive=True):
        if case_sensitive:
            return [sub for sub in self.get("subjects", []) if sub.get("title") == subject]
        else:
            subject = subject.lower()
            return [sub for sub in self.get("subjects", []) if sub.get("title", "").lower() == subject]


class MasterEOResource(EditorialObject):
    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)


class Essence(BasicMdbObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def composed_of(self) -> ResourceReference['MediaResource']:
        return ResourceReference.create(self.get("composedOf"))

    def playout_of(self) -> ResourceReference['PublicationMediaObject']:
        return ResourceReference.create(self.get("playoutOf"))


class MediaResource(BasicMdbObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def media_object(self) -> ResourceReference['MediaObject']:
        return ResourceReference.create(self.get("mediaObject"))

    def essences(self) -> ResourceReferenceCollection[Essence]:
        return self._reference_collection("essences")

    def matching_locators(self, identifier, storageType):
        return [x for x in self.get("locators") if
                x.get("identifier") == identifier and x.get("storageType", {}).get("resId") == storageType]


class MediaObject(BasicMdbObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def media_resources(self) -> ResourceReferenceCollection[MediaResource]:
        return self._reference_collection("resources")

    def published_versions(self) -> ResourceReferenceCollection['PublicationMediaObject']:
        return self._reference_collection("publishedVersions")

    def master_eo(self) -> ResourceReference['MasterEO']:
        return ResourceReference.create(self.get("masterEO"))


class PublicationMediaObject(BasicMdbObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def playouts(self) -> ResourceReferenceCollection[Essence]:
        return self._reference_collection("playouts")

    def published_version_of(self) -> ResourceReference[MediaObject]:
        return ResourceReference.create(self.get("publishedVersionOf"))


class PublicationEvent(EditorialObject):

    def __init__(self, dict_=..., **kwargs) -> None:
        super().__init__(dict_, **kwargs)

    def pmos(self) -> ResourceReferenceCollection:
        return self._reference_collection("pmos")


class StandardResponse(object):
    def __init__(self, requested_uri, response: dict, status, location=None):
        self.response = response
        self.status = status
        self.location = location
        self.requested_uri = requested_uri

    def __iter__(self):
        for i in [self.response, self.status]:
            yield i

    def is_successful(self):
        return self.status < 400


def create_response_from_std_response(std_response: StandardResponse) -> Union[
    MasterEO, PublicationMediaObject, MediaObject, MediaResource, Essence, PublicationEvent, InternalTimeline,
    GenealogyTimeline, IndexpointTimeline, TechnicalTimeline, RightsTimeline, GenealogyRightsTimeline,
    VersionGroup, MasterEOResource]:
    if not std_response.is_successful() or isinstance(std_response.response, str):
        raise Exception(f"Http {std_response.status} for {std_response.requested_uri}:\n{str(std_response.response)}")

    return create_response(std_response.response)


def create_response(response) -> \
        Union[
            MasterEO, PublicationMediaObject, MediaObject, MediaResource, Essence, PublicationEvent, InternalTimeline,
            GenealogyTimeline, IndexpointTimeline, TechnicalTimeline, RightsTimeline, GenealogyRightsTimeline,
            VersionGroup,MasterEOResource]:
    type_ = response.get("type")
    if not type_:
        return response

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
    if type_ == "http://id.nrk.no/2016/mdb/types/VersionGroup":
        return VersionGroup(response)
    if type_ == "http://id.nrk.no/2016/mdb/types/MasterEOResource":
        return MasterEOResource(response)
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


# server scope. Has no request specific state
# Use https://pypi.org/project/backoff-async/ to handle retries
class RestApiUtil(object):

    def __init__(self, session: ClientSession):
        self.session = session

    @staticmethod
    async def __unpack_response_content(uri, response, headers=None, uri_params=None):
        if response.status == 204:
            return
        if response.status == 202 and response.content_length == 0:
            return
        if response.content_type == "application/json":
            try:
                return await response.json()
            except ClientPayloadError as e:
                raise ClientPayloadError(
                    f"When resolving {uri} had status {response.status} and content_length={response.content_length} org {e}")
        params = " with params " + str(uri_params) if uri_params else " "
        headers_str = " with headers " + str(headers) if headers else " "
        raise Exception(
            f"Response={response.status} to {uri}{params}{headers_str} at {datetime.datetime.now().time()} is "
            f"{response.content_type}: {response.content}\n{str(response.headers)}")
        # return await response.text()

    @staticmethod
    async def __raise_errors(response, uri, request_payload, headers=None, uri_params=None):
        if response.status == 400:
            raise BadRequest(uri, request_payload,
                             await RestApiUtil.__unpack_response_content(uri, response, headers, uri_params))
        if response.status == 409:
            raise Conflict(uri, request_payload,
                           await RestApiUtil.__unpack_response_content(uri, response, headers, uri_params))
        if response.status == 404:
            raise Http404(uri, None, headers, uri_params)
        if response.status == 410:
            raise AggregateGoneException
        if response.status >= 400:
            raise HttpReqException(uri, request_payload,
                                   await RestApiUtil.__unpack_response_content(uri, response, headers, uri_params),
                                   response.status)

    @staticmethod
    async def __unpack_json_response(response, request_uri, headers=None, uri_params=None,
                                     request_payload=None) -> StandardResponse:
        await RestApiUtil.__raise_errors(response, request_uri, request_payload, headers, uri_params)
        return StandardResponse(request_uri,
                                await RestApiUtil.__unpack_response_content(request_uri, response, headers, uri_params),
                                response.status)

    async def http_get(self, uri, headers=None, uri_params=None) -> StandardResponse:
        async with self.session.get(uri, params=uri_params, headers=headers) as response:
            return await RestApiUtil.__unpack_json_response(response, uri, headers, uri_params)

    async def raw_http_get(self, uri, headers=None, uri_params=None) -> str:
        async with self.session.get(uri, params=uri_params, headers=headers) as response:
            return await response.text()

    async def http_get_text(self, uri, headers=None, uri_params=None) -> StandardResponse:
        async with self.session.get(uri, params=uri_params, headers=headers) as response:
            return await RestApiUtil.__unpack_json_response(response, uri, headers, uri_params)

    async def http_get_no_redirect(self, uri, headers=None, uri_params=None) -> ClientResponse:
        async with self.session.get(uri, params=uri_params, headers=headers, allow_redirects=False) as response:
            return response

    async def delete(self, uri, headers) -> StandardResponse:
        async with self.session.delete(uri, headers=headers) as response:
            return await RestApiUtil.__unpack_json_response(response, uri, headers)

    # @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
    async def http_post_follow(self, uri, json_payload, headers=None) -> StandardResponse:
        async with self.session.post(uri, json=json_payload, headers=headers) as response:
            await RestApiUtil.__raise_errors(response, uri, json_payload, headers)
            reloaded = await self.follow(response, headers)
            if isinstance(reloaded.response, str):
                raise HttpReqException(uri, json_payload, reloaded.response, reloaded.status)
            return reloaded

    # @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
    async def http_post(self, uri, json_payload, headers=None) -> StandardResponse:
        async with self.session.post(uri, json=json_payload, headers=headers) as response:
            firstlevel_response = await RestApiUtil.__unpack_json_response(response, uri, headers, None, json_payload)
            return firstlevel_response

    async def http_post_form(self, uri, dict_payload, headers=None) -> StandardResponse:
        async with self.session.post(uri, data=dict_payload, headers=headers) as response:
            firstlevel_response = await RestApiUtil.__unpack_json_response(response, uri, headers, None, dict_payload)
            return firstlevel_response

    async def put(self, uri, json_payload, headers=None) -> StandardResponse:
        async with self.session.put(uri, json=json_payload, headers=headers) as response:
            result = await RestApiUtil.__unpack_json_response(response, uri, headers, None, json_payload)
            return result

    async def follow(self, response, headers=None) -> StandardResponse:
        loc = response.headers["Location"]
        return await self.http_get(loc, headers)


def _res_id(mdb_object):
    return {"resId": mdb_object["resId"]}


def _check_if_lock(exc: HttpReqException):
    if not hasattr(exc, "message"):
        return False
    if not isinstance(exc.message, dict):
        return False
    type_ = exc.message.get("type")
    return type_ == 'LockAcquisitionFailedException' or type_ == 'DeadlockException'


def _check_if_not_lock(exc: HttpReqException):
    return not _check_if_lock(exc)


class MdbChangeListener:
    @abstractmethod
    def on_change(self, resId, topic, changes):
        pass

    @abstractmethod
    def on_add(self, resId, topic, add):
        pass

    @abstractmethod
    def on_create(self, resId, topic, add):
        pass

    @abstractmethod
    def on_delete(self, resId):
        pass


class VoidChangeListener(MdbChangeListener):

    def on_change(self, res_id, topic, changes):
        pass

    def on_add(self, res_id, topic, add):
        pass

    def on_create(self, res_id, topic, add):
        pass

    def on_delete(self, res_id):
        pass


class Change:
    def __init__(self, res_id, type, topic, payload):
        self.resId = res_id
        self.type = type
        self.topic = topic
        self.payload = payload

    def __str__(self) -> str:
        topic = f" {self.topic}" if self.topic else ""
        return f"{self.type}{topic} {self.resId} {self.payload}"


class RecordingChangeListener(MdbChangeListener):

    def __init__(self):
        self.changes = []

    def pop_changes(self):
        """
        Clear the changes, returning the values before clearing
        """
        res = self.changes
        self.changes = []
        return res

    def on_change(self, resId, topic, changes):
        self.changes.append(Change(resId, "CHANGE", topic, changes))
        pass

    def on_add(self, resId, topic, add):
        self.changes.append(Change(resId, "ADD", topic, add))

    def on_create(self, resId, topic, add):
        self.changes.append(Change(resId, "CREATE", topic, add))

    def on_delete(self, resId):
        self.changes.append(Change(resId, "DELETE"))


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

    def __init__(self, session: ClientSession, user_id: str, correlation_id: str, source_system: str = None,
                 batch_id: str = "default-batch-id",
                 force_host: bool = None, force_scheme: bool = None):
        self._global_headers = {}
        if source_system:
            if not isinstance(source_system, str):
                raise TypeError("source_system is not a string: " + type(source_system))
            self._global_headers["X-Source-System"] = source_system
        if user_id:
            if not isinstance(user_id, str):
                raise TypeError(f"user_id is not a string: {type(user_id)}")
            self._global_headers["X-userId"] = user_id
        if correlation_id:
            if not isinstance(correlation_id, str):
                raise TypeError(f"correlation_id is not a string: {type(correlation_id)}")
            self._global_headers["X-transactionId"] = correlation_id
        if batch_id:
            if not isinstance(batch_id, str):
                raise TypeError("batch_id is not a string: " + str(type(batch_id)))
            self._global_headers["X-Batch-Identifier"] = batch_id

        self.force_host = force_host
        self.force_scheme = force_scheme
        self.change_listener = VoidChangeListener()
        self.rest_api_util = RestApiUtil(session)

    @staticmethod
    def force_host_args(force_host):
        return {"force_host": force_host if force_host else None,
                "force_scheme": "http" if force_host else None}

    def add_global_header(self, key, value):
        if not isinstance(value, str):
            raise TypeError(f"header value for {key} value is not a string: {type(value)}")

        self._global_headers[key] = value

    def _merged_headers(self, request_headers: dict):
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
    async def _open_url(self, url, headers=None) -> StandardResponse:
        response = await self.rest_api_util.http_get(self._rewritten_link(url), self._merged_headers(headers))
        if not response.is_successful():
            raise Exception(f"Http {response.status} for {url}:\n{response}")
        return response


class MdbEnv(Enum):
    LOCAL = "http://localhost:22338"
    LOCAL_OVERLAY = "http://localhost:22358"
    DEV = "http://mdbklippdev.felles.ds.nrk.no"
    STAGE = "http://mdbklippstage.felles.ds.nrk.no"
    STAGE_K8S_INGRESS = "https://klipp-api-stage.kubeint.nrk.no"
    STAGE_VMWARE_NODE_1 = "http://drlxmdbklippstage01.felles.ds.nrk.no:22338"
    PROD = "http://mdbklipp.felles.ds.nrk.no"
    PROD_K8S_INGRESS = "http://klipp-api.mdb-prod.svc.int.nrk.cloud"
    PROD_VMWARE_NODE_1 = "http://malxmdbklipp01.felles.ds.nrk.no:22338"

    @property
    def is_stage(self):
        return self in (MdbEnv.STAGE, MdbEnv.STAGE_K8S_INGRESS, MdbEnv.STAGE_VMWARE_NODE_1)

    @property
    def is_prod(self):
        return self in (MdbEnv.PROD, MdbEnv.PROD_K8S_INGRESS, MdbEnv.PROD_VMWARE_NODE_1)

    @property
    def is_dev(self):
        return self == MdbEnv.DEV


class MdbJsonMethodApi(MdbJsonApi):
    """
    A jason api that knows how to invoke method calls directly by name. Requires a server addreess (api_base)
    """

    def __init__(self, session: ClientSession, api_base: Union[MdbEnv, str], user_id: str, correlation_id,
                 source_system: str = None,
                 batch_id: str = "default-batch-id",
                 force_host: bool = None, force_scheme: bool = None):
        MdbJsonApi.__init__(self, session, user_id, correlation_id, source_system, batch_id, force_host, force_scheme)
        parsed = urllib.parse.urlparse(api_base.value if isinstance(api_base, MdbEnv) else api_base)
        self.api_base = parsed.scheme + "://" + parsed.netloc + "/api"

    def __api_method(self, sub_path):
        return self.api_base + "/" + sub_path

    def _api_method(self, sub_path):
        return self.api_base + "/" + sub_path

    async def _invoke_get_method(self, name, parameters, headers=None) -> dict:
        real_method = self.__api_method(name)
        parameters_ = await self.rest_api_util.http_get(real_method, self._merged_headers(headers), parameters)
        return parameters_.response

    async def _invoke_raw_get_method(self, name, parameters, headers=None) -> str:
        real_method = self.__api_method(name)
        return await self.rest_api_util.raw_http_get(real_method, headers=headers, uri_params=parameters)

    async def _invoke_get_method_std_response(self, name, parameters, headers=None) -> StandardResponse:
        real_method = self.__api_method(name)
        return await self.rest_api_util.http_get(real_method, self._merged_headers(headers), parameters)

    async def _invoke_create_method(self, method_name, payload, headers=None) -> {}:
        real_method = self.__api_method(method_name)
        stdresponse = await self.rest_api_util.http_post_follow(real_method, payload, self._merged_headers(headers))
        response = stdresponse.response
        resId = response.get("resId") if response else None
        type = response.get("type") if response else None
        self.change_listener.on_create(resId, type if type else method_name, payload)
        return response


class MdbClient(MdbJsonMethodApi):
    def __init__(self, session: ClientSession, api_base: Union[MdbEnv, str], user_id: str, correlation_id: str,
                 source_system: str = None,
                 batch_id="default-batch-id", force_host: bool = None, force_scheme: bool = None):
        MdbJsonMethodApi.__init__(self, session, api_base, user_id, correlation_id, source_system, batch_id, force_host,
                                  force_scheme)

    @staticmethod
    def localhost(session: ClientSession, user_id: str, correlation_id=None, batch_id="default-batch-id"):
        return MdbClient(session, MdbEnv.LOCAL, user_id, correlation_id, None, batch_id)

    @staticmethod
    def localhost_overlay(session: ClientSession, user_id, correlation_id=None, batch_id="default-batch-id"):
        return MdbClient(session, MdbEnv.LOCAL_OVERLAY, user_id, correlation_id, None, batch_id)

    @staticmethod
    def dev(session: ClientSession, user_id, correlation_id=None, batch_id="default-batch-id"):
        return MdbClient(session, MdbEnv.DEV, user_id, correlation_id, None, batch_id)

    @staticmethod
    def stage(session: ClientSession, user_id, correlation_id=None, batch_id="default-batch-id"):
        return MdbClient(session, MdbEnv.STAGE, correlation_id, user_id, None, batch_id)

    @staticmethod
    def prod(session: ClientSession, user_id, correlation_id=None, batch_id="default-batch-id"):
        return MdbClient(session, MdbEnv.PROD, correlation_id, user_id, None, batch_id)

    async def __add_on_rel(self, owner, rel, payload, headers=None):
        link = self._rewritten_link(_link(owner, rel))
        response = await self._do_post(link, payload, headers)
        self.change_listener.on_add(owner.get("resId"), rel, payload)
        return response

    @backoff.on_exception(backoff.expo, ServerDisconnectedError, max_time=120)
    async def open_rel(self, owner, rel, headers=None):
        link = self._rewritten_link(_link(owner, rel))
        return await self._do_get(link, headers)

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

    async def create_rights_timeline(self, master_eo, timeline, headers=None, shallow=False) -> RightsTimeline:
        if type := timeline.get("type"):
            if type != RightsTimeline.TYPE:
                raise ValueError(f"Attempted to create a rights timeline with a supplied type {type}")
        else:
            timeline["type"] = RightsTimeline.TYPE
        # noinspection PyTypeChecker
        return await self.create_timeline(master_eo, timeline, headers, shallow);

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
    async def add_reference(self, owner, reference, headers=None):
        return await self.__add_on_rel(owner, "http://id.nrk.no/2016/mdb/relation/references", reference, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_category(self, owner, category, headers=None):
        return await self.__add_on_rel(owner, "http://id.nrk.no/2016/mdb/relation/categories", category, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_contributor(self, owner, contributor, headers=None):
        return await self.__add_on_rel(owner, "http://id.nrk.no/2016/mdb/relation/contributors", contributor, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_location(self, owner, location, headers=None):
        return await self.__add_on_rel(owner, "http://id.nrk.no/2016/mdb/relation/locations", location, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def migrate_metadata(self, version_group, headers=None):
        return await self.__add_on_rel(version_group, "temprel:migrateMetadata", {}, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def broadcast_change(self, destination, resid, headers=None):
        resolved = await self.resolve(resid)
        payload = {
            "destination": destination,
            "resId": resid,
            "type": resolved["type"]
        }
        real_method = self._api_method("changes/by-resid")
        headers = {**{"content-type": "application/x-www-form-urlencoded"}, **self._merged_headers(headers)}
        stdresponse = await self.rest_api_util.http_post_form(real_method, payload, headers)
        return stdresponse.response

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def full_reindex_single(self, type_, guid, headers=None):
        real_method = self._api_method(f"admin/mdbIndex/fullreindexsingle/{type_}/{guid}")
        headers = {**{"content-type": "application/x-www-form-urlencoded"}, **self._merged_headers(headers)}
        stdresponse = await self.rest_api_util.http_post_form(real_method, {}, headers)
        return stdresponse.response

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def __reindex_item(self, uri_part, guid, headers=None) -> str:
        real_method = self._api_method(f"admin/mdbIndex/{uri_part}/{guid}")
        headers = {**{"content-type": "application/x-www-form-urlencoded"}, **self._merged_headers(headers)}
        stdresponse = await self.rest_api_util.raw_http_get(real_method, headers=headers)
        return stdresponse

    async def reindex_meo(self, guid, headers=None):
        return await self.__reindex_item("masterEOs", guid, headers=headers)

    async def reindex_mo(self, guid, headers=None):
        return await self.__reindex_item("mediaObjects", guid, headers=headers)

    async def reindex_media_resource(self, guid, headers=None):
        return await self.__reindex_item("mediaResources", guid, headers=headers)

    async def reindex_pmo(self, guid, headers=None):
        return await self.__reindex_item("publicationMediaObjects", guid, headers=headers)

    async def reindex_essence(self, guid, headers=None):
        return await self.__reindex_item("essences", guid, headers=headers)

    async def reindex_version_group_group(self, guid, headers=None):
        return await self.__reindex_item("versionGroups", guid, headers=headers)

    async def reindex_publication_event(self, guid, headers=None):
        return await self.__reindex_item("publicationEvents", guid, headers=headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def like_query(self, like, headers=None):
        real_method = self._api_method("admin/events/likeQuery")
        stdresponse = await self.rest_api_util.http_get(real_method, headers, {"like": like})
        return stdresponse.response

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_or_replace_timeline(self, master_eo, timeline, headers=None) -> Timeline:
        type_of_timeline = timeline["Type"]
        existing_timeline_of_same_type = self._timelines_of_subtype(master_eo, type_of_timeline)
        if existing_timeline_of_same_type:
            # noinspection PyTypeChecker
            return create_response(
                await self.replace_timeline(master_eo, existing_timeline_of_same_type, timeline, headers))
        else:
            return create_response(await self._invoke_create_method("timeline", timeline, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_timeline_item(self, timeline, item, headers=None):
        return await self.__add_on_rel(timeline, REL_ITEMS, item, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_mediaresource_format(self, media_resource: MediaResource, format_, headers=None):
        return await self.__add_on_rel(media_resource, REL_FORMATS, format_, headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def add_stored_document(self, master_eo, stored_document, headers=None):
        return await self.__add_on_rel(master_eo, REL_DOCUMENTS, stored_document,
                                       headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_publication_event(self, master_eo, publication_event, headers=None) -> PublicationEvent:
        if not publication_event:
            raise Exception("Cannot create an empty publication event")
        publication_event["publishes"] = _res_id(master_eo)
        return create_response(await self._invoke_create_method("publicationEvent", publication_event, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def create_publication_media_object(self, publication_event, media_object, publication_media_object,
                                              headers=None) -> PublicationMediaObject:
        publication_media_object["publicationEvent"] = _res_id(publication_event)
        publication_media_object["publishedVersionOf"] = _res_id(media_object)
        return create_response(
            await self._invoke_create_method("publicationMediaObject", publication_media_object, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def open_url(self, url, headers=None) -> Optional[
        Union[MasterEO, PublicationMediaObject, MediaObject, MediaResource, Essence, PublicationEvent,
              InternalTimeline, GenealogyTimeline, IndexpointTimeline, TechnicalTimeline, RightsTimeline,
              GenealogyRightsTimeline, MasterEOResource]]:
        resp = await self._open_url(url)
        return create_response(resp.response)

    @backoff.on_exception(backoff.expo, ClientOSError, max_time=120)
    @backoff.on_exception(backoff.expo, HttpReqException, max_time=120, giveup=_check_if_not_lock)
    @backoff.on_exception(backoff.expo, ServerDisconnectedError, max_time=120)
    async def resolve(self, res_id: str, fail_on_missing: bool = True, headers: dict = None) -> \
            Optional[Union[MasterEO, PublicationMediaObject, MediaObject, MediaResource, Essence, PublicationEvent,
                           InternalTimeline, GenealogyTimeline, IndexpointTimeline, TechnicalTimeline, RightsTimeline,
                           GenealogyRightsTimeline]]:
        if not res_id:
            return
        parameters = {'resId': res_id}
        try:
            return create_response_from_std_response(
                await self._invoke_get_method_std_response("resolve", parameters, headers))
        except Http404:
            if not fail_on_missing:
                return None
            raise

    @backoff.on_exception(backoff.expo, ClientOSError, max_time=120)
    @backoff.on_exception(backoff.expo, HttpReqException, max_time=120, giveup=_check_if_not_lock)
    @backoff.on_exception(backoff.expo, ServerDisconnectedError, max_time=120)
    async def resolve_mmeo(self, res_id: str, headers: dict = None) -> Optional[MasterEO]:
        meo = await self.resolve(res_id, headers=headers)
        if not isinstance(meo, MasterEO):
            raise Exception(f"{res_id} resolves to a {meo.type()}, which we dont know how to meo")
        if not meo.get("isMetadataMeo"):
            vg = await self.open(meo.version_group(), headers)
            return await self.open(vg.metadata_meo(), headers)

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def find_media_object(self, name, headers: dict = None) -> Optional[MediaObject]:
        try:
            return create_response(await self._invoke_get_method("mediaObject/by-name", {"name": name}, headers))
        except Http404:
            pass

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def export_publication_event(self, aggregate_identifier, headers: dict = None) -> str:
        try:
            return await self._invoke_raw_get_method("admin/mdbExport/publicationEvents/" + aggregate_identifier, {},
                                                     headers)
        except Http404:
            pass

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def export_master_eo(self, aggregate_identifier, headers: dict = None) -> dict:
        try:
            return await self._invoke_raw_get_method("admin/mdbExport/masterEOs/" + aggregate_identifier, {},
                                                     headers)
        except Http404:
            pass

    @backoff.on_exception(backoff.expo, ClientOSError, max_time=120, giveup=_check_if_not_lock)
    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def reference(self, ref_type, value, headers=None) -> \
            List[Union[MasterEO, PublicationMediaObject, MediaObject, MediaResource, Essence, PublicationEvent,
                       InternalTimeline, GenealogyTimeline, IndexpointTimeline, TechnicalTimeline, RightsTimeline,
                       GenealogyRightsTimeline,MasterEOResource]]:
        responses = await self._invoke_get_method("references", {'type': ref_type, 'reference': value}, headers)
        return [create_response(x) for x in responses]

    @backoff.on_exception(backoff.expo, ClientOSError, max_time=120, giveup=_check_if_not_lock)
    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def reference_single(self, ref_type, value, headers=None) -> \
            Union[MasterEO, PublicationMediaObject, MediaObject, MediaResource, Essence, PublicationEvent,
                  InternalTimeline, GenealogyTimeline, IndexpointTimeline, TechnicalTimeline, RightsTimeline,
                  GenealogyRightsTimeline, None]:
        resp = await self._invoke_get_method("references", {'type': ref_type, 'reference': value}, headers)
        if resp:
            if len(resp) > 1:
                raise Exception(f"Multiple elements found when resolving {ref_type}={value}:{resp}")
            return await self.open(resp[0], headers)
        return None

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
        result = await self._do_delete(link, headers)
        self.change_listener.on_delete(owner.get("resId"))
        return result

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def open(self, owner, headers=None):
        if isinstance(owner, str):
            raise ValueError(f"Open does not expect a string, maybe you want resolve or open_url ?")

        link = self._rewritten_link(_self_link(owner))
        return create_response(await self._do_get(link, headers))

    GT = TypeVar('GT')

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def open_resource(self, owner: Optional[ResourceReference[GT]], headers=None) -> Optional[GT]:
        if not owner:
            return
        link = self._rewritten_link(_self_link(owner))
        return create_response(await self._do_get(link, headers))

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def open_resources(self, owner: ResourceReferenceCollection[GT], headers=None) -> List[GT]:
        return [await self.open_resource(x, headers) for x in owner]

    @backoff.on_exception(backoff.expo, HttpReqException, max_time=60, giveup=_check_if_not_lock)
    async def update(self, owner, updates, headers=None):
        link = self._rewritten_link(_self_link(owner))
        self.change_listener.on_change(owner.get("resId"), None, updates)
        return await self._do_post_follow(link, updates, headers)
