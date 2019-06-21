import json
import urllib.parse
import aiohttp


class AggregateGoneException(Exception):
    pass


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
        return [l for l in links_list if l["subType"] == sub_type]

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

    @staticmethod
    async def unpack_json_response(response):
        if response.status == 404:
            return None, 404
        if response.status == 410:
            raise AggregateGoneException
        if response.status >= 500:
            return await response.text(), response.status
        try:
            json_ = await response.json()
            return json_, response.status
        except Exception as e:
            print(e)
            raise e

    @staticmethod
    async def text_response(res):
        return await res.text(), res.status

    @staticmethod
    async def verbatim_response(created):
        return created


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

    # @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
    def create_get(self, uri, response_unpacker, headers=None):
        async def do_get(uri_params=None):
            async with self.session.get(uri, params=uri_params, headers=headers) as response:
                result = await response_unpacker(response)
                return result

        return do_get

    async def delete(self, uri, headers, response_unpacker):
        async with self.session.delete(uri, headers=headers) as response:
            return await response_unpacker(response)

    # @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8)
    def post_to_static_uri(self, uri, response_unpacker, headers=None):
        async def do_post(json_payload):
            pm = self.post(response_unpacker, headers)
            return await pm(json_payload, uri)

        return do_post

    def post(self, response_unpacker, headers=None):
        async def do_post(json_payload, uri):
            log_msg = f"POST TO {uri}\n{json.dumps(json_payload, indent=4, sort_keys=True)}"
            self.traffic.append(log_msg)
            async with self.session.post(uri, json=json_payload, headers=headers) as response:
                if response.status >= 400:
                    raise Exception(
                        f"Status was {response.status}: {await response.text()}, request to {uri} was {json_payload}")
                if "Location" not in response.headers:
                    raise Exception(f"No location in response {await response.text()} for {log_msg}")
                response_payload = await self.follow(response, response_unpacker)
                self.traffic.append(response_payload)
                return response_payload

        return do_post

    async def put(self, uri, json_payload, headers, responsefunc):
        async with self.session.put(uri, json=json_payload, headers=headers) as response:
            result = await responsefunc(response)
            return result

    async def follow(self, response, responsefunc):
        loc = response.headers["Location"]
        f = self.create_get(loc, responsefunc)
        return await f()


def _res_id(mdb_object):
    return {"resId": mdb_object["resId"]}


class MdbJsonApi(object):
    """
    Knows how to work with mdbclient-format hyperlinked json objects. Calls mdbclient in a responsible manner with
    all the correct headers.

    Knows how to rewrite all urls to use a host different from the one in the supplied url, using param force_host

    Does not handle http error codes, clients that need to interpret error codes will need to
    use a custom response_unpacker with this class. This response_unpacker should probably raise exceptions.

    'owner' in method names in this class refer to some json object with a "links" collection.

    This class is unaware of the url of the remote system since it's all in the hyperlinked payloads.
    """

    def __init__(self, user_id, correlation_id, source_system=None,
                 response_unpacker=ApiResponseParser.unpack_json_response,
                 force_host=None, force_scheme=None):
        self._headers = {}
        if source_system:
            self._headers["X-Source-System"] = source_system
        if user_id:
            self._headers["X-userId"] = user_id
        if correlation_id:
            self._headers["X-transactionId"] = correlation_id
        self.json_response_unpacker = response_unpacker
        self.force_host = force_host
        self.force_scheme = force_scheme
        self.parsed_json_post = None

    async def __aenter__(self):
        self.rest_api_util = RestApiUtil()
        self.parsed_json_post = self.rest_api_util.post(self.json_response_unpacker, self._headers)
        await self.rest_api_util.__aenter__()
        return self

    async def __aexit__(self, *err):
        await self.rest_api_util.__aexit__()
        self.rest_api_util = None
        self.parsed_json_post = None

    def add_header(self, key, value):
        self._headers[key] = value

    def rewritten_link(self, link):
        if not self.force_host:
            return link
        parsed = urllib.parse.urlparse(link)
        # noinspection PyProtectedMember
        replaced = parsed._replace(netloc=self.force_host, scheme="http")
        return replaced.geturl()

    async def add_on_rel(self, owner, rel, payload):
        link = self.rewritten_link(ApiResponseParser.link(owner, rel))
        resp = await self.parsed_json_post(payload, link)
        return resp

    async def delete(self, owner, additional_headers=None):
        link = self.rewritten_link(ApiResponseParser.self_link(owner))
        headers_to_use = {**self._headers, **additional_headers} if additional_headers else self._headers
        resp, status = await self.rest_api_util.delete(link, headers_to_use, ApiResponseParser.text_response)
        return resp, status

    async def open(self, owner, additional_headers=None):
        link = self.rewritten_link(ApiResponseParser.self_link(owner))
        headers_to_use = {**self._headers, **additional_headers} if additional_headers else self._headers
        get_method = self.rest_api_util.create_get(link, self.json_response_unpacker, headers_to_use)
        response, status = await get_method()
        return response

    async def get_json(self, url, additional_headers=None):
        headers_to_use = {**self._headers, **additional_headers} if additional_headers else self._headers
        get_method = self.rest_api_util.create_get(self.rewritten_link(url), self.json_response_unpacker,
                                                   headers_to_use)
        response, status = await get_method()
        if not ApiResponseParser.is_successful(status):
            raise Exception(f"Http {status} for {url}:\n{response}")
        return response

    async def update(self, owner, payload):
        link = self.rewritten_link(ApiResponseParser.self_link(owner))
        resp, status = await self.parsed_json_post(payload, link)
        return resp


class MdbClient(MdbJsonApi):
    def __init__(self, api_base, user_id, correlation_id=None, source_system=None):
        super().__init__(correlation_id, user_id, source_system)
        parsed = urllib.parse.urlparse(api_base)
        self.api_base = parsed.scheme + "://" + parsed.netloc + "/api"

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

    def api_method(self, sub_path):
        return self.api_base + "/" + sub_path

    async def __aenter__(self):
        self.rest_api_util = RestApiUtil()
        await super().__aenter__()

        def std_json_post(method_name):
            real_method = self.api_method(method_name)
            return self.rest_api_util.post_to_static_uri(real_method, ApiResponseParser.unpack_json_response,
                                                         self._headers)

        def std_json_get(method_name):
            real_method = self.api_method(method_name)
            return self.rest_api_util.create_get(real_method, ApiResponseParser.unpack_json_response, self._headers)

        self.meo_create = std_json_post("masterEO")
        self.pe_create = std_json_post("publicationEvent")
        self.mo_create = std_json_post("mediaObject")
        self.mr_create = std_json_post("mediaResource")
        self.pmo_create = std_json_post("publicationMediaObject")
        self.timeline_create = std_json_post("timeline")
        self.essence_create = std_json_post("essence")
        self.resolve_resid = std_json_get("resolve")
        self.resolve_reference = std_json_get("references")
        self.get_serie = std_json_get("serie/by_title")
        self.serie_create = std_json_post("serie")
        self.season_create = std_json_post("season")
        return self

    async def create_master_eo(self, master_eo):
        eo_, status = await self.meo_create(master_eo)
        return eo_

    async def create_media_object(self, master_eo, media_object):
        media_object["masterEO"] = _res_id(master_eo)
        mo, status = await self.mo_create(media_object)
        return mo

    async def create_media_resource(self, media_object, media_resource):
        media_resource["mediaObject"] = _res_id(media_object)
        mr, status = await self.mr_create(media_resource)
        return mr

    async def create_essence(self, publication_media_object, media_resource, essence):
        essence["composedOf"] = _res_id(media_resource)
        essence["playoutOf"] = _res_id(publication_media_object)
        essence_, status = await self.essence_create(essence)
        return essence_

    async def create_timeline(self, master_eo, timeline):
        timeline["masterEO"] = _res_id(master_eo)
        timeline_, status = await self.timeline_create(timeline)
        return timeline_

    async def create_publication_event(self, master_eo, publication_event):
        if not publication_event:
            raise Exception("Cannot create an empty publication event")
        publication_event["publishes"] = _res_id(master_eo)
        publication_event["subType"] = "http://authority.nrk.no/datadictionary/broadcast"
        pe, status = await self.pe_create(publication_event)
        return pe

    async def create_publication_media_object(self, publication_event, media_object, publication_media_object):
        publication_media_object["publicationEvent"] = _res_id(publication_event)
        publication_media_object["publishedVersionOf"] = _res_id(media_object)
        pmo, status = await self.pmo_create(publication_media_object)
        return pmo

    async def resolve(self, res_id):
        if not res_id:
            return
        resp, status = await self.resolve_resid({'resId': res_id})
        return resp

    async def reference(self, ref_type, value):
        resp, status = await self.resolve_reference({'type': ref_type, 'reference': value})
        return resp

    async def reference_single(self, ref_type, value):
        resp, status = await self.resolve_reference({'type': ref_type, 'reference': value})
        if resp:
            if len(resp) > 1:
                raise Exception(f"Multiple elements found when resolving {ref_type}={value}:{resp}")
            return await self.open(resp[0])
        return resp

    @staticmethod
    def _timelines_of_subtype(master_eo, sub_type):
        item = (tl for tl in master_eo["timelines"] if tl["subType"] == sub_type)
        return next(item, None)

    async def create_or_replace_timeline(self, master_eo, timeline):
        type_of_timeline = timeline["Type"]
        existing_timeline_of_same_type = self._timelines_of_subtype(master_eo, type_of_timeline)
        if existing_timeline_of_same_type:
            return await self.replace_timeline(master_eo, existing_timeline_of_same_type, timeline)
        else:
            return await self.timeline_create(timeline)

    async def replace_timeline(self, master_eo, existing_timeline, timeline):
        timeline.MasterEO = {'ResId': master_eo.ResId}
        self_link = ApiResponseParser.self_link(existing_timeline)
        resp, status = await self.rest_api_util.put(self_link, timeline, self._headers,
                                                    ApiResponseParser.verbatim_response)
        return resp

    async def find_serie(self, title, master_system):
        serie, status = await self.get_serie({'title': title, 'masterSystem': master_system})
        return serie[0] if serie else None

    async def create_serie(self, title, master_system):
        payload = {"title": title, "masterSystem": master_system}
        serie, status = await self.serie_create(payload)
        return serie

    async def create_serie_2(self, payload):
        serie, status = await self.serie_create(payload)
        return serie

    async def create_season(self, season):
        season, status = await self.season_create(season)
        return season

    async def create_episode(self, season_id, episode):
        real_method = self.api_method(f"serie/{season_id}/episode")
        serie = await self.parsed_json_post(episode, real_method)
        return serie
