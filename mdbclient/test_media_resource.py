import random
import string

import aiohttp
import pytest

from mdbclient.headers import create_headers
from mdbclient.mdbclient import MdbClient, Timeline, EditorialObject, MediaResource

mr_src = {
    "resId": "http://id.nrk.no/2016/mdb/mediaResource/1af85163-829f-4461-b851-63829fa4613c",
    "links": [
        {
            "rel": "self",
            "type": "application/json",
            "href": "http://localhost:22358/api/mediaResource/1af85163-829f-4461-b851-63829fa4613c"
        },
        {
            "rel": "http://id.nrk.no/2016/mdb/relation/locators",
            "type": "application/json",
            "href": "http://localhost:22358/api/mediaResource/1af85163-829f-4461-b851-63829fa4613c/locators"
        },
        {
            "rel": "http://id.nrk.no/2016/mdb/relation/formats",
            "type": "application/json",
            "href": "http://localhost:22358/api/mediaResource/1af85163-829f-4461-b851-63829fa4613c/formats"
        },
        {
            "rel": "http://id.nrk.no/2016/mdb/relation/model",
            "type": "text/turtle",
            "href": "http://localhost:22358/api/mediaResource/1af85163-829f-4461-b851-63829fa4613c/model"
        }
    ],
    "created": "2020-04-16T07:12:04.113595Z",
    "lastUpdated": "2020-04-16T07:12:04.113595Z",
    "essences": [
        {
            "resId": "http://id.nrk.no/2016/mdb/essence/b15a36fb-d1f6-4944-9a36-fbd1f6594477",
            "links": [
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": "http://localhost:22358/api/essence/b15a36fb-d1f6-4944-9a36-fbd1f6594477"
                }
            ],
            "type": "http://id.nrk.no/2016/mdb/types/Essence"
        }
    ],
    "locators": [
        {
            "resId": "http://id.nrk.no/2016/locator/rest-client/object/42f66cc4-2ab0-42c1-b66c-c42ab0c2c1ca",
            "links": [
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": "http://localhost:22358/api/mediaResource/1af85163-829f-4461-b851-63829fa4613c/locators/http:%2F%2Fid.nrk.no%2F2016%2Flocator%2Frest-client%2Fobject%2F42f66cc4-2ab0-42c1-b66c-c42ab0c2c1ca"
                }
            ],
            "identifier": "archiveNumber.id.nrk.no:%20A%2063976",
            "title": " A 63976",
            "storageType": {
                "resId": "http://authority.nrk.no/datadictionary/archiveNumber",
                "isSuppressed": False
            }
        }
    ],
    "deleted": False,
    "mediaObject": {
        "resId": "http://id.nrk.no/2016/mdb/mediaObject/c83b3ba4-0761-4a60-bb3b-a40761ba60be",
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "href": "http://localhost:22358/api/mediaObject/c83b3ba4-0761-4a60-bb3b-a40761ba60be"
            }
        ],
        "type": "http://id.nrk.no/2016/mdb/types/MediaObject"
    },
    "type": "http://id.nrk.no/2016/mdb/types/MediaResource"
}


@pytest.mark.asyncio
async def test_create_update_meos():

    mr = MediaResource(mr_src)
    matching = mr.matching_locators("archiveNumber.id.nrk.no:%20A%2063976", "http://authority.nrk.no/datadictionary/archiveNumber")
    assert len(matching) == 1

    assert not mr.matching_locators("archiveNumber.id.nrk.no:%20A%2063976", "zz")
    assert not mr.matching_locators("zz", "http://authority.nrk.no/datadictionary/archiveNumber")
