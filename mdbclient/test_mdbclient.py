import random
import string

import aiohttp
import pytest

from mdbclient.headers import create_headers
from mdbclient.mdbclient import MdbClient, Timeline


@pytest.mark.asyncio
async def test_create_update_meos():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        result = await client.create_master_eo({"title": "fozz"})
        assert result['title'] == 'fozz'
        updated = await client.update(result, {"title": "fizz"})
        assert updated['title'] == 'fizz'


@pytest.mark.asyncio
async def test_delete_meos():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        result = await client.create_master_eo({"title": "fozz"})
        assert result['title'] == 'fozz'
        await client.delete(result)
        updated = await client.open(result)
        assert updated['deleted'] == True


@pytest.mark.asyncio
async def test_find_mo_by_name():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        result = await client.create_master_eo({"title": "fozz"})
        assert result['title'] == 'fozz'
        await client.delete(result)
        updated = await client.open(result)
        assert updated['deleted'] == True


@pytest.mark.asyncio
async def test_create_update_publication_event():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        meo = await client.create_master_eo({"title": "fozz"})
        pe = await client.create_publication_event(meo, {"subType": "http://authority.nrk.no/datadictionary/onDemand",
                                                         "title": "en kald vårdag"})
        assert pe['title'] == 'en kald vårdag'
        updatepe = await client.update(pe, {"title": "bruz"})
        assert updatepe['title'] == 'bruz'


@pytest.mark.asyncio
async def test_create_update_publication_event():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        meo = await client.create_master_eo({"title": "fozz"})
        pe = await client.create_publication_event(meo, {"subType": "http://authority.nrk.no/datadictionary/onDemand",
                                                         "title": "en kald vårdag"})
        assert pe['title'] == 'en kald vårdag'
        updatepe = await client.update(pe, {"title": "bruz"})
        assert updatepe['title'] == 'bruz'


@pytest.mark.asyncio
async def test_create_update_publication_media_object():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        meo = await client.create_master_eo({"title": "fozz"})
        mo = await client.create_media_object(meo, {})
        pe = await client.create_publication_event(meo, {"subType": "http://authority.nrk.no/datadictionary/onDemand",
                                                         "title": "en kald vårdag"})
        pmo = await client.create_publication_media_object(pe, mo, {})
        assert pmo['type'] == 'http://id.nrk.no/2016/mdb/types/PublicationMediaObject'


@pytest.mark.asyncio
async def test_create_essence():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        meo = await client.create_master_eo({"title": "fozz"})
        mo = await client.create_media_object(meo, {})
        mr = await client.create_media_resource(mo, {})
        pe = await client.create_publication_event(meo, {"subType": "http://authority.nrk.no/datadictionary/onDemand",
                                                         "title": "en kald vårdag"})
        pmo = await client.create_publication_media_object(pe, mo, {})
        essence = await client.create_essence(pmo, mr, {})
        assert essence['type'] == 'http://id.nrk.no/2016/mdb/types/Essence'


@pytest.mark.asyncio
async def test_add_timeline_item():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        meo = await client.create_master_eo({"title": "fozz"})
        tl = await client.create_timeline(meo, {"type": 'http://id.nrk.no/2017/mdb/timelinetype/IndexPoints'})
        item_to_ad = {
            "title": 'first indexpoint',
            "name": 'FIRST_INDEX',
        }
        resp = await client.add_timeline_item(tl, item_to_ad)
        assert resp["type"] == 'http://id.nrk.no/2017/mdb/timelineitem/IndexpointTimelineItem'


@pytest.mark.asyncio
async def test_replace_timeline():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        meo = await client.create_master_eo({"title": "fozz"})
        tl = await client.create_timeline(meo, {"type": 'http://id.nrk.no/2017/mdb/timelinetype/IndexPoints'})
        replacement = {"type": "http://id.nrk.no/2017/mdb/timelinetype/IndexPoints", "items": [{
            "type": "http://id.nrk.no/2017/mdb/timelineitem/IndexpointTimelineItem",
            "title": 'first indexpoint',
            "name": 'FIRST_INDEX',
        }]}
        resp = await client.replace_timeline(meo, tl, replacement)
        assert resp["type"] == 'http://id.nrk.no/2017/mdb/timelinetype/IndexPoints'


@pytest.mark.asyncio
async def test_resolve_meo():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        result = await client.create_master_eo({"title": "fozz"})
        assert result['title'] == 'fozz'
        resolved = await client.resolve(result['resId'])
        assert resolved is not None
        test_open = await client.open(result)
        assert test_open is not None


@pytest.mark.asyncio
async def test_resolve_meo_fast():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        result = await client.create_master_eo({"title": "fozz"})
        assert result['title'] == 'fozz'
        resolved = await client.resolve(result['resId'], fast=True)
        assert resolved is not None
        assert resolved["resId"] == result["resId"]


@pytest.mark.asyncio
async def test_resolve_reference():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        result = await client.create_master_eo(
            {"title": "fozz", "references": [{"type": "x-test:reference-type", "reference": "123"}]})
        assert result['title'] == 'fozz'
        resolved = await client.reference("x-test:reference-type", "123")
        assert resolved is not None


@pytest.mark.asyncio
async def test_add_subjects():
    async with aiohttp.ClientSession() as session:
        client = MdbClient.localhost("test", session, "test_correlation")
        result = await client.create_master_eo({"title": "fozz"})
        subj = await client.add_subject(result, {"title": 'sub2'})
        assert subj["title"] == 'sub2'


def random_string(string_length=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    # noinspection PyUnusedLocal
    return ''.join(random.choice(letters) for i in range(string_length))


@pytest.mark.asyncio
async def test_find_media_object():
    async with aiohttp.ClientSession() as session:
        mediaobject_name = "fozz-bizz&cz_" + random_string(10)
        client = MdbClient.localhost("test", session, "test_correlation")
        result = await client.create_master_eo({"title": "fozz"})
        mo = await client.create_media_object(result, {"name": mediaobject_name})
        found = await client.find_media_object(mediaobject_name)
        assert mo["resId"] == found["resId"]


@pytest.mark.asyncio
async def test_create_headers():
    hdrs = create_headers(("X-A", "B"), ("X-C", "D"))
    assert len(hdrs) == 2
    assert hdrs["X-A"] == "B"
    assert hdrs["X-C"] == "D"

@pytest.mark.asyncio
async def test_timeline_item():
    timeline = Timeline({"items": [{"resId": "abc", "offset" : 10}, {"resId": "cde",  "offset" : 20}]})
    first = timeline.find_item("abc")
    assert first["offset"] == 10