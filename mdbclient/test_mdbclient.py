import pytest

from mdbclient.mdbclient import MdbClient


def create_mdb_client():
    return MdbClient.localhost("test", "test_correlation")


@pytest.mark.asyncio
async def test_create_update_meos():
    async with create_mdb_client() as client:
        result = await client.create_master_eo({"title": "fozz"})
        assert result['title'] == 'fozz'
        updated = await client.update(result, {"title": "fizz"})
        assert updated['title'] == 'fizz'


@pytest.mark.asyncio
async def test_delete_meos():
    async with create_mdb_client() as client:
        result = await client.create_master_eo({"title": "fozz"})
        assert result['title'] == 'fozz'
        await client.delete(result)
        updated = await client.open(result)
        assert updated['deleted'] == True


@pytest.mark.asyncio
async def test_create_update_publication_event():
    async with create_mdb_client() as client:
        meo = await client.create_master_eo({"title": "fozz"})
        pe = await client.create_publication_event(meo, {"subType": "http://authority.nrk.no/datadictionary/onDemand",
                                                         "title": "en kald vårdag"})
        assert pe['title'] == 'en kald vårdag'
        updatepe = await client.update(pe, {"title": "bruz"})
        assert updatepe['title'] == 'bruz'


@pytest.mark.asyncio
async def test_create_update_publication_event():
    async with create_mdb_client() as client:
        meo = await client.create_master_eo({"title": "fozz"})
        pe = await client.create_publication_event(meo, {"subType": "http://authority.nrk.no/datadictionary/onDemand",
                                                         "title": "en kald vårdag"})
        assert pe['title'] == 'en kald vårdag'
        updatepe = await client.update(pe, {"title": "bruz"})
        assert updatepe['title'] == 'bruz'


@pytest.mark.asyncio
async def test_create_update_publication_media_object():
    async with create_mdb_client() as client:
        meo = await client.create_master_eo({"title": "fozz"})
        mo = await client.create_media_object(meo, {})
        pe = await client.create_publication_event(meo, {"subType": "http://authority.nrk.no/datadictionary/onDemand",
                                                         "title": "en kald vårdag"})
        pmo = await client.create_publication_media_object(pe, mo, {})
        assert pmo['type'] == 'http://id.nrk.no/2016/mdb/types/PublicationMediaObject'


@pytest.mark.asyncio
async def test_create_essence():
    async with create_mdb_client() as client:
        meo = await client.create_master_eo({"title": "fozz"})
        mo = await client.create_media_object(meo, {})
        mr = await client.create_media_resource(mo, {})
        pe = await client.create_publication_event(meo, {"subType": "http://authority.nrk.no/datadictionary/onDemand",
                                                         "title": "en kald vårdag"})
        pmo = await client.create_publication_media_object(pe, mo, {})
        essence = await client.create_essence(pmo, mr, {})
        assert essence['type'] == 'http://id.nrk.no/2016/mdb/types/Essence'


@pytest.mark.asyncio
async def test_resolve_meo():
    async with create_mdb_client() as client:
        result = await client.create_master_eo({"title": "fozz"})
        assert result['title'] == 'fozz'
        resolved = await client.resolve(result['resId'])
        assert resolved is not None
        test_open = await client.open(result)
        assert test_open is not None


@pytest.mark.asyncio
async def test_resolve_reference():
    async with create_mdb_client() as client:
        result = await client.create_master_eo(
            {"title": "fozz", "references": [{"type": "x-test:reference-type", "reference": "123"}]})
        assert result['title'] == 'fozz'
        resolved = await client.reference("x-test:reference-type", "123")
        assert resolved is not None


@pytest.mark.asyncio
async def test_add_on_rel():
    async with create_mdb_client() as client:
        result = await client.create_master_eo({"title": "fozz"})
        subj, status = await client.add_on_rel(result, "http://id.nrk.no/2016/mdb/relation/subjects", {"title": 'sub2'})
        assert status == 200
        assert subj['subjects'][0]["title"] == 'sub2'
