import aiohttp
from mdbclient.mdbclient import MdbClient


async def create_publication():
    async with aiohttp.ClientSession() as session:
        client = MdbClient("http://mdbklippdev.felles.ds.nrk.no/api", "my-user-id", "my-correlation-id", session)
        meo = await client.create_master_eo({"title": "myMeo"})
        pe = await client.create_publication_event(meo, {"title": "min publisering"})


async def resolve_mmeo_and_create_subject(uri):
    async with aiohttp.ClientSession() as session:
        client = MdbClient("http://mdbklippdev.felles.ds.nrk.no/api", "my-user-id", "my-correlation-id", session)
        meo = await client.open(uri)
        vg = await client.open(meo["versionGroup"])
        metadata_meo = await client.open(vg["metadataMeo"])
        # add a tag (subject)
        await client.add_subject(meo, "http://id.nrk.no/2016/mdbclient/relation/subjects", {"title" : "min TestTagg"})







