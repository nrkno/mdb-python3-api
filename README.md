Mdb Python3 Api
=====================

This project contains an api for accessing mdb. The api exists at two different levels; an
"object" level api and a lower-level rest-layer api.

Simple usage of the object api:

async def create_publication():
    async with MdbClient("http://mdbklippdev.felles.ds.nrk.no/api", "my-user-id", "my-correlation-id") as client:
        meo = await client.create_master_eo({"title": "myMeo"})
        pe = await client.create_publication_event(meo, {"title": "min publisering"})


    
If you are receiving a url from an external source (like a rabbitMQ queue), you can use the rest layer api:

    async def resolve_mmeo_and_create_subject(uri):
        async with MdbJsonApi("my-user-id","my-correlation-id") as client:
            meo = await client.open(uri)
            vg = await client.open(meo["versionGroup"])
            metadata_meo = await client.open(vg["metadataMeo"])
            # add a tag (subject)
            await client.add_on_rel(meo, "http://id.nrk.no/2016/mdb/relation/subjects", {"title" : "min TestTagg"})




