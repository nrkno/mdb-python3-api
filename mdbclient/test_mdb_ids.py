from uuid import UUID

from mdbclient.mdb_ids import parse_res_id, MasterEOId, PublicationEventId, MediaObjectId, \
    PublicationMediaObjectId, EssenceId, MediaResourceId, SerieId, TimelineId, SeasonId, BagId, MasterEOResourceId, \
    VersionGroupId


def test_parse_master_eo_resid():
    sut = "http://id.nrk.no/2016/mdb/masterEO/49686e33-ab3e-4240-a86e-33ab3e224002"
    meo = parse_res_id(sut)
    assert isinstance(meo, MasterEOId)
    assert meo.as_resid() == sut


def test_parse_master_eo_id():
    sut = "http://id.nrk.no/2016/mdb/masterEO/49686e33-ab3e-4240-a86e-33ab3e224002"
    meo = MasterEOId.parse(sut)
    assert isinstance(meo, MasterEOId)
    assert str(meo) == "49686e33-ab3e-4240-a86e-33ab3e224002"
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)


def test_parse_publication_event_resid():
    sut = "http://id.nrk.no/2016/mdb/publicationEvent/cf5fd0c3-7f4e-4fb3-9fd0-c37f4e2fb3eb"
    meo = parse_res_id(sut)
    assert isinstance(meo, PublicationEventId)
    assert str(meo.as_resid()) == sut


def test_parse_publication_event_id():
    guid = "49686e33-ab3e-4240-a86e-33ab3e224002"
    sut = f"http://id.nrk.no/2016/mdb/publicationEvent/{guid}"
    meo = PublicationEventId.parse(sut)
    assert isinstance(meo, PublicationEventId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)


def test_version_group_resid():
    sut = "http://id.nrk.no/2016/mdb/versionGroup/cf5fd0c3-7f4e-4fb3-9fd0-c37f4e2fb3eb"
    meo = parse_res_id(sut)
    assert isinstance(meo, VersionGroupId)
    assert str(meo.as_resid()) == sut


def test_version_group_id():
    guid = "cf5fd0c3-7f4e-4fb3-9fd0-c37f4e2fb3eb"
    sut = f"http://id.nrk.no/2016/mdb/versionGroup/{guid}"
    meo = VersionGroupId.parse(sut)
    assert isinstance(meo, VersionGroupId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)


def test_parse_mo():
    sut = "http://id.nrk.no/2016/mdb/mediaObject/f3517cb4-b1e1-4160-917c-b4b1e1c16085"
    meo = parse_res_id(sut)
    assert isinstance(meo, MediaObjectId)
    assert str(meo.as_resid()) == sut

def test_parse_mo_id():
    guid = "f3517cb4-b1e1-4160-917c-b4b1e1c16085"
    sut = f"http://id.nrk.no/2016/mdb/mediaObject/{guid}"
    meo = MediaObjectId.parse(sut)
    assert isinstance(meo, MediaObjectId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)


def test_parse_pmo():
    sut = "http://id.nrk.no/2016/mdb/publicationMediaObject/79d90fa8-20ae-465e-990f-a820aed65e57"
    meo = parse_res_id(sut)
    assert isinstance(meo, PublicationMediaObjectId)
    assert str(meo.as_resid()) == sut

def test_parse_pmo_id():
    guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
    sut = f"http://id.nrk.no/2016/mdb/publicationMediaObject/{guid}"
    meo = PublicationMediaObjectId.parse(sut)
    assert isinstance(meo, PublicationMediaObjectId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)


def test_parse_essence():
    sut = "http://id.nrk.no/2016/mdb/essence/d975c909-336e-4adb-b5c9-09336e9adbd8"
    meo = parse_res_id(sut)
    assert isinstance(meo, EssenceId)
    assert str(meo.as_resid()) == sut

def test_parse_essence_id():
    guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
    sut = f"http://id.nrk.no/2016/mdb/essence/{guid}"
    meo = EssenceId.parse(sut)
    assert isinstance(meo, EssenceId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)

def test_parse_media_resorce():
    sut = "http://id.nrk.no/2016/mdb/mediaResource/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, MediaResourceId)
    assert str(meo.as_resid()) == sut

def test_parse_media_resorce_id():
    guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
    sut = f"http://id.nrk.no/2016/mdb/mediaResource/{guid}"
    meo = MediaResourceId.parse(sut)
    assert isinstance(meo, MediaResourceId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)


def test_parse_bag():
    sut = "http://id.nrk.no/2016/mdb/bag/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, BagId)
    assert str(meo.as_resid()) == sut

def test_parse_bag_id():
    guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
    sut = f"http://id.nrk.no/2016/mdb/bag/{guid}"
    meo = BagId.parse(sut)
    assert isinstance(meo, BagId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)


def test_parse_mastereoresource():
    sut = "http://id.nrk.no/2016/mdb/masterEOResource/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, MasterEOResourceId)
    assert str(meo.as_resid()) == sut

def test_parse_mastereoresource_id():
    guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
    sut = f"http://id.nrk.no/2016/mdb/masterEOResource/{guid}"
    meo = MasterEOResourceId.parse(sut)
    assert isinstance(meo, MasterEOResourceId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)


def test_parse_serie():
    sut = "http://id.nrk.no/2016/mdb/serie/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, SerieId)
    assert meo.as_resid() == sut

def test_parse_serie_id():
    guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
    sut = f"http://id.nrk.no/2016/mdb/serie/{guid}"
    meo = SerieId.parse(sut)
    assert isinstance(meo, SerieId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)

def test_parse_season():
    sut = "http://id.nrk.no/2016/mdb/season/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, SeasonId)
    assert str(meo.as_resid()) == sut

def test_parse_season_id():
    guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
    sut = f"http://id.nrk.no/2016/mdb/season/{guid}"
    meo = SeasonId.parse(sut)
    assert isinstance(meo, SeasonId)
    assert str(meo) == guid
    assert meo.as_resid() == sut
    assert isinstance(meo.guid, UUID)

def test_parse_timeline():
    sut = "http://id.nrk.no/2017/mdb/timeline/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, TimelineId)
    assert str(meo.as_resid()) == sut

def test_parse_timeline_id():
    guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
    sut = f"http://id.nrk.no/2017/mdb/timeline/{guid}"
    id_ = TimelineId.parse(sut)
    assert isinstance(id_, TimelineId)
    assert str(id_) == guid
    assert id_.as_resid() == sut
    assert isinstance(id_.guid, UUID)
