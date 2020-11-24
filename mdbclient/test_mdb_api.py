from uuid import UUID

from mdbclient.mdb_ids import MasterEOResId, parse_res_id, PublicationEventResId, MediaObjectResId, \
    PublicationMediaObjectResId, EssenceResId, MediaResourceResId, MasterEOId, PublicationEventId, MediaObjectId, \
    PublicationMediaObjectId, EssenceId, MediaResourceId, BagResId, MasterEOResourceResId, SerieResId, SeasonResId, \
    SerieId, TimelineResId, TimelineId, SeasonId, BagId, MasterEOResourceId, VersionGroupResId, VersionGroupId


def test_parse_master_eo():
    sut = "http://id.nrk.no/2016/mdb/masterEO/49686e33-ab3e-4240-a86e-33ab3e224002"
    meo = parse_res_id(sut)
    assert isinstance(meo, MasterEOResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, MasterEOId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_publication_event():
    sut = "http://id.nrk.no/2016/mdb/publicationEvent/cf5fd0c3-7f4e-4fb3-9fd0-c37f4e2fb3eb"
    meo = parse_res_id(sut)
    assert isinstance(meo, PublicationEventResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, PublicationEventId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_version_group():
    sut = "http://id.nrk.no/2016/mdb/versionGroup/cf5fd0c3-7f4e-4fb3-9fd0-c37f4e2fb3eb"
    meo = parse_res_id(sut)
    assert isinstance(meo, VersionGroupResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, VersionGroupId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_mo():
    sut = "http://id.nrk.no/2016/mdb/mediaObject/f3517cb4-b1e1-4160-917c-b4b1e1c16085"
    meo = parse_res_id(sut)
    assert isinstance(meo, MediaObjectResId)
    assert str(meo) == sut

    assert isinstance(meo.mdb_id, MediaObjectId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_pmo():
    sut = "http://id.nrk.no/2016/mdb/publicationMediaObject/79d90fa8-20ae-465e-990f-a820aed65e57"
    meo = parse_res_id(sut)
    assert isinstance(meo, PublicationMediaObjectResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, PublicationMediaObjectId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_essence():
    sut = "http://id.nrk.no/2016/mdb/essence/d975c909-336e-4adb-b5c9-09336e9adbd8"
    meo = parse_res_id(sut)
    assert isinstance(meo, EssenceResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, EssenceId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_media_resorce():
    sut = "http://id.nrk.no/2016/mdb/mediaResource/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, MediaResourceResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, MediaResourceId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_bag():
    sut = "http://id.nrk.no/2016/mdb/bag/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, BagResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, BagId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_mastereoresource():
    sut = "http://id.nrk.no/2016/mdb/masterEOResource/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, MasterEOResourceResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, MasterEOResourceId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_serie():
    sut = "http://id.nrk.no/2016/mdb/serie/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, SerieResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, SerieId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_season():
    sut = "http://id.nrk.no/2016/mdb/season/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, SeasonResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, SeasonId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut


def test_parse_timeline():
    sut = "http://id.nrk.no/2017/mdb/timeline/796d659f-a805-4c96-ad65-9fa805ac96cb"
    meo = parse_res_id(sut)
    assert isinstance(meo, TimelineResId)
    assert str(meo) == sut
    assert isinstance(meo.mdb_id, TimelineId)
    assert isinstance(meo.mdb_id.guid, UUID)
    assert str(meo.mdb_id.as_resid()) == sut
