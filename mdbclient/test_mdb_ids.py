from uuid import UUID

from mdbclient.mdb_ids import parse_res_id, MasterEOResId, PublicationEventResId, VersionGroupResId, MediaObjectResId, \
    PublicationMediaObjectResId, EssenceResId, MediaResourceResId, BagResId, MasterEOResourceResId, SerieResId, \
    SeasonResId

master_eo_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
master_eo_sut = f"http://id.nrk.no/2016/mdb/masterEO/{master_eo_guid}"


def check_master_eo(master_eo):
    assert isinstance(master_eo, MasterEOResId)
    assert master_eo.id() == master_eo_guid
    assert str(master_eo) == master_eo_sut
    assert isinstance(master_eo.mdb_id.guid, UUID)


def test_parse_res_id_master_eo_resid():
    check_master_eo(parse_res_id(master_eo_sut))


def test_master_eo_parse():
    check_master_eo(MasterEOResId.parse(master_eo_sut))


def test_master_eo_of_id():
    check_master_eo(MasterEOResId.of_id(master_eo_guid))


pe_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
pe_sut = f"http://id.nrk.no/2016/mdb/publicationEvent/{pe_guid}"


def check_pe(pe):
    assert isinstance(pe, PublicationEventResId)
    assert pe.id() == pe_guid
    assert str(pe) == pe_sut
    assert isinstance(pe.mdb_id.guid, UUID)


def test_parse_res_id_pe_resid():
    check_pe(parse_res_id(pe_sut))


def test_pe_parse():
    check_pe(PublicationEventResId.parse(pe_sut))


def test_pe_of_id():
    check_pe(PublicationEventResId.of_id(pe_guid))


vg_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
vg_sut = f"http://id.nrk.no/2016/mdb/versionGroup/{vg_guid}"


def check_vg(vg):
    assert isinstance(vg, VersionGroupResId)
    assert vg.id() == vg_guid
    assert str(vg) == vg_sut
    assert isinstance(vg.mdb_id.guid, UUID)


def test_parse_res_id_vg_resid():
    check_vg(parse_res_id(vg_sut))


def test_vg_parse():
    check_vg(VersionGroupResId.parse(vg_sut))


def test_vg_of_id():
    check_vg(VersionGroupResId.of_id(vg_guid))


mo_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
mo_sut = f"http://id.nrk.no/2016/mdb/mediaObject/{mo_guid}"


def check_mo(mo):
    assert isinstance(mo, MediaObjectResId)
    assert mo.id() == mo_guid
    assert str(mo) == mo_sut
    assert isinstance(mo.mdb_id.guid, UUID)


def test_parse_res_id_mo_resid():
    check_mo(parse_res_id(mo_sut))


def test_mo_parse():
    check_mo(MediaObjectResId.parse(mo_sut))


def test_mo_of_id():
    check_mo(MediaObjectResId.of_id(mo_guid))


pmo_guid = "79d90fa8-20ae-465e-990f-a820aed65e57"
pmo_sut = f"http://id.nrk.no/2016/mdb/publicationMediaObject/{pmo_guid}"


def check_pmo(pmo):
    assert isinstance(pmo, PublicationMediaObjectResId)
    assert pmo.id() == pmo_guid
    assert str(pmo) == pmo_sut
    assert isinstance(pmo.mdb_id.guid, UUID)


def test_pmo_parse():
    check_pmo(PublicationMediaObjectResId.parse(pmo_sut))


def test_pmo_of_id():
    check_pmo(PublicationMediaObjectResId.of_id(pmo_guid))


essence_guid = "d975c909-336e-4adb-b5c9-09336e9adbd8"
essence_sut = f"http://id.nrk.no/2016/mdb/essence/{essence_guid}"


def check_essence(pmo):
    assert isinstance(pmo, EssenceResId)
    assert pmo.id() == essence_guid
    assert str(pmo) == essence_sut
    assert isinstance(pmo.mdb_id.guid, UUID)


def test_essence_parse():
    check_essence(EssenceResId.parse(essence_sut))


def test_essence_of_id():
    check_essence(EssenceResId.of_id(essence_guid))


mr_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
mr_sut = f"http://id.nrk.no/2016/mdb/mediaResource/{mr_guid}"


def check_mr(mr):
    assert isinstance(mr, MediaResourceResId)
    assert mr.id() == mr_guid
    assert str(mr) == mr_sut
    assert isinstance(mr.mdb_id.guid, UUID)


def test_mr_parse():
    check_mr(MediaResourceResId.parse(mr_sut))


def test_mr_of_id():
    check_mr(MediaResourceResId.of_id(mr_guid))


bag_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
bag_sut = f"http://id.nrk.no/2016/mdb/bag/{bag_guid}"


def check_bag(bag):
    assert isinstance(bag, BagResId)
    assert bag.id() == bag_guid
    assert str(bag) == bag_sut
    assert isinstance(bag.mdb_id.guid, UUID)


def test_bag_parse():
    check_bag(BagResId.parse(bag_sut))


def test_bag_of_id():
    check_bag(BagResId.of_id(bag_guid))


mrr_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
mrr_sut = f"http://id.nrk.no/2016/mdb/masterEOResource/{mrr_guid}"


def check_mrr(mrr):
    assert isinstance(mrr, MasterEOResourceResId)
    assert mrr.id() == mrr_guid
    assert str(mrr) == mrr_sut
    assert isinstance(mrr.mdb_id.guid, UUID)


def test_mrr_parse():
    check_mrr(MasterEOResourceResId.parse(mrr_sut))


def test_mrr_of_id():
    check_mrr(MasterEOResourceResId.of_id(mrr_guid))


serie_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
serie_sut = f"http://id.nrk.no/2016/mdb/serie/{mrr_guid}"


def check_serie(serie):
    assert isinstance(serie, SerieResId)
    assert serie.id() == serie_guid
    assert str(serie) == serie_sut
    assert isinstance(serie.mdb_id.guid, UUID)


def test_serie_parse():
    check_serie(SerieResId.parse(serie_sut))


def test_serie_of_id():
    check_serie(SerieResId.of_id(serie_guid))


season_guid = "796d659f-a805-4c96-ad65-9fa805ac96cb"
season_sut = f"http://id.nrk.no/2016/mdb/season/{mrr_guid}"


def check_season(season):
    assert isinstance(season, SeasonResId)
    assert season.id() == season_guid
    assert str(season) == season_sut
    assert isinstance(season.mdb_id.guid, UUID)


def test_season_parse():
    check_season(SeasonResId.parse(season_sut))


def test_season_of_id():
    check_season(SeasonResId.of_id(season_guid))
