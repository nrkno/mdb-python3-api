import json

import pytest

from mdbclient.mdbclient import EditorialObject, Contributor


@pytest.mark.asyncio
async def test_editorialbject_find_by_subtype():
    eo = EditorialObject({"items": [{"type": "abc", "offset": 10, "subType": "ABC"},
                                    {"type": "cde", "offset": 20, "subType": "CDE"},
                                    {"type": "cde2", "offset": 20, "subType": "CDE2"}]})
    first = eo._reference_collection("items").of_subtype(sub_type="CDE2")
    assert len(first) == 1
    assert first[0]["type"] == "cde2"

    first = eo._reference_collection("items").of_type("cde2")
    assert len(first) == 1
    assert first[0]["subType"] == "CDE2"


def combine_lists(*lists):
    all_ = []
    [all_.extend(x) for x in lists if x]
    return all_


@pytest.mark.asyncio
async def test_editorialbject_find_by_subtype():
    with open("meo_testdata.json") as json_file:
        json_ = json.load(json_file)
        eo = EditorialObject(dict(json_))
        contrs = eo.contributors()
        kjeys = [contr.key() for contr in contrs]
        assert len(kjeys) == 5

        assert len(Contributor.unique(contrs)) == 5
        assert len(Contributor.unique(combine_lists(contrs, contrs))) == 5
