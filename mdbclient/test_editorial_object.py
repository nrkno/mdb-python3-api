import pytest

from mdbclient.mdbclient import EditorialObject


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
