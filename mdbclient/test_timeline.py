import pytest

from mdbclient.mdbclient import Timeline

timeline = Timeline(
    {"items": [{"resId": "abc", "offset": 10, "title": "ABC"}, {"resId": "cde", "offset": 20, "title": "CDE"},
               {"resId": "cde2", "offset": 20, "title": "CDE2"}]})


@pytest.mark.asyncio
async def test_timeline_item():
    first = timeline.find_item("abc")
    assert first["offset"] == 10


@pytest.mark.asyncio
async def test_timeline_item_title_offset():
    first = timeline.find_index_point_by_title_and_offset("ABC", 10)
    assert first["resId"] == "abc"


@pytest.mark.asyncio
async def test_timeline_item_offset_duration():
    first = timeline.find_index_points_by_offset_and_duration(20, None)
    assert len(first) == 2
    assert first[0]["resId"] == "cde"
    assert first[1]["resId"] == "cde2"


