#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import codecs
import json
import os

from mdbclient.mdbclient import ApiResponseParser

THIS_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

with codecs.open(os.path.join(THIS_DIRECTORY, 'meo_testdata.json'), mode='r', encoding='iso8859-1') as f:
    meo_testdata = json.loads(f.read())


def test_find_link():
    result = ApiResponseParser.timeline_of_sub_type(meo_testdata, "http://id.nrk.no/2017/mdbclient/timelinetype/Rights")
    link = ApiResponseParser.self_link(result)
    res_id = result.get("resId")
    assert link == "http://localhost:22338/api/timeline/966ccc52-a53f-4ef5-accc-52a53fbef777"
    assert res_id == "http://id.nrk.no/2017/mdbclient/timeline/966ccc52-a53f-4ef5-accc-52a53fbef777"


def test_link_of_type():
    links = ApiResponseParser.find_link(meo_testdata.get("links"), "http://id.nrk.no/2016/mdbclient/relation/subjects")
    assert len(links) == 1
