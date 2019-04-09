import copy

import pytest

from mdbclient.tools.diff_calculator import Differ, Diff
from mdbclient.tools.eo_fixup import remove_duplicates

contact_to1 = {
    'resId': 'http://id.nrk.no/2016/mdb/publicationEvent/rest-client/object/18a40a71-c447-431f-a40a-71c447831f12',
    'title': 'ole olsen'}
contact_to2 = {
    'resId': 'http://id.nrk.no/2016/mdb/publicationEvent/rest-client/object/18a40a71-c447-431f-a40a-71c447831f22',
    'title': 'ole nilsen'}
role_to1 = {'resId': "http://authority.nrk.no/role/V23", 'title': 'aTitle', 'groupId': 'aGroup'}
role_to2 = {'resId': "http://authority.nrk.no/role/N01", 'title': 'medvirkende', 'groupId': 'aGroup'}

c1 = {'contact': contact_to1, 'role': role_to1, 'characterName': 'abc', 'comment': 'aContactComment',
      'capacity': 'Contactcapacity'}
c2 = {'contact': contact_to2, 'role': role_to1, 'characterName': 'abc', 'comment': 'aContactComment',
      'capacity': 'Contactcapacity'}


def test_collapse_to_single():
    pe = {"contributors": [c1, c1]}
    remove_duplicates(pe)
    assert len(pe["contributors"]) == 1


def test_diff_edited_value():
    pe = {"contributors": [c1, c1, c2]}
    remove_duplicates(pe)
    assert len(pe["contributors"]) == 2


def diff_character_name():
    mod = copy.deepcopy(c1)
    mod["characterName"] = "xxx"
    pe = {"contributors": [c1, mod]}
    remove_duplicates(pe)
    assert len(pe["contributors"]) == 2

def diff_comment():
    mod = copy.deepcopy(c1)
    mod["comment"] = "xxx"
    pe = {"contributors": [c1, mod]}
    remove_duplicates(pe)
    assert len(pe["contributors"]) == 2

def diff_capacity():
    mod = copy.deepcopy(c1)
    mod["capacity"] = "xxx"
    pe = {"contributors": [c1, mod]}
    remove_duplicates(pe)
    assert len(pe["contributors"]) == 2
