import pytest

from mdbclient.tools.diff_calculator import Differ, Diff


def test_diff_edited_value():
    original = {'title': 'foo', 'baz': 'bazz'}
    modified = {'title': 'foo', 'baz': 'bazt'}
    changes = Differ(original, modified).calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified['baz'] == 'bazt'


def test_diff_added():
    original = {'title': 'foo', 'baz': 'bazt'}
    modified = {'title': 'foo', 'baz': 'bazt', 'fizz': 'buzz'}
    changes = Differ(original, modified).calculate()
    assert len(changes.Added) == 1
    assert changes.Added['fizz'] == 'buzz'

def test_apply_attribute_diff():
    diff = Diff()
    diff.Modified["cat"] = "fido"
    sut = {"cat" : "knut"}
    diff.recursive_apply_modifications(sut)
    assert sut["cat"] == "fido"


def test_apply_collection_diff():
    diff = Diff()
    diff.Modified["cat"] = ["fido", None, "Brutus"]
    sut = {"cat" : ["fzz", "ape", "rtrt"]}
    diff.recursive_apply_modifications(sut)
    assert sut["cat"] == ["fido", "ape", "Brutus"]


def test_recursive_apply():
    diff = Diff()
    sut = {"house": "big", "rooms": [{"room1": "big"}, {"room2": "small"}]}
    diff.Modified["cat"] = {"house": "big1", "rooms": [{"room1": "big1"}, {"room2": "small1"}]}
    diff.recursive_apply_modifications(sut)
    assert sut["cat"] == {"house": "big1", "rooms": [{"room1": "big1"}, {"room2": "small1"}]}


def test_recursive_apply_more_complex():
    diff = Diff()
    sut = {"house": "big", "rooms": [{"room1": [{"foo": "bar"}, {"baz": {"bazt": "fizz"}}]}, {"room2": "small"}]}
    diff.Modified["cat"] = {"house": "big1", "rooms": [{"room1": [{"foo": "bar1"}, {"baz": {"bazt": "fizz1"}}]}, {"room2": "small1"}]}
    diff.recursive_apply(diff.Modified, sut)
    assert sut["cat"] == {"house": "big1", "rooms": [{"room1": [{"foo": "bar1"}, {"baz": {"bazt": "fizz1"}}]}, {"room2": "small1"}]}

@pytest.mark.asyncio
async def test_with_change():
    original = {'title': 'foo', 'baz': 'bazt'}
    modified = {'title': 'foo', 'baz': 'bazt', 'fizz': 'buzz'}
    changes = Differ(original, modified).calculate()
    assert len(changes.Added) == 1
    assert changes.Added['fizz'] == 'buzz'
    async def check_it(xz):
        assert xz["fizz"] == "buzz"
    await changes.Added.with_change("fizz", check_it)


@pytest.mark.asyncio
async def test_if_present():
    original = {'title': 'foo', 'baz': 'bazt'}
    modified = {'title': 'foo', 'baz': 'bazt', 'fizz': 'buzz'}
    changes = Differ(original, modified).calculate()
    assert len(changes.Added) == 1
    assert changes.Added['fizz'] == 'buzz'
    async def check_it(xz):
        assert xz == "buzz"
    await changes.Added.if_present("fizz", check_it)


def test_diff_removed():
    original = {'title': 'foo', 'baz': 'bazt'}
    modified = {'title': 'foo'}
    changes = Differ(original, modified).calculate()
    assert len(changes.Removed) == 1
    assert "baz" in changes.Removed

def test_adds():
    diff = Diff()
    sut = {"rooms": [{"room3": {"room3": "small"}}]}
    diff.Added["cat"] = "fritz"
    diff.Added["rooms"] = [{"room1": [{"foo": "bar1"}, {"baz": {"bazt": "fizz1"}}]}, {"room2": "small1"}]

    diff.apply_adds(sut)
    assert sut["cat"] == "fritz"
    assert sut["rooms"] == [{"room3": {"room3": "small"}},{"room1": [{"foo": "bar1"}, {"baz": {"bazt": "fizz1"}}]}, {"room2": "small1"}]


def test_adds_to_empty():
    diff = Diff()
    sut = {}
    diff.Added["cat"] = "fritz"
    diff.Added["rooms"] = [{"room1": [{"foo": "bar1"}, {"baz": {"bazt": "fizz1"}}]}, {"room2": "small1"}]

    diff.apply_adds(sut)
    assert sut["cat"] == "fritz"
    assert sut["rooms"] == [{"room1": [{"foo": "bar1"}, {"baz": {"bazt": "fizz1"}}]}, {"room2": "small1"}]

def test_primitive_valued_fields():
    diff = Diff()
    sut = {}
    diff.Added["cat"] = "fritz"
    diff.Added["rooms"] = [{"room1": [{"foo": "bar1"}, {"baz": {"bazt": "fizz1"}}]}, {"room2": "small1"}]

    actual = diff.Added.primitive_valued_fields()
    assert len(actual) == 1
    assert actual == ["cat"]


def test_diff_added_updated():
    original = {'titttle': 'foo', 'baz': 'bazt'}
    modified = {'baz': 'bazz', 'fizz': 'buzz'}
    changes = Differ(original, modified).calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified['baz'] == 'bazz'
    assert len(changes.Added) == 1
    assert changes.Added['fizz'] == 'buzz'
    assert len(changes.Removed) == 1
    assert "titttle" in changes.Removed


def test_added_categories():
    original = {'title': 'foo', 'categories': [{"resource": "c1"}, {"resource": "c2"}]}
    modified = {'title': 'foo', 'categories': [{"resource": "c1"}, {"resource": "c2"}, {"resource": "c3"}]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Added) == 1
    assert changes.Added['categories'] == [{"resource": "c3"}]


def test_modified_categories():
    original = {'title': 'foo', 'categories': [{"resource": "c1"}, {"resource": "c2"}]}
    modified = {'title': 'foo', 'categories': [{"resource": "c1"}, {"resource": "c2", "title": "øl"}]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified['categories'] == [None, {"resource": "c2", "title": "øl"}]


def test_modified_multiple_categories():
    original = {'title': 'foo',
                'categories': [{"resource": "c1"}, {"resource": "c2"}, {"resource": "c3"}, {"resource": "c4"}]}
    modified = {'title': 'foo',
                'categories': [{"resource": "c1"}, {"resource": "c2", "title": "øl"}, {"resource": "c3"},
                               {"resource": "c4", "title": "vold"}]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified['categories'] == [None, {"resource": "c2", "title": "øl"}, None,
                                              {"resource": "c4", "title": "vold"}]


def test_removed_categories():
    original = {'title': 'foo', 'categories': [{"resource": "c1"}, {"resource": "c2"}, {"resource": "c3"}]}
    modified = {'title': 'foo', 'categories': [{"resource": "c1"}, {"resource": "c2"}]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Removed) == 1
    assert changes.Removed['categories'] == [None, None, {"resource": "c3"}]


def test_add_contributor():
    contr = {'contact': {'title': 'ole olsen', 'characterName': 'abc', 'comment': 'aContactComment',
                         'capacity': 'Contactcapacity'}}
    original = {'title': 'foo'}
    modified = {'title': 'foo', 'contributors': [contr]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Added) == 1
    assert changes.Added['contributors'] == [contr]


def test_add_contributor_with_existing():
    org_contr = {'contact': {'title': 'ole2 olsen', 'characterName': 'abc', 'comment': 'aContactComment',
                             'capacity': 'Contactcapacity'}}
    original = {'title': 'bazbaz', 'contributors': [org_contr]}
    contr = {'contact': {'title': 'ole olsen', 'characterName': 'abc', 'comment': 'aContactComment',
                         'capacity': 'Contactcapacity'}}
    modified = {'title': 'foo', 'contributors': [contr]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Added) == 1
    assert changes.Added['contributors'] == [contr]


def test_modify_existing_contributor():
    org_contr = {'contact': {'title': 'ole olsen', 'characterName': 'abc_org', 'comment': 'aContactComment',
                             'capacity': 'Contactcapacity'}}
    original = {'title': 'foo', 'contributors': [org_contr]}

    innocent_bystander = {'contact': {'title': 'fredf', 'characterName': 'abc', 'comment': 'aContactComment',
                                      'capacity': 'Contactcapacity'}}
    contr = {'contact': {'title': 'ole olsen', 'characterName': 'abc_mod', 'comment': 'aContactComment',
                         'capacity': 'Contactcapacity'}}
    modified = {'title': 'foo', 'contributors': [innocent_bystander, contr]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified['contributors'] == [contr]


def test_modify_existing_contributor_with_resId():
    org_contr = {'contact': {'title': 'ole olsen', 'characterName': 'abc_org', 'comment': 'aContactComment',
                             'capacity': 'Contactcapacity'}}
    original = {'resId': 'http://org1', 'title': 'foo', 'contributors': [org_contr]}

    innocent_bystander = {'contact': {'title': 'fredf', 'characterName': 'abc', 'comment': 'aContactComment',
                                      'capacity': 'Contactcapacity'}}
    contr = {'resId': 'http://org1',
             'contact': {'title': 'ole olsen', 'characterName': 'abc_mod', 'comment': 'aContactComment',
                         'capacity': 'Contactcapacity'}}
    modified = {'title': 'foo', 'contributors': [innocent_bystander, contr]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified['contributors'] == [contr]


def test_modify_existing_contributor_with_ignored_resId():
    innocent_bystander = {'contact': {'title': 'fredf', 'characterName': 'abc', 'comment': 'aContactComment',
                                      'capacity': 'Contactcapacity'}}
    org_contr = {'contact': {'title': 'ole olsen', 'characterName': 'abc_org', 'comment': 'aContactComment',
                             'capacity': 'Contactcapacity'}}
    original = {'resId': 'http://org1', 'title': 'foo', 'contributors': [innocent_bystander, org_contr]}

    contr = {'resId': 'http://org1_same_same_but_different_different',
             'contact': {'title': 'ole olsen', 'characterName': 'abc_mod', 'comment': 'aContactComment',
                         'capacity': 'Contactcapacity'}}
    modified = {'title': 'foo', 'contributors': [contr]}
    differ = Differ(original, modified)
    # ignore resid for identity
    changes = differ.calculate()
    assert len(changes.Modified) == 1
    modified_contributors = changes.Modified['contributors']
    assert modified_contributors == [None, contr]


def test_removed_contributor():
    innocent_bystander = {'contact': {'title': 'fredf', 'characterName': 'abc', 'comment': 'aContactComment',
                                      'capacity': 'Contactcapacity'}}
    org_contr = {'contact': {'title': 'ole olsen', 'characterName': 'abc_org', 'comment': 'aContactComment',
                             'capacity': 'Contactcapacity'}}
    original = {'title': 'foo', 'contributors': [innocent_bystander, org_contr]}

    contr = {'contact': {'title': 'ole olsen', 'characterName': 'abc_mod', 'comment': 'aContactComment',
                         'capacity': 'Contactcapacity'}}
    modified = {'title': 'foo', 'contributors': [innocent_bystander]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Removed) == 1
    assert changes.Removed['contributors'] == [None, org_contr]


def test_subjects_added():
    original = {'subjects': [{'resId': 'http://s1', 'title': 'sub1'}, {'title': 'sub2'}]}
    modified = {'subjects': [{'resId': 'http://s1', 'title': 'sub1'}, {'title': 'sub2'}, {'title': 'sub3'}]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Added) == 1
    assert changes.Added['subjects'] == [{'title': 'sub3'}]


def test_subjects_removed():
    original = {'subjects': [{'resId': 'http://s1', 'title': 'sub1'}, {'title': 'sub2'}, {'title': 'sub3'}]}
    modified = {'subjects': [{'resId': 'http://s1', 'title': 'sub1'}, {'title': 'sub2'}]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Removed) == 1
    assert changes.Removed['subjects'] == [None, None, {'title': 'sub3'}]


def test_subjects_modified_identity():
    original = {'subjects': [{'resId': 'http://s1', 'title': 'sub1'}, {'resId': 'http://sx', 'title': 'sub2'}]}
    modified = {'subjects': [{'resId': 'http://s1', 'title': 'sub1'}, {'resId': 'http://sx', 'title': 'subX'}]}
    differ = Differ(original, modified)
    differ.subjects_identity_comparator = Differ.subject_mixed_identity_comparator
    changes = differ.calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified['subjects'] == [None, {'resId': 'http://sx', 'title': 'subX'}]


def test_subjects_modified_value():
    original = {'subjects': [{'title': 'sub1'}, {'title': 'sub2'}]}
    modified = {'subjects': [{'title': 'sub1'}, {'title': 'subX'}]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Modified) == 0
    assert changes.Added['subjects'] == [{'title': 'subX'}]
    assert changes.Removed['subjects'] == [None, {'title': 'sub2'}]
    assert changes.has_diff()


def test_subjects_removed_2():
    original = {'subjects': [{'resId': 'http://s1', 'title': 'sub1'}, {'title': 'sub2'}, {'title': 'sub3'}]}
    modified = {'subjects': [{'resId': 'http://s1', 'title': 'sub1'}, {'title': 'sub2'}]}
    changes = Differ(original, modified).calculate()
    assert len(changes.Removed) == 1
    assert changes.Removed['subjects'] == [None, None, {'title': 'sub3'}]


def test_spatials_added():
    original = {'spatials': [
        {'resId': 'http://xx.nrk.no/a/b/zz', 'stadnamn': {'resId': 'http://stadnamn.nrk.no/a/b/c'}, 'name': 'Oslo',
         'latitude': 7.123, 'longitude': -1.456}
    ]}
    modified = {'spatials': [
        {'resId': 'http://xx.nrk.no/a/b/z2', 'stadnamn': {'resId': 'http://stadnamn.nrk.no/a/b/c'}, 'name': 'Oslo2',
         'latitude': 7.123, 'longitude': -1.456}
    ]}
    differ = Differ(original, modified)
    changes = differ.calculate()
    assert len(changes.Added) == 1
    assert changes.Added['spatials'] == modified['spatials']


def test_spatials_added_that_is_noop():
    original = {'spatials': [
        {'resId': 'http://xx.nrk.no/a/b/zz', 'stadnamn': {'resId': 'http://stadnamn.nrk.no/a/b/c'}, 'name': 'Oslo',
         'latitude': 7.123, 'longitude': -1.456}
    ]}
    modified = {'spatials': [
        {'resId': 'http://xx.nrk.no/NOTSAMEs', 'stadnamn': {'resId': 'http://stadnamn.nrk.no/a/b/c'}, 'name': 'Oslo',
         'latitude': 7.123, 'longitude': -1.456}
    ]}
    differ = Differ(original, modified)
    differ.spatials_identity_comparator = Differ.are_same_spatial
    changes = differ.calculate()
    assert len(changes.Added) == 0
    assert not changes.has_diff()


def test_spatials_modification():
    original = {'spatials': [
        {'resId': 'http://xx.nrk.no/a/b/zz', 'stadnamn': {'resId': 'http://stadnamn.nrk.no/a/b/c'}, 'name': 'Oslo',
         'latitude': 7.123, 'longitude': -1.456}
    ]}
    modified = {'spatials': [
        {'resId': 'http://xx.nrk.no/NOTSAMEs', 'stadnamn': {'resId': 'http://stadnamn.nrk.no/a/b/FTYEFTE'},
         'name': 'Oslo',
         'latitude': 7.123, 'longitude': -1.456}
    ]}
    differ = Differ(original, modified)
    differ.spatials_identity_comparator = Differ.are_same_spatial
    changes = differ.calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified["spatials"] == modified["spatials"]
    assert changes.has_diff()


test_subjects_modified_value()
'''
test_diff_edited_value()
test_diff_added()
test_diff_removed()
test_diff_added_updated()
test_added_categories()
test_modified_categories()
test_modified_multiple_scategories()
test_removed_categories()
'''
