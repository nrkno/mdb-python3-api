from tools.diff_calculator import Differ


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


def test_diff_removed():
    original = {'title': 'foo', 'baz': 'bazt'}
    modified = {'title': 'foo'}
    changes = Differ(original, modified).calculate()
    assert len(changes.Removed) == 1
    assert "baz" in changes.Removed


def test_diff_added_updated():
    original = {'title': 'foo', 'baz': 'bazt'}
    modified = {'baz': 'bazz', 'fizz': 'buzz'}
    changes = Differ(original, modified).calculate()
    assert len(changes.Modified) == 1
    assert changes.Modified['baz'] == 'bazz'
    assert len(changes.Added) == 1
    assert changes.Added['fizz'] == 'buzz'
    assert len(changes.Removed) == 1
    assert "title" in changes.Removed


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
    changes = Differ(original, modified).calculate()
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
        {'resId': 'http://xx.nrk.no/a/b/z2', 'stadnamn': {'resId': 'http://stadnamn.nrk.no/a/b/c'}, 'name': 'Oslo',
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
