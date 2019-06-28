from copy import deepcopy

from mdbclient.tools.diff_functions import illustration_changes, categories_changes, attribute_change

without_illustration = {}

illustration_core = {
    "identifier": "_iXhb8d-6JPD4srMv4-rSA",
    "storageType": {
        "resId": "http://id.nrk.no/2015/kaleido/image",
        "label": "Kaleido",
        "isSuppressed": False
    },
    "resId": "https://kaleido.nrk.no/service/api/1.0/data/_iXhb8d-6JPD4srMv4-rSA"
}

cropping = {"illustrationAttributes": {
    "cropOffsetX": 0.09,
    "cropOffsetY": 0.060728395,
    "cropWidth": 0.91,
    "cropHeight": 0.91
}}


def illustration_of(d1, d2=None):
    if d2:
        return {"illustration": deepcopy(dict(**d1, **d2))}
    return {"illustration": deepcopy(d1)}


def test_illustration_added():
    changes = illustration_changes(without_illustration, illustration_of(illustration_core))
    assert changes.added


def test_illustration_crop_added():
    changes = illustration_changes(illustration_of(illustration_core, {}), illustration_of(illustration_core, cropping))
    assert changes.modified


def test_illustration_crop_modified_offset_x():
    cloned = illustration_of(illustration_core, cropping)
    cloned["illustration"]["illustrationAttributes"]["cropOffsetX"] = 3
    changes = illustration_changes(illustration_of(illustration_core, cropping), cloned)
    assert changes.modified


def test_illustration_crop_modified_offset_y():
    cloned = illustration_of(illustration_core, cropping)
    cloned["illustration"]["illustrationAttributes"]["cropOffsetY"] = 3
    changes = illustration_changes(illustration_of(illustration_core, cropping), cloned)
    assert changes.modified


def test_illustration_crop_modified_crop_width():
    cloned = illustration_of(illustration_core, cropping)
    cloned["illustration"]["illustrationAttributes"]["cropWidth"] = 37
    changes = illustration_changes(illustration_of(illustration_core, cropping), cloned)
    assert changes.modified


def test_illustration_crop_modified_crop_height():
    cloned = illustration_of(illustration_core, cropping)
    cloned["illustration"]["illustrationAttributes"]["cropHeight"] = 3
    changes = illustration_changes(illustration_of(illustration_core, cropping), cloned)
    assert changes.modified
    assert changes.has_diff()
    assert changes.has_add_modify_diff()


def test_illustration_removed():
    changes = illustration_changes(illustration_of(illustration_core), without_illustration)
    assert changes.removed
    assert changes.has_diff()
    assert not changes.has_add_modify_diff()


def test_illustration_modified():
    different_ill = deepcopy(illustration_of(illustration_core))
    different_ill["illustration"]["identifier"] = "badsk8ter"
    changes = illustration_changes(illustration_of(illustration_core), different_ill)
    assert changes.modified


def test_illustration_unchanged():
    changes = illustration_changes(illustration_of(illustration_core), illustration_of(illustration_core))
    assert not changes.has_diff()
    assert not changes.has_add_modify_diff()


def test_added_categories():
    original = {'title': 'foo', 'categories': [{"resId": "c1"}, {"resId": "c2"}]}
    modified = {'title': 'foo', 'categories': [{"resId": "c1"}, {"resId": "c2"}, {"resId": "c3"}]}
    changes = categories_changes(original, modified)
    assert changes.added
    assert changes.added == [{"resId": "c3"}]
    explained = changes.explain()
    assert explained == "categories added: {'resId': 'c3'}"


def test_modified_categories():
    original = {'title': 'foo', 'categories': [{"resId": "c1"}, {"resId": "c2"}]}
    modified = {'title': 'foo', 'categories': [{"resId": "c1"}, {"resId": "c2", "title": "øl"}]}
    changes = categories_changes(original, modified)
    assert changes.modified
    assert changes.modified == [None, {"resId": "c2", "title": "øl"}]
    explained = changes.explain()
    assert explained == "categories modified: øl"  # who hoo bad description!



def test_modified_multiple_categories():
    original = {'title': 'foo',
                'categories': [{"resId": "c1"}, {"resId": "c2"}, {"resId": "c3"}, {"resId": "c4"}]}
    modified = {'title': 'foo',
                'categories': [{"resId": "c1"}, {"resId": "c2", "title": "øl"}, {"resId": "c3"},
                               {"resId": "c4", "title": "vold"}]}
    changes = categories_changes(original, modified)
    assert changes.modified
    assert changes.modified == [None, {"resId": "c2", "title": "øl"}, None,
                                {"resId": "c4", "title": "vold"}]


def test_removed_categories():
    original = {'title': 'foo', 'categories': [{"resId": "c1"}, {"resId": "c2"}, {"resId": "c3"}]}
    modified = {'title': 'foo', 'categories': [{"resId": "c1"}, {"resId": "c2"}]}
    changes = categories_changes(original, modified)
    assert changes.removed
    assert changes.removed == [None, None, {"resId": "c3"}]


def test_attribute_change_modified():
    original = {'title': 'foo', 'baz': 'bazz'}
    modified = {'title': 'foo', 'baz': 'bazt'}
    changes = attribute_change(original, modified, "baz")
    assert changes.added is None
    assert changes.modified == 'bazt'
    assert changes.removed is None


def test_attribute_change_removed():
    original = {'title': 'foo', 'baz': 'bazz'}
    modified = {'title': 'foo'}
    changes = attribute_change(original, modified, "baz")
    assert changes.added is None
    assert changes.modified is None
    assert changes.removed == 'bazz'


def test_attribute_change_added():
    original = {'title': 'foo'}
    modified = {'title': 'foo', 'baz': 'bazt'}
    changes = attribute_change(original, modified, "baz")
    assert changes.added == 'bazt'
    assert changes.modified is None
    assert changes.removed is None

def test_attribute_no_change():
    original = {'title': 'foo', 'baz': 'bazt'}
    modified = {'title': 'foo', 'baz': 'bazt'}
    changes = attribute_change(original, modified, "baz")
    assert changes.added is None
    assert changes.modified is None
    assert changes.removed is None
