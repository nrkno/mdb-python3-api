import json
import math


class Diff:
    def __init__(self):

        """
        Items that have been added in modified
        """
        self.Added = {}
        """
        Removed elements contain the key and their original value.
        """
        self.Removed = {}
        """
        Lists in Modified have the same amount of elements as the source collection, where unchanged elements are
        None. Modified elements will contain the full modified payload 
        """
        self.Modified = {}

    def add_to_added(self, name, elements):
        for element in elements:
            if name not in self.Added:
                self.Added[name] = []
            self.Added[name].append(element)

    def add_to_removed(self, name, elements):
        for element in elements:
            if name not in self.Removed:
                self.Removed[name] = []
            self.Removed[name].append(element)

    def add_to_modified(self, name, elements):
        for element in elements:
            if name not in self.Modified:
                self.Modified[name] = []
            self.Modified[name].append(element)

    def has_removals_only(self):
        return (not self.Modified and not self.Added) and self.Removed

    def has_diff(self):
        return self.Modified or self.Added or self.Removed

    def has_add_modify_diff(self):
        return self.Modified or self.Added

    def changed_v23(self):
        return self.__added_v3() and self.__removed_v3()

    def eliminiate_changed_categories(self):
        if "categories" in self.Added:
            del self.Added["categories"]
        if "modified" in self.Modified:
            del self.Modified["categories"]
        if "categories" in self.Removed:
            del self.Removed["categories"]

    def changed_n58(self):
        return self.__added_n58() or self.__removed_n58()

    def eliminate_changed_v23(self):
        added = self.__indices_of_contributor_v23(self.Added)
        added.reverse()
        for c in added:
            del self.Added['contributors'][c]
        removed = self.__indices_of_contributor_v23(self.Removed)
        removed.reverse()
        for c in removed:
            del self.Removed['contributors'][c]
        self.pack()

    def eliminate_changed_n58(self):
        added = self.__indices_of_contributor_n58(self.Added)
        added.reverse()
        for c in added:
            del self.Added['contributors'][c]
        removed = self.__indices_of_contributor_n58(self.Removed)
        removed.reverse()
        for c in removed:
            del self.Removed['contributors'][c]
        self.pack()

    def pack(self):
        self.__pack_change(self.Added)
        self.__pack_change(self.Modified)
        self.__pack_change(self.Removed)

    @staticmethod
    def __pack_change(dict_):
        todelete = []
        for (k, v) in dict_.items():
            if isinstance(v, list):
                elements = [x for x in v if x]
                if not elements:
                    todelete.append(k)
        for x in todelete:
            del dict_[x]

    def __added_v3(self):
        return self.__contributor_v23(self.Added)

    def __removed_v3(self):
        return self.__contributor_v23(self.Removed)

    def __added_n58(self):
        return self.__contributor_n58(self.Added)

    def __removed_n58(self):
        return self.__contributor_n58(self.Removed)

    def __contributor_v23(self, collection):
        vars_ = self.__indices_of_contributor_v23(collection)
        return [collection.get('contributors', [])[index] for index in vars_]

    def __contributor_n58(self, collection):
        vars_ = self.__indices_of_contributor_n58(collection)
        return [collection.get('contributors', [])[index] for index in vars_]

    @staticmethod
    def __indices_of_contributor_v23(collection):
        return [idx for idx, x in enumerate(collection.get('contributors', [])) if
                x and x.get('role', {}).get('resId') == 'http://authority.nrk.no/role/V23']

    @staticmethod
    def __indices_of_contributor_n58(collection):
        return [idx for idx, x in enumerate(collection.get('contributors', [])) if
                x and x.get('role', {}).get('resId') == 'http://authority.nrk.no/role/N58']

    def explain_diff(self):
        res = ""
        if self.Added:
            res += "Added:\n" + json.dumps(self.Added, indent=4)
        if self.Modified:
            res += "Modified\n" + json.dumps(self.Modified, indent=4)
        if self.Removed:
            res += "Removed\n" + json.dumps(self.Removed, indent=4)
        return res

    @staticmethod
    def explain_contributor_change(contributor):
        if contributor:
            return contributor.get("contact", {}).get("title", "") + " as " + contributor.get("role", {}).get("resId",
                                                                                                              "")
        return ""

    def explain_contributor_changes(self):
        res = ""
        added = self.Added.get('contributors', [])
        if added:
            res += "Added: "
            res += ", ".join([self.explain_contributor_change(x) for x in added])
        removed = self.Removed.get('contributors', [])
        if removed:
            res += " Removed: "
            res += ", ".join([self.explain_contributor_change(x) for x in removed])

        res = res.strip(", ")
        if len(res) > 0:
            res += "\n"
        return res

    def explain_diff_short(self):
        res = self.explain_contributor_changes()
        if self.Added:
            res += "Added: " + " ".join(self.Added.keys())
        if self.Modified:
            res += ", Modified: " + " ".join(self.Modified.keys())
        if self.Removed:
            res += ", Removed: " + " ".join(self.Removed.keys())
        res = res.strip(", ")
        return res


class Differ:
    """
    Diffs to EOs. Please note that some behaviour is defined in the constructor of this class.
    Currently clients wishing different bevhaviour need to construct the differ and set
    these attributes themselves.
    """

    def __init__(self, existing, modified):
        self.existing = existing
        self.modified = modified
        self.diff = Diff()
        self.category_identity_comparator = self.__category_reference_equals
        self.category_value_comparator = self.__category_value_equals
        self.contributors_identity_comparator = self.__contributor_value_based_identity
        self.contributors_value_comparator = self.__contributor_value_equals
        self.subjects_identity_comparator = self.__subject_value_equals
        self.subjects_value_comparator = self.__subject_value_equals
        self.spatials_identity_comparator = self.__spatial_value_identity_comparator
        self.spatials_value_comparator = self.__spatial_value_equals
        self.ignorables = {"title", "shortDescription", "published", "embeddingAllowed", "duration"}

    @staticmethod
    def are_same_coordinate(existing, coord):
        if not existing and not coord:
            return True
        if not existing or not coord:
            return False
        return math.isclose(existing, coord)

    @staticmethod
    def has_same_spatial(existing_meo, coord_):
        return [x for x in existing_meo.get("spatials", []) if Differ.are_same_spatial(x, coord_)]

    @staticmethod
    def are_same_spatial(existing, coord_):
        e_name = existing.get("name")
        c_name = coord_.get("name")
        if e_name and e_name == c_name:
            return True
        if e_name and e_name != c_name:
            return False
        return Differ.are_same_lat_lon(existing, coord_)

    @staticmethod
    def are_same_lat_lon(p1, p2):
        return Differ.are_same_coordinate(p1.get("latitude"), p2.get("latitude")) and Differ.are_same_coordinate(
            p1.get("longitude"),
            p2.get("longitude"))

    '''
        spatial:
            {'resId': 'http://xx.nrk.no/a/b/z2', 'stadnamn': {'resId': 'http://stadnamn.nrk.no/a/b/c'}, 'name': 'Oslo',
             'latitude': 7.123, 'longitude': -1.456}
    '''

    @staticmethod
    def __spatial_mixed_identity_comparator(original, modified):
        if original.get("resId"):
            return original.get("resId") == modified.get("resId")
        return Differ.__spatial_value_identity_comparator(original, modified)

    @staticmethod
    def __spatial_value_identity_comparator(original, modified):
        return Differ.are_same_spatial(original, modified)

    @staticmethod
    def __spatial_value_equals(existing, s):
        return Differ.is_spatial(s) and not Differ.has_same_spatial(existing, s) and existing.get("stadnamn", {}).get(
            "resId") == s.get("stadnamn", {}).get("resId")

    @staticmethod
    def has_spatial(existing, spatial):
        return [cat for cat in existing.get("spatials", []) if Differ.are_same_coordinate(spatial, cat)]

    @staticmethod
    def __category_reference_equals(existing, modified):
        return existing.get("resource") == modified.get("resource")

    @staticmethod
    def __category_value_equals(existing, modified):
        return existing.get("title") == modified.get("title")

    @staticmethod
    def has_reference(existing, reference):
        return [ref for ref in existing.references if
                ref.type == reference.type and ref.reference == reference.reference]

    @staticmethod
    def has_subject(existing, subject):
        return [cat for cat in existing.get("subjects", []) if cat.get("title") == subject.get("title")]

    @staticmethod
    def subject_mixed_identity_comparator(original, modified):
        if original.get("resId"):
            return original.get("resId") == modified.get("resId")
        return Differ.__subject_value_equals(original, modified)

    @staticmethod
    def __subject_value_equals(cat, subject):
        return cat.get("title") == subject.get("title")

    @staticmethod
    def _contributors_id_matcher(c1, c2):
        if "resId" in c1:
            return c1.get("resId") == c2.get("resId")
        return Differ._contributors_name_and_role_matcher(c1, c2)

    @staticmethod
    def _contributors_name_and_role_matcher(c1, c2):
        return c1.get("contact", {}).get("title") == c2.get("contact", {}).get("title") and c1.get("role", {}).get(
            "resId") == c2.get("role", {}).get("resId")

    '''
    {resId: 'http://c1', contact: {title: 'aTitle'}, role: {resId: 'http://aRes'}, characterName: 'Mikke Mus'}
    '''

    @staticmethod
    def __contributor_value_based_identity(c1, c2):
        return c1.get("contact", {}).get("title") == c2.get("contact", {}).get("title") and \
               c1.get("role", {}).get("title") == c2.get("role", {}).get("title")

    # todo: something is wonky about characterName
    @staticmethod
    def __contributor_value_equals(c1, c2):
        return c1.get("contact", {}).get("title") == c2.get("contact", {}).get("title") and \
               c1.get("contact", {}).get("characterName") == c2.get("contact", {}).get("characterName") and \
               c1.get("contact", {}).get("comment") == c2.get("contact", {}).get("comment") and \
               c1.get("contact", {}).get("capacity") == c2.get("contact", {}).get("capacity") and \
               c1.get("role", {}).get("resId") == c2.get("role", {}).get("resId") and \
               c1.get("role", {}).get("title") == c2.get("role", {}).get("title")

    @staticmethod
    def references(sift_publication_event, g4id, clip_id):
        result = []
        if g4id:
            result.append({'type': 'http://id.nrk.no/2016/mdb/reference/psAPI', 'reference': g4id})

        if clip_id:
            result.append({'type': 'http://id.nrk.no/2018/mdb/reference/guri', 'reference': clip_id})

        if sift_publication_event.get("references"):
            result.extend(sift_publication_event["references"])
        return result

    @staticmethod
    def is_wellformed_subject(subject):
        return subject.get("title")

    @staticmethod
    def is_wellformed_contributor(conributor):
        return conributor.title.length > 0

    @staticmethod
    def is_spatial(spatial):
        lon = spatial.get("longitude")
        lat = spatial.get("longitude")
        return spatial.get("name") or (lat and lon and not math.isnan(lat) and not math.isnan(lon))

    def attribute_changes(self):
        original = self.existing
        modified = self.modified

        def is_ignorable(attr_name):
            return attr_name in self.ignorables

        def should_process(attr_name):
            return attr_name != "resId" and attr_name != "created" and attr_name != "lastUpdated" and not is_ignorable(
                attr_name)

        def is_modified(k):
            return k in original and k in modified and modified.get(k) != original[k]

        def is_accepted_type(v):
            return isinstance(v, str) or isinstance(v, int) or isinstance(v, float)

        for key in [k for (k, v) in original.items() if should_process(k) and is_accepted_type(v) and is_modified(k)]:
            self.diff.Modified[key] = modified[key]

        for key in [k for k, v in modified.items() if should_process(k) and k not in original and is_accepted_type(v)]:
            self.diff.Added[key] = modified[key]

        for key in [k for (k, v) in original.items() if
                    k not in modified and is_accepted_type(v) and should_process(k)]:
            self.diff.Removed[key] = original[key]

    def _apply_changes_editorial_object_collections(self):

        def find(collection, comparator, modified_):
            return [cat for cat in collection if comparator(cat, modified_)]

        def has_valued_element(coll):
            return [x for x in coll if x]

        def handle_collections(field, ref_equality_predicate, value_equality_predicate):
            """Updates diff for a collection

            Keyword arguments:
            ref_equality_predicate -- a comparator that matches for reference-equality
                                    (may match on value if references cannot be trusted)
            value_equality_predicate -- compares the value fields of the object.
                                        Should generally not compare fields compared in ref_equality_predicate
            """
            existing_collection = list(self.existing.get(field, []))
            modified_collection = list(self.modified.get(field, []))

            added_items = [c for c in modified_collection if
                           not find(existing_collection, ref_equality_predicate, c)]
            if has_valued_element(added_items):
                self.diff.add_to_added(field, added_items)

            def is_updated_x(ex, modified_):
                return ref_equality_predicate(ex, modified_) and not value_equality_predicate(ex, modified_)

            modified_items = [
                find(modified_collection, is_updated_x, modified_elem)[0] if find(modified_collection, is_updated_x,
                                                                                  modified_elem) else None for
                modified_elem in existing_collection]
            if has_valued_element(modified_items):
                self.diff.add_to_modified(field, modified_items)

            removed_items = [ex if not find(modified_collection, ref_equality_predicate, ex) else None for ex in
                             existing_collection]
            if has_valued_element(removed_items):
                self.diff.add_to_removed(field, removed_items)

        handle_collections("contributors", self.contributors_identity_comparator, self.contributors_value_comparator)
        handle_collections("categories", self.category_identity_comparator, self.category_value_comparator)
        handle_collections("subjects", self.subjects_identity_comparator, self.subjects_value_comparator)
        handle_collections("spatials", self.spatials_identity_comparator, self.spatials_value_comparator)

    def calculate(self):
        self.attribute_changes()
        self._apply_changes_editorial_object_collections()
        return self.diff


'''
Tidslinjer (timeline) - kun på meo
Tagger (subjects)
Steder (spatials)
Kategori (categories)
Medvirkende (Anonym filtreres bort) (contributors)
'''
