class FieldDiffResult:
    def __init__(self, added, modified, removed, field_name=None):
        self.field_name = field_name
        self.added = added
        self.modified = modified
        self.removed = removed

    @staticmethod
    def with_modified(field_name, modification):
        return FieldDiffResult(None, modification, None, field_name)

    @staticmethod
    def with_added(field_name, addition):
        return FieldDiffResult(addition, None, None, field_name)

    @staticmethod
    def with_removal(field_name, removal):
        return FieldDiffResult(None, None, removal, field_name)

    @staticmethod
    def unchanged(field_name):
        return FieldDiffResult(None, None, None, field_name)

    def has_diff(self):
        return self.modified or self.added or self.removed

    def has_add_modify_diff(self):
        return self.modified or self.added

    def __print_it(self, item):
        if "name" in item:
            text = item.get("name", str(item))
            return text

        if "title" in item:
            text = item.get("title", str(item))
            return text
        contact = item.get("contact", {})
        role_obj = item.get("role", {})
        title = contact.get("title", "")
        role_str = role_obj.get("title", role_obj.get("resId", ""))
        if title:
            text = title
            if role_str:
                text += " as " + role_str
            return text
        return str(item)

    def explain(self):
        res = []

        def explain_item(action, item):
            if isinstance(item, list):
                for x in item:
                    if x:
                        res.append(f"{self.field_name} {action}: " + self.__print_it(x))
            elif item:
                res.append(f"{self.field_name}{action}: " + str(self.field_name))
            return res

        explain_item("added", self.added)
        explain_item("modified", self.modified)
        explain_item("removed", self.removed)
        return "\n".join(res)

    def apply_primitive_field_to_edit(self, edit):
        if self.added and self.modified:
            raise Exception("Cannot have both added and modified for primitive fields")
        if self.added:
            edit[self.field_name] = self.added
        if self.modified:
            edit[self.field_name] = self.modified
        if self.removed:
            edit[self.field_name] = None


def illustration_changes(existing, modified):
    field_name = "illustration"
    existing_image = existing.get(field_name, {})
    modified_image = modified.get(field_name, {})

    if not existing_image and modified_image:
        return FieldDiffResult.with_added(field_name, modified_image)
    elif existing_image and not modified_image:
        return FieldDiffResult.with_removal(field_name, existing_image)
    elif existing_image.get("identifier") != modified_image.get("identifier"):
        return FieldDiffResult.with_modified(field_name, modified_image)
    else:
        existing_attrs = existing_image.get("illustrationAttributes")
        modified_attrs = modified_image.get("illustrationAttributes")
        if not existing_attrs and modified_attrs:
            return FieldDiffResult.with_modified(field_name, modified_attrs)
        if existing_attrs and not modified_attrs:
            return FieldDiffResult.with_modified(field_name, modified_attrs)
        if existing_attrs != modified_attrs:
            return FieldDiffResult.with_modified(field_name, modified_image)
        elif not existing_attrs and not modified_attrs:
            return FieldDiffResult.unchanged(field_name)
        elif len(existing_attrs) != len(modified_attrs):
            return FieldDiffResult.with_modified(field_name, modified_image)
        else:
            for key in existing_attrs:
                if existing_attrs[key] != modified_attrs[key]:
                    return FieldDiffResult.with_modified(field_name, modified_image)
    return FieldDiffResult.unchanged(field_name)


def __category_reference_equals(existing, modified):
    return existing.get("resId") == modified.get("resId")


def __category_value_equals(existing, modified):
    return existing.get("title") == modified.get("title")


def categories_changes(existing, modified, reference_equals=__category_reference_equals,
                       value_equals=__category_value_equals):
    def find(collection, comparator, modified_):
        return [cat for cat in collection if comparator(cat, modified_)]

    def has_valued_element(coll):
        return [x for x in coll if x]

    def handle_collections(field, ref_equality_predicate, value_equality_predicate):
        existing_collection = list(existing.get(field, []))
        modified_collection = list(modified.get(field, []))

        added_items = [c for c in modified_collection if
                       not find(existing_collection, ref_equality_predicate, c)]

        def is_updated_x(ex, modified_):
            return ref_equality_predicate(ex, modified_) and not value_equality_predicate(ex, modified_)

        modified_items = [
            find(modified_collection, is_updated_x, modified_elem)[0] if find(modified_collection, is_updated_x,
                                                                              modified_elem) else None for
            modified_elem in existing_collection]

        removed_items = [ex if not find(modified_collection, ref_equality_predicate, ex) else None for ex in
                         existing_collection]
        return FieldDiffResult(added_items if has_valued_element(added_items) else None,
                               modified_items if has_valued_element(modified) else None,
                               removed_items if has_valued_element(removed_items) else None, "categories")

    return handle_collections("categories", reference_equals, value_equals)


def attribute_change(original, modified, key):
    if key in original and key not in modified:
        return FieldDiffResult.with_removal(key, original[key])
    if key not in original and key in modified:
        return FieldDiffResult.with_added(key, modified[key])
    if key in original and key in modified and modified.get(key) != original[key]:
        return FieldDiffResult.with_modified(key, modified.get(key))
    return FieldDiffResult.unchanged(key)
