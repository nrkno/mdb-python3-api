def remove_duplicates(pe):
    if "contributors" in pe:
        pe["contributors"] = squash_byvalue_equal_contributors(pe["contributors"])

def squash_byvalue_equal_contributors(contributors):
    seen = set()
    revised = []
    for item in contributors:
        key = __key(item)
        if key not in seen:
            seen.add(key)
            revised.append(item)
    return revised


def __key(item):
    contact = item.get("contact", {})
    role = item.get("role", {})
    role_resid = role.get("resId","")
    if "rest_client" in role_resid:
        raise Exception(f"client generated role resid {str(role)}")
    key_elements = [contact.get("title", ""), role.get("title", ""),
                    item.get("characterName", ""), item.get("comment", ""), item.get("capacity", "")]
    return ":".join(key_elements)
