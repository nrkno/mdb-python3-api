def remove_duplicates(pe):
    if "contributors" in pe:
        pe["contributors"] = squash_byvalue_equal_contributors(pe["contributors"])
    if "spatials" in pe:
        pe["spatials"] = squash_byvalue_equal_contributors(pe["spatials"])


def squash_byvalue_equal_contributors(contributors):
    return squash_by_key(contributors, __key)


def squash_byvalue_equal_spatials(spatials):
    return squash_by_key(spatials, _spatials_key)


def squash_by_key(collection, key_func):
    seen = set()
    revised = []
    for item in collection:
        key = key_func(item)
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


def _spatials_key(item):
    name = item.get("name", "NNAME")
    lat = str(item.get("latitude", "NLAT"))
    lon = str(item.get("longitude","NLON"))
    return ":".join([name, lat, lon])
