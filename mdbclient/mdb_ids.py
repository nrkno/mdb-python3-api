import os
from typing import Union
from uuid import UUID


def _try_uuid(val):
    try:
        return UUID(val)
    except ValueError:
        print(f"{val} is not a uuid, using string")
        return val


class MdbId:
    def __init__(self, guid: Union[str, UUID]):
        self.guid = _try_uuid(guid) if isinstance(guid, str) else guid

    def __str__(self):
        return str(self.guid)

    def __eq__(self, other):
        if isinstance(other, MdbId):
            return self.guid == other.guid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.guid)


bases = {
    "BagResId": "http://id.nrk.no/2016/mdb/bag",
    "SerieResId": "http://id.nrk.no/2016/mdb/serie",
    "MasterEOResId": "http://id.nrk.no/2016/mdb/masterEO"
}


class ResId:
    def __init__(self, base, mdb_id: MdbId):
        self.base = base
        self.mdb_id: MdbId = mdb_id

    def __str__(self) -> str:
        infix = "/" if self.base else ""
        return self.base + infix + str(self.mdb_id)

    def id(self) -> str:
        return str(self.mdb_id)

    @staticmethod
    def of_id(id_string) -> "ResId":
        return ResId("", MdbId(id_string))


class BagResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/bag"

    def __init__(self, mdb_id):
        super().__init__(BagResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "BagResId":
        head, tail = os.path.split(res_id_string)
        if BagResId.BASE == head:
            return BagResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a Bag resid")

    @staticmethod
    def of_id(id_string) -> "BagResId":
        return BagResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(BagResId.BASE) and not "rest-client" in resid


# noinspection SpellCheckingInspection
class SerieResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/serie"

    def __init__(self, mdb_id):
        super().__init__(SerieResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "SerieResId":
        head, tail = os.path.split(res_id_string)
        if SerieResId.BASE == head:
            return SerieResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a serie resid")

    @staticmethod
    def of_id(id_string) -> "SerieResId":
        return SerieResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(SerieResId.BASE)


class SeasonResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/season"

    def __init__(self, mdb_id: MdbId):
        super().__init__(SeasonResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "SeasonResId":
        head, tail = os.path.split(res_id_string)
        if SeasonResId.BASE == head:
            return SeasonResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a season resid")

    @staticmethod
    def of_id(id_string) -> "SeasonResId":
        return SeasonResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(SeasonResId.BASE)


class MasterEOResourceResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/masterEOResource"

    def __init__(self, mdb_id: MdbId):
        super().__init__(MasterEOResourceResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "MasterEOResourceResId":
        head, tail = os.path.split(res_id_string)
        if MasterEOResourceResId.BASE == head:
            return MasterEOResourceResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a MasterEOResource resid")

    @staticmethod
    def of_id(id_string) -> "MasterEOResourceResId":
        return MasterEOResourceResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(MasterEOResourceResId.BASE)


class MasterEOResId(ResId):
    BASE = bases.get("MasterEOResId")

    def __init__(self, mdb_id: MdbId):
        super().__init__(MasterEOResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "MasterEOResId":
        head, tail = os.path.split(res_id_string)
        if MasterEOResId.BASE == head:
            guid = MdbId(tail)
            return MasterEOResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a MasterEO resid")

    @staticmethod
    def of_id(id_string) -> "MasterEOResId":
        return MasterEOResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(MasterEOResId.BASE)


class PublicationEventResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/publicationEvent"

    def __init__(self, mdb_id: MdbId):
        super().__init__(PublicationEventResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "PublicationEventResId":
        head, tail = os.path.split(res_id_string)
        if PublicationEventResId.BASE == head:
            return PublicationEventResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a PublicationEvent resid")

    @staticmethod
    def of_id(id_string) -> "PublicationEventResId":
        return PublicationEventResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(PublicationEventResId.BASE)


class PublicationMediaObjectResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/publicationMediaObject"

    def __init__(self, mdb_id: MdbId):
        super().__init__(PublicationMediaObjectResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "PublicationMediaObjectResId":
        head, tail = os.path.split(res_id_string)
        if PublicationMediaObjectResId.BASE == head:
            return PublicationMediaObjectResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a PublicationMediaObject resid")

    @staticmethod
    def of_id(id_string) -> "PublicationMediaObjectResId":
        return PublicationMediaObjectResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(PublicationMediaObjectResId.BASE)


class MediaObjectResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/mediaObject"

    def __init__(self, mdb_id: MdbId):
        super().__init__(MediaObjectResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "MediaObjectResId":
        head, tail = os.path.split(res_id_string)
        if MediaObjectResId.BASE == head:
            guid = MdbId(tail)
            return MediaObjectResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a MediaObject resid")

    @staticmethod
    def of_id(id_string) -> "MediaObjectResId":
        return MediaObjectResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(MediaObjectResId.BASE)


class MediaResourceResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/mediaResource"

    def __init__(self, mdb_id: MdbId):
        super().__init__(MediaResourceResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "MediaResourceResId":
        head, tail = os.path.split(res_id_string)
        if MediaResourceResId.BASE == head:
            return MediaResourceResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a resid")

    @staticmethod
    def of_id(id_string) -> "MediaResourceResId":
        return MediaResourceResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(MediaResourceResId.BASE)


class EssenceResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/essence"

    def __init__(self, mdb_id: MdbId):
        super().__init__(EssenceResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "EssenceResId":
        head, tail = os.path.split(res_id_string)
        if EssenceResId.BASE == head:
            return EssenceResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a resid")

    @staticmethod
    def of_id(id_string) -> "EssenceResId":
        return EssenceResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(EssenceResId.BASE)


class VersionGroupResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/versionGroup"

    def __init__(self, mdb_id: MdbId):
        super().__init__(VersionGroupResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "VersionGroupResId":
        head, tail = os.path.split(res_id_string)
        if VersionGroupResId.BASE == head:
            return VersionGroupResId(MdbId(tail))
        if strict:
            raise ValueError(f"{res_id_string} is not a VersionGroup resid")

    @staticmethod
    def of_id(id_string) -> "VersionGroupResId":
        return VersionGroupResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(VersionGroupResId.BASE)


class TimelineResId(ResId):
    BASE = "http://id.nrk.no/2017/mdb/timeline"

    def __init__(self, mdb_id: MdbId):
        super().__init__(TimelineResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "TimelineResId":
        head, tail = os.path.split(res_id_string)
        if TimelineResId.BASE == head:
            guid = MdbId(tail)
            return TimelineResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a Timeline resid")

    @staticmethod
    def of_id(id_string) -> "TimelineResId":
        return TimelineResId(MdbId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(TimelineResId.BASE)


bases[type(PublicationEventResId)] = "http://id.nrk.no/2016/mdb/publicationEvent"


def lenient_parse_res_id(resid: str) -> ResId:
    result = try_parse_res_id(resid)
    if not result:
        return ResId.of_id(resid)
    return result


def parse_res_id(resid: str) -> ResId:
    result = try_parse_res_id(resid)
    if not result:
        raise ValueError(f"Unknown type {resid}")
    return result


def try_parse_res_id(resid: str) -> ResId:
    if BagResId.matches(resid):
        return BagResId.parse(resid)
    if SerieResId.matches(resid):
        return SerieResId.parse(resid)
    if SeasonResId.matches(resid):
        return SeasonResId.parse(resid)
    if MasterEOResourceResId.matches(resid):
        return MasterEOResourceResId.parse(resid)
    if MasterEOResId.matches(resid):
        return MasterEOResId.parse(resid)
    if PublicationEventResId.matches(resid):
        return PublicationEventResId.parse(resid)
    if PublicationMediaObjectResId.matches(resid):
        return PublicationMediaObjectResId.parse(resid)
    if MediaObjectResId.matches(resid):
        return MediaObjectResId.parse(resid)
    if MediaResourceResId.matches(resid):
        return MediaResourceResId.parse(resid)
    if EssenceResId.matches(resid):
        return EssenceResId.parse(resid)
    if VersionGroupResId.matches(resid):
        return VersionGroupResId.parse(resid)
    if TimelineResId.matches(resid):
        return TimelineResId.parse(resid)


typemappings = {
    "EssenceAggregate": EssenceResId,
    "MasterEOAggregate": MasterEOResId,
    "MasterEOResourceAggregate": MasterEOResourceResId,
    "MediaObjectAggregate": MediaObjectResId,
    "MediaResourceAggregate": MediaResourceResId,
    "PublicationEventAggregate": PublicationEventResId,
    "PublicationMediaObjectAggregate": PublicationMediaObjectResId,
    "SeasonAggregate": SeasonResId,
    "SerieAggregate": SerieResId,
    "TimelineAggregate": TimelineResId,
    "VersionGroupAggregate": VersionGroupResId
}


def from_aggregate_type(aggregate_type, guid):
    type_ = typemappings.get(aggregate_type)
    return type_.of_id(guid) if type_ else None
