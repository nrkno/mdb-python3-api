import os
from typing import Union
from uuid import UUID

TypedId = Union["TypedMdbId", "ResId", str]
UntypedId = Union["ResId", "MdbId", str, UUID]


def as_id(id_: UntypedId) -> str:
    if isinstance(id_, ResId):
        return str(id_.mdb_id)
    return str(id_)


def as_resid(id_: TypedId) -> str:
    return str(id_)


def try_uuid(val):
    try:
        return UUID(val)
    except ValueError:
        print(f"{val} is not a uuid, using string")
        return val


class MdbId:
    def __init__(self, guid: Union[str, UUID]):
        self.guid = guid

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


class TypedMdbId(MdbId):
    def __init__(self, type_, guid: Union[str, UUID]):
        super().__init__(guid)
        self.mdb_type = type_


class ResId:
    def __init__(self, base, mdb_id: TypedMdbId):
        self.base = base
        self.mdb_id: TypedMdbId = mdb_id

    def __str__(self):
        return self.base + "/" + str(self.mdb_id)


class BagResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/bag"

    def __init__(self, mdb_id):
        super().__init__(BagResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "BagResId":
        head, tail = os.path.split(res_id_string)
        if BagResId.BASE == head:
            guid = BagId(try_uuid(tail))
            return BagResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a Bag resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(BagResId.BASE)


# noinspection SpellCheckingInspection
class SerieResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/serie"

    def __init__(self, mdb_id):
        super().__init__(SerieResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "SerieResId":
        head, tail = os.path.split(res_id_string)
        if SerieResId.BASE == head:
            guid = SerieId(try_uuid(tail))
            return SerieResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a serie resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(SerieResId.BASE)


class SeasonResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/season"

    def __init__(self, mdb_id):
        super().__init__(SeasonResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "SeasonResId":
        head, tail = os.path.split(res_id_string)
        if SeasonResId.BASE == head:
            guid = SeasonId(try_uuid(tail))
            return SeasonResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a season resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(SeasonResId.BASE)


class MasterEOResourceResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/masterEOResource"

    def __init__(self, mdb_id):
        super().__init__(MasterEOResourceResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "MasterEOResourceResId":
        head, tail = os.path.split(res_id_string)
        if MasterEOResourceResId.BASE == head:
            guid = MasterEOResourceId(try_uuid(tail))
            return MasterEOResourceResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a MasterEOResource resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(MasterEOResourceResId.BASE)


class MasterEOResId(ResId):
    BASE = bases.get("MasterEOResId")

    def __init__(self, mdb_id: "MasterEOId"):
        super().__init__(MasterEOResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "MasterEOResId":
        head, tail = os.path.split(res_id_string)
        if MasterEOResId.BASE == head:
            guid = MasterEOId(try_uuid(tail))
            return MasterEOResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a MasterEO resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(MasterEOResId.BASE)


class PublicationEventResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/publicationEvent"

    def __init__(self, mdb_id: "PublicationEventId"):
        super().__init__(PublicationEventResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "PublicationEventResId":
        head, tail = os.path.split(res_id_string)
        if PublicationEventResId.BASE == head:
            guid = PublicationEventId(try_uuid(tail))
            return PublicationEventResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a PublicationEvent resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(PublicationEventResId.BASE)


class PublicationMediaObjectResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/publicationMediaObject"

    def __init__(self, mdb_id: "PublicationMediaObjectId"):
        super().__init__(PublicationMediaObjectResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "PublicationMediaObjectResId":
        head, tail = os.path.split(res_id_string)
        if PublicationMediaObjectResId.BASE == head:
            guid = PublicationMediaObjectId(try_uuid(tail))
            return PublicationMediaObjectResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a PublicationMediaObject resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(PublicationMediaObjectResId.BASE)


class MediaObjectResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/mediaObject"

    def __init__(self, mdb_id: "MediaObjectId"):
        super().__init__(MediaObjectResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "MediaObjectResId":
        head, tail = os.path.split(res_id_string)
        if MediaObjectResId.BASE == head:
            guid = MediaObjectId(try_uuid(tail))
            return MediaObjectResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a MediaObject resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(MediaObjectResId.BASE)


class MediaResourceResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/mediaResource"

    def __init__(self, mdb_id):
        super().__init__(MediaResourceResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "MediaResourceResId":
        head, tail = os.path.split(res_id_string)
        if MediaResourceResId.BASE == head:
            guid = MediaResourceId(try_uuid(tail))
            return MediaResourceResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(MediaResourceResId.BASE)


class EssenceResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/essence"

    def __init__(self, mdb_id):
        super().__init__(EssenceResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "EssenceResId":
        head, tail = os.path.split(res_id_string)
        if EssenceResId.BASE == head:
            guid = EssenceId(try_uuid(tail))
            return EssenceResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(EssenceResId.BASE)


class VersionGroupResId(ResId):
    BASE = "http://id.nrk.no/2016/mdb/versionGroup"

    def __init__(self, mdb_id):
        super().__init__(VersionGroupResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "VersionGroupResId":
        head, tail = os.path.split(res_id_string)
        if VersionGroupResId.BASE == head:
            guid = VersionGroupId(try_uuid(tail))
            return VersionGroupResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a VersionGroup resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(VersionGroupResId.BASE)


class TimelineResId(ResId):
    BASE = "http://id.nrk.no/2017/mdb/timeline"

    def __init__(self, mdb_id):
        super().__init__(TimelineResId.BASE, mdb_id)

    @staticmethod
    def parse(res_id_string, strict: bool = True) -> "TimelineResId":
        head, tail = os.path.split(res_id_string)
        if TimelineResId.BASE == head:
            guid = TimelineId(try_uuid(tail))
            return TimelineResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a Timeline resid")

    @staticmethod
    def matches(resid: str):
        return resid.startswith(TimelineResId.BASE)


class MasterEOId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("MasterEO", guid)

    def as_resid(self) -> MasterEOResId:
        return MasterEOResId(self)


class VersionGroupId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("VersionGroup", guid)

    def as_resid(self) -> VersionGroupResId:
        return VersionGroupResId(self)


class PublicationEventId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("PublicationEvent", guid)

    def as_resid(self) -> PublicationEventResId:
        return PublicationEventResId(self)


class MediaObjectId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("MediaObject", guid)

    def as_resid(self) -> MediaObjectResId:
        return MediaObjectResId(self)


class PublicationMediaObjectId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("PublicationMediaObject", guid)

    def as_resid(self) -> PublicationMediaObjectResId:
        return PublicationMediaObjectResId(self)


class EssenceId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Essence", guid)

    def as_resid(self) -> EssenceResId:
        return EssenceResId(self)


class MediaResourceId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("MediaResource", guid)

    def as_resid(self) -> MediaResourceResId:
        return MediaResourceResId(self)


class BagId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Bag", guid)

    def as_resid(self) -> BagResId:
        return BagResId(self)


class MasterEOResourceId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("MasterEOResource", guid)

    def as_resid(self) -> MasterEOResourceResId:
        return MasterEOResourceResId(self)


class SeasonId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Season", guid)

    def as_resid(self) -> SeasonResId:
        return SeasonResId(self)


class SerieId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Serie", guid)

    def as_resid(self) -> SerieResId:
        return SerieResId(self)


class TimelineId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Timeline", guid)

    def as_resid(self) -> TimelineResId:
        return TimelineResId(self)


bases[type(PublicationEventResId)] = "http://id.nrk.no/2016/mdb/publicationEvent"


def parse_res_id(resid: str) -> ResId:
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
    raise ValueError(f"Unknown type {resid}")
