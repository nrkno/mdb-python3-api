import os
from abc import abstractmethod
from typing import Union
from uuid import UUID

TypedId = Union["TypedMdbId", "ResId", str]
UntypedId = Union["ResId", "MdbId", str, UUID]


def as_id(id_: UntypedId) -> str:
    if isinstance(id_, ResId):
        return str(id_.mdb_id)
    return str(id_)


def as_resid(id_: TypedId) -> str:
    if isinstance(id_, TypedMdbId):
        return id_.as_resid()
    return str(id_)


def try_uuid(val):
    try:
        return UUID(val)
    except ValueError:
        print(f"{val} is not a uuid, using string")
        return val


class MdbId:
    def __init__(self, guid: Union[str, UUID]):
        self.guid = try_uuid(guid) if isinstance(guid, str) else guid

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

    @abstractmethod
    def as_resid(self) -> str:
        raise NotImplementedError()


class ResId:
    def __init__(self, base, mdb_id: TypedMdbId):
        self.base = base
        self.mdb_id: TypedMdbId = mdb_id

    def __str__(self) -> str:
        return self.base + "/" + str(self.mdb_id)

    def id(self) -> str:
        return str(self.mdb_id)


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
    def of_id(id_string) -> "BagResId":
        return BagResId(BagId(id_string))

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
    def of_id(id_string) -> "SerieResId":
        return SerieResId(SerieId(id_string))

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
    def of_id(id_string) -> "SeasonResId":
        return SeasonResId(SeasonId(id_string))

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
    def of_id(id_string) -> "MasterEOResourceResId":
        return MasterEOResourceResId(MasterEOResourceId(id_string))

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
    def of_id(id_string) -> "MasterEOResId":
        return MasterEOResId(MasterEOId(id_string))

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
            guid = PublicationEventId(tail)
            return PublicationEventResId(guid)
        if strict:
            raise ValueError(f"{res_id_string} is not a PublicationEvent resid")

    @staticmethod
    def of_id(id_string) -> "PublicationEventResId":
        return PublicationEventResId(PublicationEventId(id_string))

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
    def of_id(id_string) -> "PublicationMediaObjectResId":
        return PublicationMediaObjectResId(PublicationMediaObjectId(id_string))


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
    def of_id(id_string) -> "MediaObjectResId":
        return MediaObjectResId(MediaObjectId(id_string))

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
    def of_id(id_string) -> "MediaResourceResId":
        return MediaResourceResId(MediaResourceId(id_string))

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
    def of_id(id_string) -> "EssenceResId":
        return EssenceResId(EssenceId(id_string))

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
    def of_id(id_string) -> "VersionGroupResId":
        return VersionGroupResId(VersionGroupId(id_string))

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
    def of_id(id_string) -> "TimelineResId":
        return TimelineResId(TimelineId(id_string))

    @staticmethod
    def matches(resid: str):
        return resid.startswith(TimelineResId.BASE)


class MasterEOId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("MasterEO", guid)

    def as_resid(self) -> str:
        return str(MasterEOResId(self))

    @staticmethod
    def parse(resid_or_id) -> "MasterEOId":
        if MasterEOResId.matches(resid_or_id):
            resid = MasterEOResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return MasterEOId(resid_or_id)


class VersionGroupId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("VersionGroup", guid)

    def as_resid(self) -> str:
        return str(VersionGroupResId(self))

    @staticmethod
    def parse(resid_or_id) -> "VersionGroupId":
        if VersionGroupResId.matches(resid_or_id):
            resid = VersionGroupResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return VersionGroupId(resid_or_id)


class PublicationEventId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("PublicationEvent", guid)

    def as_resid(self) -> str:
        return str(PublicationEventResId(self))

    @staticmethod
    def parse(resid_or_id) -> "PublicationEventId":
        if PublicationEventResId.matches(resid_or_id):
            resid = PublicationEventResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return PublicationEventId(resid_or_id)


class MediaObjectId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("MediaObject", guid)

    def as_resid(self) -> str:
        return str(MediaObjectResId(self))

    @staticmethod
    def parse(resid_or_id) -> "MediaObjectId":
        if MediaObjectResId.matches(resid_or_id):
            resid = MediaObjectResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return MediaObjectId(resid_or_id)


class PublicationMediaObjectId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("PublicationMediaObject", guid)

    def as_resid(self) -> str:
        return str(PublicationMediaObjectResId(self))

    @staticmethod
    def parse(resid_or_id) -> "PublicationMediaObjectId":
        if PublicationMediaObjectResId.matches(resid_or_id):
            resid = PublicationMediaObjectResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return PublicationMediaObjectId(resid_or_id)


class EssenceId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Essence", guid)

    def as_resid(self) -> str:
        return str(EssenceResId(self))

    @staticmethod
    def parse(resid_or_id) -> "EssenceId":
        if EssenceResId.matches(resid_or_id):
            resid = EssenceResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return EssenceId(resid_or_id)


class MediaResourceId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("MediaResource", guid)

    def as_resid(self) -> str:
        return str(MediaResourceResId(self))

    @staticmethod
    def parse(resid_or_id) -> "MediaResourceId":
        if MediaResourceResId.matches(resid_or_id):
            resid = MediaResourceResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return MediaResourceId(resid_or_id)


class BagId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Bag", guid)

    def as_resid(self) -> str:
        return str(BagResId(self))

    @staticmethod
    def parse(resid_or_id) -> "BagId":
        if BagResId.matches(resid_or_id):
            resid = BagResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return BagId(resid_or_id)


class MasterEOResourceId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("MasterEOResource", guid)

    def as_resid(self) -> str:
        return str(MasterEOResourceResId(self))

    @staticmethod
    def parse(resid_or_id) -> "MasterEOResourceId":
        if MasterEOResourceResId.matches(resid_or_id):
            resid = MasterEOResourceResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return MasterEOResourceId(resid_or_id)


class SeasonId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Season", guid)

    def as_resid(self) -> str:
        return str(SeasonResId(self))

    @staticmethod
    def parse(resid_or_id) -> "SeasonId":
        if SeasonResId.matches(resid_or_id):
            resid = SeasonResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return SeasonId(resid_or_id)


class SerieId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Serie", guid)

    def as_resid(self) -> str:
        return str(SerieResId(self))

    @staticmethod
    def parse(resid_or_id) -> "SerieId":
        if SerieResId.matches(resid_or_id):
            resid = SerieResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return SerieId(resid_or_id)


class TimelineId(TypedMdbId):
    def __init__(self, guid):
        super().__init__("Timeline", guid)

    def as_resid(self) -> str:
        return str(TimelineResId(self))

    @staticmethod
    def parse(resid_or_id) -> "TimelineId":
        if TimelineResId.matches(resid_or_id):
            resid = TimelineResId.parse(resid_or_id)
            # noinspection PyTypeChecker
            return resid.mdb_id
        return TimelineId(resid_or_id)


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

