# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Util class for building protobuf Message objects."""
from typing import Any, Generic, List, Type, TypeVar

from google.protobuf.message import Message as ProtobufMessage
from proto import Message as ProtoPlusMessage

ProtoPlusType = TypeVar("ProtoPlusType", bound=ProtoPlusMessage)
ProtobufType = TypeVar("ProtobufType", bound=ProtobufMessage)


class ProtobufBuilder(Generic[ProtobufType]):
    """Util class for building protobuf Message objects by composition.

    Usage example:
        obj = ProtobufBuilder(protobuf.MessageClassOfSomeKind).compose(
            protobuf.MessageClassOfSomeKind(arg='val')
        ).update_args(
            some_other_arg=3
        ).build()

        ... is equivalent to:

        obj = protobuf.MessageClassOfSomeKind(
            arg='val',
            some_other_arg=3,
        )
    """

    def __init__(self, proto_cls: Type[ProtobufType]):
        self.proto_cls: Type[ProtobufType] = proto_cls
        self.proto: ProtobufType = proto_cls()

    def update_args(self, **kwargs: Any) -> "ProtobufBuilder":
        """Update proto message with provided args. If args already exist, will
        overwrite with these values.
        """
        return self.compose(self.proto_cls(**kwargs))

    def compose(self, other_proto: ProtobufType) -> "ProtobufBuilder":
        """Compose proto message object into the proto being built by this
        builder.
        """
        self.proto.MergeFrom(other_proto)
        return self

    def clear_field(self, *fields: List[str]) -> "ProtobufBuilder":
        self.proto.ClearField(*fields)
        return self

    def build(self) -> ProtobufType:
        """Returns current state of built proto."""
        return self.proto


class ProtoPlusBuilder(Generic[ProtoPlusType]):
    """Mirrored implementation of ProtobufBuilder but for use with ProtoPlus objects"""

    def __init__(self, proto_cls: Type[ProtoPlusType]):
        self.proto_cls: Type[ProtoPlusType] = proto_cls
        self.proto: ProtoPlusType = proto_cls()

    def update_args(self, **kwargs: Any) -> "ProtoPlusBuilder":
        """Update proto message with provided args. If args already exist, will
        overwrite with these values.
        """
        return self.compose(self.proto_cls(**kwargs))

    def compose(self, other_proto: ProtoPlusMessage) -> "ProtoPlusBuilder":
        """Compose proto message object into the proto being built by this
        builder.
        """
        self.proto_cls.pb(self.proto).MergeFrom(self.proto_cls.pb(other_proto))

        return self

    def build(self) -> ProtoPlusType:
        """Returns current state of built proto."""
        return self.proto

    def clear_field(self, *fields: str) -> "ProtoPlusBuilder":
        for field in fields:
            self.proto_cls.pb(self.proto).ClearField(field)

        return self
