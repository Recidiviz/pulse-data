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
from typing import TypeVar, Generic, Type, Any

from google.protobuf.message import Message

ProtoType = TypeVar('ProtoType', bound=Message)


class ProtobufBuilder(Generic[ProtoType]):
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

    def __init__(self, proto_cls: Type[ProtoType]):
        self.proto_cls: Type[ProtoType] = proto_cls
        self.proto: ProtoType = proto_cls()

    def update_args(self, **kwargs: Any) -> 'ProtobufBuilder[ProtoType]':
        """Update proto message with provided args. If args already exist, will
        overwrite with these values.
        """
        return self.compose(self.proto_cls(**kwargs))

    def compose(self, other_proto: ProtoType) -> 'ProtobufBuilder[ProtoType]':
        """Compose proto message object into the proto being built by this
        builder.
        """
        self.proto.MergeFrom(other_proto)
        return self

    def build(self) -> ProtoType:
        """Returns current state of built proto."""
        return self.proto
