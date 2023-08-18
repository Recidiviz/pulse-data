from typing import Any, Dict, Optional

from google.protobuf.message import Message as ProtobufMessage

class Message:
    def __init__(self, mapping: Optional[Dict] = None, **kwargs: Any) -> None: ...
    @staticmethod
    def pb(obj: Any) -> ProtobufMessage: ...
