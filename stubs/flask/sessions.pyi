from typing import Optional

from flask.sessions import SessionMixin

class SessionInterface:
    def open_session(self) -> Optional[SessionMixin]: ...
