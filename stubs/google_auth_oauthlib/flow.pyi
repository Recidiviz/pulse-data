# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

from typing import Any, List

from google.auth.credentials import Credentials

_DEFAULT_AUTH_PROMPT_MESSAGE: str
_DEFAULT_WEB_SUCCESS_MESSAGE: str

class InstalledAppFlow:
    @classmethod
    def from_client_secrets_file(
        cls, client_secrets_file: str, scopes: List[str], **kwargs: Any
    ) -> "InstalledAppFlow": ...
    def run_local_server(
        self,
        host: str = "localhost",
        port: int = 8080,
        authorization_prompt_message: str = _DEFAULT_AUTH_PROMPT_MESSAGE,
        success_message: str = _DEFAULT_WEB_SUCCESS_MESSAGE,
        open_browser: bool = True,
        **kwargs: Any
    ) -> Credentials: ...
