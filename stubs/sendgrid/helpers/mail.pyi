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
from typing import List, Optional

class Attachment:
    def __init__(
        self,
        file_content: Optional[FileContent] = None,
        file_name: Optional[FileName] = None,
        file_type: Optional[FileType] = None,
        disposition: Optional[Disposition] = None,
    ): ...

class Cc(Email): ...

class Disposition:
    def __init__(self, disposition: Optional[str] = None): ...

class Email:
    def __init__(
        self,
        email: Optional[str] = None,
        name: Optional[str] = None,
        subject: Optional[str] = None,
    ): ...

class FileContent:
    def __init__(self, file_content: Optional[str] = None): ...

class FileName:
    def __init__(self, file_nam: Optional[str] = None): ...

class FileType:
    def __init__(self, file_type: Optional[str] = None): ...

class Mail:
    def __init__(
        self,
        from_email: Optional[Email] = None,
        to_emails: Optional[str] = None,
        subject: Optional[str] = None,
        plain_text_content: Optional[str] = None,
        html_content: Optional[str] = None,
        amp_html_content: Optional[str] = None,
    ): ...
    @property
    def attachment(self) -> Attachment: ...
    @attachment.setter
    def attachment(self, attachment: Attachment) -> None: ...
    @property
    def cc(self) -> List[Cc]: ...
    @cc.setter
    def cc(self, cc_emails: List[Cc]) -> None: ...
