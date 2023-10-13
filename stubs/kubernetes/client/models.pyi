# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
from datetime import datetime
from typing import Dict, List, Optional

class V1PodStatus:
    phase: str
    start_time: Optional[datetime]
    def __init__(self, phase: str, start_time: Optional[datetime]) -> None: ...

class V1ObjectMeta:
    name: str

    def __init__(self, name: str) -> None: ...

class V1Pod:
    metadata: V1ObjectMeta
    status: V1PodStatus

    def __init__(self, metadata: V1ObjectMeta, status: V1PodStatus) -> None: ...

class V1PodList:
    items: List[V1Pod]
    def __init__(self, items: List[V1Pod]) -> None: ...

class V1ResourceRequirements:
    def __init__(
        self, limits: Optional[Dict[str, str]], requests: Optional[Dict[str, str]]
    ) -> None: ...

class V1EnvVar:
    name: str
    value: str

    def __init__(
        self, name: Optional[str] = None, value: Optional[str] = None
    ) -> None: ...
