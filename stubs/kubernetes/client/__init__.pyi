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
from typing import Optional

from kubernetes.client.models import (
    V1EnvVar,
    V1ObjectMeta,
    V1Pod,
    V1PodList,
    V1PodStatus,
    V1ResourceRequirements,
)

class CoreV1Api:
    def __init__(self) -> None: ...
    def list_namespaced_pod(
        self, namespace: str, field_selector: Optional[str] = None
    ) -> V1PodList: ...
    def delete_namespaced_pod(self, namespace: str, name: str) -> None: ...

__all__ = [
    "CoreV1Api",
    "V1PodStatus",
    "V1ObjectMeta",
    "V1Pod",
    "V1PodList",
    "V1ResourceRequirements",
    "V1EnvVar",
]
