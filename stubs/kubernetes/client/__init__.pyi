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
from typing import Any, Dict, Optional

from kubernetes.client.models import (
    V1Deployment,
    V1EnvVar,
    V1ObjectMeta,
    V1Pod,
    V1PodList,
    V1PodStatus,
    V1PriorityClass,
    V1ResourceRequirements,
)

class CoreV1Api:
    def __init__(self) -> None: ...
    def list_namespaced_pod(
        self,
        namespace: str,
        field_selector: Optional[str] = None,
        label_selector: Optional[str] = None,
    ) -> V1PodList: ...
    def delete_namespaced_pod(self, namespace: str, name: str) -> None: ...
    def create_namespaced_pod(self, namespace: str, body: V1Pod) -> V1Pod: ...
    def delete_collection_namespaced_pod(
        self, namespace: str, label_selector: Optional[str] = None
    ) -> None: ...

class AppsV1Api:
    def __init__(self, api_client: Optional[Any] = None) -> None: ...
    def create_namespaced_deployment(
        self, namespace: str, body: V1Deployment
    ) -> V1Deployment: ...
    def patch_namespaced_deployment(
        self, name: str, namespace: str, body: Dict[str, Any]
    ) -> V1Deployment: ...
    def read_namespaced_deployment(self, name: str, namespace: str) -> V1Deployment: ...

class SchedulingV1Api:
    def __init__(self, api_client: Optional[Any] = None) -> None: ...
    def create_priority_class(self, body: V1PriorityClass) -> V1PriorityClass: ...

__all__ = [
    "CoreV1Api",
    "AppsV1Api",
    "SchedulingV1Api",
    "V1PodStatus",
    "V1ObjectMeta",
    "V1Pod",
    "V1PodList",
    "V1ResourceRequirements",
    "V1EnvVar",
    "V1Deployment",
    "V1PriorityClass",
]
