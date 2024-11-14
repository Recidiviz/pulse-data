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

class V1EmptyDirVolumeSource: ...

class V1Volume:
    name: str
    empty_dir: Optional[V1EmptyDirVolumeSource]

    def __init__(self, *, name: str, empty_dir: V1EmptyDirVolumeSource) -> None: ...

class V1VolumeMount:
    name: str
    mount_path: str

    def __init__(self, name: str, mount_path: str): ...

class V1PodSpec:
    containers: Optional[List[V1Container]]
    volumes: Optional[List[V1Volume]]
    volume_mounts: Optional[List[V1VolumeMount]]
    env: Optional[List[V1EnvVar]]

class V1SecurityContext:
    run_as_non_root: bool

    def __init__(self, run_as_non_root: bool) -> None: ...

class V1HTTPGetAction:
    path: str
    port: int
    scheme: str

    def __init__(self, path: str, port: int, scheme: str) -> None: ...

class V1Probe:
    failure_threshold: int
    http_get: Optional[V1HTTPGetAction]
    period_seconds: int
    success_threshold: int
    timeout_seconds: int

    def __init__(
        self,
        failure_threshold: int,
        http_get: Optional[V1HTTPGetAction],
        period_seconds: int,
        success_threshold: int,
        timeout_seconds: int,
    ) -> None: ...

class V1ExecAction:
    command: List[str]

    def __init__(self, command: List[str]) -> None: ...

class V1LifecycleHandler:
    _exec: Optional[V1ExecAction]

    def __init__(self, _exec: Optional[V1ExecAction]) -> None: ...

class V1Lifecycle:
    post_start: Optional[V1LifecycleHandler]
    pre_stop: Optional[V1LifecycleHandler]

    def __init__(
        self,
        post_start: Optional[V1LifecycleHandler] = None,
        pre_stop: Optional[V1LifecycleHandler] = None,
    ) -> None: ...

class V1Container:
    name: str
    image: str
    command: Optional[List[str]]
    args: Optional[List[str]]
    env: Optional[List[V1EnvVar]]
    volume_mounts: Optional[List[V1VolumeMount]]
    security_context: Optional[V1SecurityContext]
    startup_probe: Optional[V1Probe]
    lifecycle: Optional[V1Lifecycle]

    def __init__(
        self,
        name: str,
        image: str,
        command: Optional[List[str]] = None,
        args: Optional[List[str]] = None,
        volume_mounts: Optional[List[V1VolumeMount]] = None,
        security_context: Optional[V1SecurityContext] = None,
        startup_probe: Optional[V1Probe] = None,
        lifecycle: Optional[V1Lifecycle] = None,
    ) -> None: ...

class V1Pod:
    metadata: V1ObjectMeta
    status: V1PodStatus
    spec: V1PodSpec

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
