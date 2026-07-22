# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Parsed CPU/memory limits for a Kubernetes pod, in canonical numeric units."""
import attr
from kubernetes.client import models as k8s

from recidiviz.common import attr_validators

# Kubernetes memory quantity suffixes -> multiplier in bytes.
_MEMORY_SUFFIXES = {
    "Ki": 2**10,
    "Mi": 2**20,
    "Gi": 2**30,
    "Ti": 2**40,
    "K": 10**3,
    "M": 10**6,
    "G": 10**9,
    "T": 10**12,
}


def _parse_cpu_cores(value: str) -> float:
    """Parses a Kubernetes CPU quantity (e.g. "1000m", "1.5") into cores."""
    return float(value[:-1]) / 1000 if value.endswith("m") else float(value)


def _parse_memory_bytes(value: str) -> float:
    """Parses a Kubernetes memory quantity (e.g. "1.75Gi", "500Mi") into bytes."""
    for suffix, multiplier in _MEMORY_SUFFIXES.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * multiplier
    return float(value)


@attr.define(frozen=True, kw_only=True)
class KubernetesPodComputeResourceLimits:
    """A pod's CPU/memory limits parsed into canonical numeric units, so limits
    drawn from different sources can be compared and combined without juggling the
    various Kubernetes quantity-string suffixes."""

    cpu_cores: float = attr.ib(validator=attr_validators.is_positive_float)
    """CPU limit in cores (e.g. 1.0 for the quantity "1000m")."""

    memory_bytes: float = attr.ib(validator=attr_validators.is_positive_float)
    """Memory limit in bytes (e.g. 1879048192.0 for the quantity "1.75Gi")."""

    @classmethod
    def from_kubernetes_resource_requirements(
        cls, requirements: k8s.V1ResourceRequirements
    ) -> "KubernetesPodComputeResourceLimits":
        """Returns the `limits` of |requirements| parsed into cores and bytes."""
        limits = requirements.limits
        if limits is None:
            raise ValueError(f"No resource limits set on requirements [{requirements}]")
        return cls(
            cpu_cores=_parse_cpu_cores(limits["cpu"]),
            memory_bytes=_parse_memory_bytes(limits["memory"]),
        )

    def to_kubernetes_quantities(self) -> dict[str, str]:
        """Returns these limits as Kubernetes quantity strings, suitable for a
        V1ResourceRequirements `limits`/`requests` mapping. CPU is rendered in
        millicores and memory in whole bytes."""
        return {
            "cpu": f"{round(self.cpu_cores * 1000)}m",
            "memory": f"{round(self.memory_bytes)}",
        }
