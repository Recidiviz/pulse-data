# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Utils for working with GCP resource labels"""

from typing import List, Optional

import attrs

from recidiviz.common import attr_validators, google_cloud_attr_validators


@attrs.define(kw_only=True)
class ResourceLabel:
    key: str = attrs.field(
        validator=google_cloud_attr_validators.is_valid_resource_label_key
    )
    value: str = attrs.field(
        validator=google_cloud_attr_validators.is_valid_resource_label_value
    )
    parents: Optional[List["ResourceLabel"]] = attrs.field(
        default=None, validator=attr_validators.is_opt_list
    )

    def flatten_to_dictionary(self) -> dict[str, str]:
        parents = (
            coalesce_resource_labels(*self.parents, should_throw_on_conflict=True)
            if self.parents
            else {}
        )
        return {
            **parents,
            self.key: self.value,
        }


def coalesce_resource_labels(
    *labels: ResourceLabel, should_throw_on_conflict: bool
) -> dict[str, str]:
    """Given a sequence of |labels|, returns the union of all GoogleCloudResourceLabel
    labels as a dictionary, optionally throwing on conflict, otherwise selecting the
    value that comes first in |labels|.
    """
    job_labels: dict[str, str] = {}
    for label in labels:
        for key, value in label.flatten_to_dictionary().items():
            if key in job_labels and value != job_labels[key]:
                if should_throw_on_conflict:
                    raise ValueError(
                        f"Found conflicting labels for key [{key}]: [{value}] and [{job_labels[key]}]"
                    )
                continue

            job_labels[key] = value

    return job_labels
