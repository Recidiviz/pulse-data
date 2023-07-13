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
"""Functions for generating an Outliers Supervisor Chart asset."""


import attr
import cattrs

from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.outliers.querier.querier import OutlierMetricInfo, TargetStatus
from recidiviz.outliers.types import PersonName
from recidiviz.reporting.asset_generation.types import PayloadBase


@attr.s
class OutliersSupervisorChartPayload(PayloadBase):
    # must match the schema defined by asset generation service
    data: dict = attr.ib()


_converter = cattrs.Converter()
_converter.register_unstructure_hook(PersonName, lambda pn: pn.formatted_first_last)


def _convert_keys(key: str) -> str:
    """transforms from snake to camel while passing through TargetStatus enum values"""
    if key in list(TargetStatus):
        return key
    return snake_to_camel(key)


def prepare_data(data: OutlierMetricInfo) -> dict:
    return convert_nested_dictionary_keys(
        {
            k: v
            for k, v in _converter.unstructure(data).items()
            # only include keys that are required for the payload
            if k in ["target", "other_officers", "highlighted_officers"]
        },
        _convert_keys,
    )
