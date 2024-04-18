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
"""Class for supplemental pipeline parameters"""
from typing import List

import attr

from recidiviz.common import attr_validators
from recidiviz.pipelines.pipeline_parameters import PipelineParameters
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET


@attr.define(kw_only=True)
class SupplementalPipelineParameters(PipelineParameters):
    """Class for supplemental pipeline parameters"""

    output: str = attr.ib(
        validator=attr_validators.is_str, default=SUPPLEMENTAL_DATA_DATASET
    )

    @property
    def flex_template_name(self) -> str:
        return "supplemental"

    @classmethod
    def get_input_dataset_param_names(cls) -> List[str]:
        return ["reference_view_input"]

    @classmethod
    def get_output_dataset_param_names(cls) -> List[str]:
        return ["output"]
