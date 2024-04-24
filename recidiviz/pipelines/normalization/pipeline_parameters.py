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
"""Class for normalization pipeline parameters"""
from typing import List, Optional, Set

import attr

from recidiviz.calculator.query.state.dataset_config import (
    STATE_BASE_DATASET,
    normalized_state_dataset_for_state_code,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.pipeline_parameters import PipelineParameters


@attr.define(kw_only=True)
class NormalizationPipelineParameters(PipelineParameters):
    """Class for normalization pipeline parameters"""

    @property
    def state_data_input(self) -> str:
        return self.get_input_dataset(STATE_BASE_DATASET)

    person_filter_ids: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    @property
    def output(self) -> str:
        return self.get_output_dataset(
            normalized_state_dataset_for_state_code(StateCode(self.state_code))
        )

    @property
    def flex_template_name(self) -> str:
        return "normalization"

    def _get_base_job_name(self) -> str:
        return self._to_job_name_friendly(f"{self.state_code}-normalization")

    @classmethod
    def custom_sandbox_indicator_parameters(cls) -> Set[str]:
        return {"person_filter_ids"}

    @classmethod
    def get_input_dataset_property_names(cls) -> List[str]:
        return ["state_data_input"]

    @classmethod
    def get_output_dataset_property_names(cls) -> List[str]:
        return ["output"]
