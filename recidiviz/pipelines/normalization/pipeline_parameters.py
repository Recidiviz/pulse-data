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
from typing import List, Optional

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

    state_data_input: str = attr.ib(
        default=STATE_BASE_DATASET, validator=attr_validators.is_str
    )

    person_filter_ids: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    @property
    def flex_template_name(self) -> str:
        return "normalization"

    def define_output(self) -> str:
        return normalized_state_dataset_for_state_code(StateCode(self.state_code))

    @classmethod
    def get_sandboxable_dataset_param_names(cls) -> List[str]:
        return [
            *super().get_sandboxable_dataset_param_names(),
            "state_data_input",
        ]
