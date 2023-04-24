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
import attr

from recidiviz.calculator.pipeline.pipeline_parameters import PipelineParameters
from recidiviz.calculator.query.state.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.common.constants.states import StateCode


@attr.define(kw_only=True)
class NormalizationPipelineParameters(PipelineParameters):
    """Class for normalization pipeline parameters"""

    @property
    def flex_template_name(self) -> str:
        return "normalization"

    def define_output(self) -> str:
        return normalized_state_dataset_for_state_code(StateCode(self.state_code))
