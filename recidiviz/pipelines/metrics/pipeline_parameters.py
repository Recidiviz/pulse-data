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
"""Class for metrics pipeline parameters"""
from typing import List, Optional

import attr

from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.common import attr_validators
from recidiviz.pipelines.pipeline_parameters import PipelineParameters


@attr.define(kw_only=True)
class MetricsPipelineParameters(PipelineParameters):
    """Class for metrics pipeline parameters"""

    # TODO(#27373): Update to default to normalized_state_dataset_for_state_code() once
    #  the ingest/normalization pipeline outputs all entities to the
    #  us_xx_normalized_state dataset, whether or not they are normalized.
    normalized_input: str = attr.ib(
        default=NORMALIZED_STATE_DATASET, validator=attr_validators.is_str
    )

    person_filter_ids: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    metric_types: str = attr.ib(default="ALL", validator=attr_validators.is_str)
    calculation_month_count: int = attr.ib(
        default=-1, validator=attr_validators.is_int, converter=int
    )

    @property
    def flex_template_name(self) -> str:
        return "metrics"

    def define_output(self) -> str:
        return DATAFLOW_METRICS_DATASET

    @classmethod
    def get_sandboxable_dataset_param_names(cls) -> List[str]:
        return [
            *super().get_sandboxable_dataset_param_names(),
            "normalized_input",
        ]
