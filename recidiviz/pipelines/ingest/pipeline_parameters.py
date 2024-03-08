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
"""Class for ingest pipeline parameters"""
import json
from typing import Dict, List, Optional

import attr

from recidiviz.calculator.query.state.dataset_config import state_dataset_for_state_code
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    ingest_view_materialization_results_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.pipeline_parameters import PipelineParameters

INGEST_PIPELINE_NAME = "ingest"


@attr.define(kw_only=True)
class IngestPipelineParameters(PipelineParameters):
    """Class for ingest pipeline parameters"""

    raw_data_table_input: str = attr.ib(validator=attr_validators.is_str)

    @raw_data_table_input.default
    def _raw_data_table_input_default(self) -> str:
        return raw_tables_dataset_for_region(
            state_code=StateCode(self.state_code),
            instance=DirectIngestInstance(self.ingest_instance.upper()),
        )

    ingest_view_results_output: str = attr.ib(validator=attr_validators.is_str)

    @ingest_view_results_output.default
    def _ingest_view_results_output_default(self) -> str:
        return ingest_view_materialization_results_dataset(
            StateCode(self.state_code), DirectIngestInstance(self.ingest_instance)
        )

    ingest_view_results_only: bool = attr.ib(
        default=False,
        validator=attr_validators.is_bool,
        converter=attr.converters.to_bool,
    )

    raw_data_upper_bound_dates_json: str = attr.ib(
        validator=attr_validators.is_non_empty_str
    )

    raw_data_upper_bound_dates: Dict[str, str] = attr.ib()

    @raw_data_upper_bound_dates.default
    def _raw_data_upper_bound_dates(self) -> Dict[str, str]:
        raw_json = json.loads(self.raw_data_upper_bound_dates_json)
        return dict(raw_json.items())

    ingest_views_to_run: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    @property
    def flex_template_name(self) -> str:
        return "ingest"

    def define_output(self) -> str:
        return state_dataset_for_state_code(
            StateCode(self.state_code), DirectIngestInstance(self.ingest_instance)
        )

    def __attrs_post_init__(self) -> None:
        both_are_overwritten = (
            self.output != self.define_output()
            and self.ingest_view_results_output
            != self._ingest_view_results_output_default()
        )
        both_are_default = (
            self.output == self.define_output()
            and self.ingest_view_results_output
            == self._ingest_view_results_output_default()
        )
        if not both_are_overwritten and not both_are_default:
            raise ValueError(
                "Invalid pipeline parameters for output datasets. Only one of the "
                "following is a sandbox dataset (either both must be sandbox or neither): "
                f"output is {self.output}, ingest_view_results_output is {self.ingest_view_results_output}"
            )
        if both_are_default and self.ingest_views_to_run:
            raise ValueError(
                "Invalid pipeline parameters for ingest_views_to_run. Cannot run a subset"
                " of ingest views without specifying a sandbox dataset."
            )

    @classmethod
    def get_sandboxable_dataset_param_names(cls) -> List[str]:
        return [
            *super().get_sandboxable_dataset_param_names(),
            "ingest_view_results_output",
        ]
