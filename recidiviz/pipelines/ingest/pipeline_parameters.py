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
from typing import Dict, List, Optional, Set

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
    normalized_state_dataset_for_state_code,
    state_dataset_for_state_code,
)
from recidiviz.pipelines.pipeline_parameters import PipelineParameters


@attr.define(kw_only=True)
class IngestPipelineParameters(PipelineParameters):
    """Class for ingest pipeline parameters"""

    raw_data_source_instance: str = attr.ib(
        default=DirectIngestInstance.PRIMARY.value, validator=attr_validators.is_str
    )

    @property
    def raw_data_table_input(self) -> str:
        return self.get_input_dataset(
            raw_tables_dataset_for_region(
                state_code=StateCode(self.state_code),
                instance=DirectIngestInstance(self.raw_data_source_instance.upper()),
            )
        )

    @property
    def ingest_view_results_output(self) -> str:
        return self.get_output_dataset(
            default_dataset_id=ingest_view_materialization_results_dataset(
                StateCode(self.state_code)
            )
        )

    @property
    def pre_normalization_output(self) -> str:
        return self.get_output_dataset(
            default_dataset_id=state_dataset_for_state_code(StateCode(self.state_code))
        )

    @property
    def normalized_output(self) -> str:
        return self.get_output_dataset(
            default_dataset_id=normalized_state_dataset_for_state_code(
                StateCode(self.state_code)
            )
        )

    # If set to true, will only run the pipeline through ingest view result generation
    # and materialization steps, but will not run ingest mappings or normalization.
    ingest_view_results_only: bool = attr.ib(
        default=False,
        validator=attr_validators.is_bool,
        converter=attr.converters.to_bool,
    )

    # If set to true, will only run the pipeline through ingest view and mapping steps
    # but will not run normalization.
    pre_normalization_only: bool = attr.ib(
        default=False,
        validator=attr_validators.is_bool,
        converter=attr.converters.to_bool,
    )

    raw_data_upper_bound_dates_json: str = attr.ib(
        validator=attr_validators.is_non_empty_str
    )

    # This maps a raw data table to the max update_datetime
    # from the most recent completed raw data import of that table.
    # This allows us to correctly filter out partial data
    # if ingest and raw data import are happening concurrently.
    raw_data_upper_bound_dates: Dict[str, str] = attr.ib()

    @raw_data_upper_bound_dates.default
    def _raw_data_upper_bound_dates(self) -> Dict[str, str]:
        raw_json = json.loads(self.raw_data_upper_bound_dates_json)
        return dict(raw_json.items())

    # This is the user-provided filter for which ingest views to run
    ingest_views_to_run: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        if self.ingest_view_results_only and self.pre_normalization_only:
            raise ValueError(
                "Can only set the ingest_view_results_only or pre_normalization_only "
                "but not both"
            )

    def launchable_ingest_views_to_run(
        self, all_launchable_views: list[str]
    ) -> list[str]:
        """This is the full list of ingest views to process in this pipeline, with any
        user filter applied, if relevant.
        """
        return (
            self.ingest_views_to_run.split(" ")
            if self.ingest_views_to_run
            else all_launchable_views
        )

    @property
    def flex_template_name(self) -> str:
        return "ingest"

    def _get_base_job_name(self) -> str:
        return self._to_job_name_friendly(f"{self.state_code}-ingest")

    @classmethod
    def custom_sandbox_indicator_parameters(cls) -> Set[str]:
        return {
            "raw_data_source_instance",
            "ingest_views_to_run",
            "ingest_view_results_only",
            "pre_normalization_only",
        }

    @classmethod
    def get_input_dataset_property_names(cls) -> List[str]:
        return ["raw_data_table_input"]

    @classmethod
    def get_output_dataset_property_names(cls) -> List[str]:
        return [
            "ingest_view_results_output",
            "pre_normalization_output",
            "normalized_output",
        ]
