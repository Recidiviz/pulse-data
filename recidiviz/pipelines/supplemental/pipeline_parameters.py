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
from typing import List, Set

import attr

from recidiviz.pipelines.pipeline_names import SUPPLEMENTAL_PIPELINE_NAME
from recidiviz.pipelines.pipeline_parameters import PipelineParameters
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET


@attr.define(kw_only=True)
class SupplementalPipelineParameters(PipelineParameters):
    """Class for supplemental pipeline parameters"""

    @property
    def output(self) -> str:
        return self.get_output_dataset(SUPPLEMENTAL_DATA_DATASET)

    @property
    def flex_template_name(self) -> str:
        return SUPPLEMENTAL_PIPELINE_NAME

    def _get_base_job_name(self) -> str:
        if not self.pipeline.endswith("_supplemental"):
            raise ValueError(
                f"Unexpected supplemental pipeline name [{self.pipeline}]. Must end "
                f"with '_supplemental'. "
            )
        return self._to_job_name_friendly(self.pipeline[: -len("_supplemental")])

    @classmethod
    def custom_sandbox_indicator_parameters(cls) -> Set[str]:
        return set()

    @classmethod
    def get_input_dataset_property_names(cls) -> List[str]:
        return []

    @classmethod
    def get_output_dataset_property_names(cls) -> List[str]:
        return ["output"]
