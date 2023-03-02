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
"""
    Helper classes for passing parameters to run flex template pipelines.
"""
import abc
from typing import Any, Dict, List, Optional

import attr

from recidiviz.common import attr_validators


# TODO(#19118): add in debug parameters
@attr.define(kw_only=True)
class PipelineParameters:
    """Parent class with base template parameters."""

    # Args passed through to pipeline template
    state_code: str = attr.ib(validator=attr_validators.is_str)
    pipeline: str = attr.ib(validator=attr_validators.is_str)
    output: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)

    # Args used for job configuration
    region: str = attr.ib(validator=attr_validators.is_str)
    job_name: str = attr.ib(validator=attr_validators.is_str)
    machine_type: str = attr.ib(
        default="n1-standard-32", validator=attr_validators.is_str
    )
    disk_gb_size: int = attr.ib(default=200, validator=attr_validators.is_int)
    staging_only: bool = attr.ib(default=False, validator=attr_validators.is_bool)

    @property
    @abc.abstractmethod
    def flex_template_name(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def _template_parameter_keys(self) -> List[str]:
        pass

    def template_parameters(self) -> Dict[str, Any]:
        parameters = {key: getattr(self, key) for key in self._template_parameter_keys}

        # The Flex template expects all parameter values to be strings, so cast all non-null values to strings.
        return {k: str(v) for k, v in parameters.items() if v is not None}


@attr.define(kw_only=True)
class MetricsPipelineParameters(PipelineParameters):
    """Class for metrics pipeline parameters"""

    metric_types: str = attr.ib(validator=attr_validators.is_str)
    calculation_month_count: int = attr.ib(default=-1, validator=attr_validators.is_int)

    @property
    def flex_template_name(self) -> str:
        return "metrics"

    @property
    def _template_parameter_keys(self) -> List[str]:
        """Keys listed in recidiviz/calculator/pipeline/metrics/template_metadata.json."""
        # TODO(#19117): have keys read from flex template metadata json files instead of explicitly listing them

        return [
            "pipeline",
            "output",
            "state_code",
            "metric_types",
            "calculation_month_count",
        ]


@attr.define(kw_only=True)
class SupplementalPipelineParameters(PipelineParameters):
    """Class for supplemental pipeline parameters"""

    @property
    def flex_template_name(self) -> str:
        return "supplemental"

    @property
    def _template_parameter_keys(self) -> List[str]:
        """Keys listed in recidiviz/calculator/pipeline/supplemental/template_metadata.json."""
        return ["pipeline", "output", "state_code"]


@attr.define(kw_only=True)
class NormalizationPipelineParameters(PipelineParameters):
    """Class for normalization pipeline parameters"""

    @property
    def flex_template_name(self) -> str:
        return "normalization"

    @property
    def _template_parameter_keys(self) -> List[str]:
        """Keys listed in recidiviz/calculator/pipeline/normalization/template_metadata.json."""
        return ["pipeline", "output", "state_code"]
