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
    The parameters in the PipelineParameters classes (state_code, pipeline, output, etc) need to match the parameters
    in their respective flex template json files.

    MetricsPipelineParameters -> calculator/pipeline/metrics/template_metadata.json,
    NormalizationPipelineParameters -> calculator/pipeline/normalization/template_metadata.json
    SupplementalPipelineParameters -> calculator/pipeline/supplemental/template_metadata.json

    A parameter change, addition, or deletion must be reflected in both the class and it's respective json file.

"""
import abc
import os.path
from typing import Any, Dict, List, Optional

import attr

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common import attr_validators


@attr.define(kw_only=True)
class PipelineParameters:
    """Parent class with base template parameters."""

    # Args passed through to pipeline template
    state_code: str = attr.ib(validator=attr_validators.is_str)
    pipeline: str = attr.ib(validator=attr_validators.is_str)
    output: Optional[str] = attr.ib(default=None, validator=attr_validators.is_opt_str)
    data_input: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    normalized_input: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    reference_view_input: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    person_filter_ids: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    # Args used for job configuration
    region: str = attr.ib(validator=attr_validators.is_str)
    job_name: str = attr.ib(validator=attr_validators.is_str)
    machine_type: str = attr.ib(
        default="n1-standard-32", validator=attr_validators.is_str
    )
    disk_gb_size: int = attr.ib(default=200, validator=attr_validators.is_int)
    staging_only: bool = attr.ib(default=False, validator=attr_validators.is_bool)

    template_metadata_subdir: str = attr.ib(
        default="template_metadata", validator=attr_validators.is_str
    )

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

    def template_gcs_path(self, project_id: str) -> str:
        return GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name=f"{project_id}-dataflow-flex-templates",
            blob_name=os.path.join(
                self.template_metadata_subdir, f"{self.flex_template_name}.json"
            ),
        ).uri()

    def flex_template_launch_body(self, project_id: str) -> Dict[str, Any]:
        return {
            "launchParameter": {
                "containerSpecGcsPath": self.template_gcs_path(project_id),
                "jobName": self.job_name,
                "parameters": self.template_parameters(),
                # DEFAULTS
                "environment": {
                    "machineType": self.machine_type,
                    "diskSizeGb": self.disk_gb_size,
                    "tempLocation": f"gs://{project_id}-dataflow-templates-scratch/temp/",
                    "stagingLocation": f"gs://{project_id}-dataflow-templates/staging/",
                    "additionalExperiments": [
                        "shuffle-mode=service",
                        "use-beam-bq-sink",
                        "use-runner-v2",
                        "enable_google_cloud_profiler",
                    ],
                    "network": "default",
                    "subnetwork": f"https://www.googleapis.com/compute/v1/projects/{project_id}/regions/{self.region}/subnetworks/default",
                    "ipConfiguration": "WORKER_IP_PRIVATE",
                },
            }
        }


@attr.define(kw_only=True)
class MetricsPipelineParameters(PipelineParameters):
    """Class for metrics pipeline parameters"""

    metric_types: str = attr.ib(validator=attr_validators.is_str)
    static_reference_input: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )
    calculation_month_count: Optional[int] = attr.ib(
        default=None, validator=attr_validators.is_opt_int
    )
    calculation_end_month: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    @property
    def flex_template_name(self) -> str:
        return "metrics"

    @property
    def _template_parameter_keys(self) -> List[str]:
        return [
            "pipeline",
            "state_code",
            "metric_types",
            "calculation_month_count",
            "calculation_end_month",
            "output",
            "static_reference_input",
            "data_input",
            "normalized_input",
            "reference_view_input",
            "person_filter_ids",
        ]


@attr.define(kw_only=True)
class SupplementalPipelineParameters(PipelineParameters):
    """Class for supplemental pipeline parameters"""

    @property
    def flex_template_name(self) -> str:
        return "supplemental"

    @property
    def _template_parameter_keys(self) -> List[str]:
        return [
            "pipeline",
            "state_code",
            "output",
            "data_input",
            "normalized_input",
            "reference_view_input",
            "person_filter_ids",
        ]


@attr.define(kw_only=True)
class NormalizationPipelineParameters(PipelineParameters):
    """Class for normalization pipeline parameters"""

    @property
    def flex_template_name(self) -> str:
        return "normalization"

    @property
    def _template_parameter_keys(self) -> List[str]:
        return [
            "pipeline",
            "state_code",
            "output",
            "data_input",
            "normalized_input",
            "reference_view_input",
            "person_filter_ids",
        ]
