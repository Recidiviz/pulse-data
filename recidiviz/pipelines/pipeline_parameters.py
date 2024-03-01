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
"""Parent class with base template parameters."""
import abc
import argparse
import inspect
import json
import logging
import os.path
import re
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union

import attr
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    WorkerOptions,
)
from attr import Attribute
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    REFERENCE_VIEWS_DATASET,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common import attr_validators
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.utils.script_helpers import run_command
from recidiviz.utils.environment import GCP_PROJECTS, in_test
from recidiviz.utils.params import str_matches_regex_type


class NormalizeSandboxJobName(argparse.Action):
    """Since this is a test run, make sure the job name has a -test suffix."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Any = None,
    ) -> None:
        job_name = values
        if not job_name.endswith("-test"):
            job_name = job_name + "-test"
            logging.info(
                "Appending -test to the job_name because this is a test job: [%s]",
                job_name,
            )
        setattr(namespace, self.dest, job_name)


class ValidateSandboxDataset(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: Any = None,
    ) -> None:
        if values == DATAFLOW_METRICS_DATASET:
            parser.error(
                f"--sandbox_output_dataset argument for test pipelines must be "
                f"different than the standard Dataflow metrics dataset: "
                f"{DATAFLOW_METRICS_DATASET}."
            )
        setattr(namespace, self.dest, values)


PipelineParametersT = TypeVar("PipelineParametersT", bound="PipelineParameters")


@attr.define(kw_only=True)
class PipelineParameters:
    """Parent class with base template parameters."""

    # Args passed through to pipeline template
    project: str = attr.ib(validator=attr_validators.is_str)
    state_code: str = attr.ib(validator=attr_validators.is_str)
    pipeline: str = attr.ib(validator=attr_validators.is_str)
    ingest_instance: str = attr.ib(
        default=DirectIngestInstance.PRIMARY.value, validator=attr_validators.is_str
    )
    output: str = attr.ib(validator=attr_validators.is_str)

    @output.default
    def _output_default(self) -> str:
        return self.define_output()

    reference_view_input: str = attr.ib(
        default=REFERENCE_VIEWS_DATASET, validator=attr_validators.is_str
    )

    # Args used for job configuration
    region: str = attr.ib(validator=attr_validators.is_str)
    job_name: str = attr.ib(validator=attr_validators.is_str)
    machine_type: str = attr.ib(
        default="n1-standard-32", validator=attr_validators.is_str
    )
    disk_gb_size: int = attr.ib(
        default=200, validator=attr_validators.is_int, converter=int
    )
    staging_only: bool = attr.ib(
        default=False, validator=attr_validators.is_bool, converter=bool
    )

    template_metadata_subdir: str = attr.ib(
        default="template_metadata", validator=attr_validators.is_str
    )

    service_account_email: str = attr.ib()

    @service_account_email.default
    def _service_account_email_default(self) -> str:
        return f"direct-ingest-state-{self.state_code.replace('_', '-').lower()}-df@{self.project}.iam.gserviceaccount.com"

    @service_account_email.validator
    def _service_account_email_validator(
        self, _attribute: Attribute, service_account_email: Optional[str]
    ) -> None:
        regex = r"[a-z0-9-]+@[a-z0-9-]+\.iam\.gserviceaccount\.com"
        default_regex = r"[a-z0-9-]+@developer\.gserviceaccount\.com"
        if (
            service_account_email
            and re.match(regex, service_account_email) is None
            and re.match(default_regex, service_account_email) is None
        ):
            raise ValueError(
                f"service_account_email must be a valid service account email address, but was {service_account_email}"
            )

    # These will only be set when the pipeline options are derived
    # from command-line args (when flex pipeline is actually being
    # run in Dataflow).
    apache_beam_pipeline_options: Optional[PipelineOptions] = attr.ib(default=None)

    @classmethod
    @abc.abstractmethod
    def get_sandboxable_dataset_param_names(cls) -> List[str]:
        """Returns a list of dataset parameter names that will be used in the pipeline and can take sandbox prefixes."""
        return ["output", "reference_view_input"]

    @property
    @abc.abstractmethod
    def flex_template_name(self) -> str:
        pass

    @abc.abstractmethod
    def define_output(self) -> str:
        pass

    @classmethod
    def parse_from_args(
        cls: Type[PipelineParametersT], argv: List[str], sandbox_pipeline: bool
    ) -> PipelineParametersT:
        args, _ = cls.parse_args(argv, sandbox_pipeline=sandbox_pipeline)
        apache_beam_pipeline_options = PipelineOptions(
            argv,
            dataflow_service_options=[
                # Prevents VMs from accepting SSH keys that are stored in project metadata.
                # This is an additional measure of security for disallowing remote access to our VM instance
                "block_project_ssh_keys",
                "enable_dynamic_thread_scaling",
            ],
        )
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = True
        worker_options = apache_beam_pipeline_options.view_as(WorkerOptions)
        worker_options.default_sdk_harness_log_level = "WARNING"
        # Collect all parsed arguments and filter out the ones that are not set
        set_kwargs = {k: v for k, v in vars(args).items() if v is not None}
        return cls(
            **set_kwargs, apache_beam_pipeline_options=apache_beam_pipeline_options
        )

    @classmethod
    def parse_args(
        cls, argv: List[str], sandbox_pipeline: bool
    ) -> Tuple[argparse.Namespace, List[str]]:
        """Parses needed arguments for pipeline parameters and pipeline options."""
        parser: argparse.ArgumentParser = argparse.ArgumentParser()
        parser.add_argument(
            "--project",
            type=str,
            help="ID of the GCP project.",
            choices=GCP_PROJECTS if not in_test() else None,
            required=True,
        )

        if sandbox_pipeline:
            parser.add_argument(
                "--job_name",
                dest="job_name",
                type=str,
                help="The name of the pipeline job to be run.",
                required=True,
                action=NormalizeSandboxJobName,
            )
        else:
            parser.add_argument(
                "--job_name",
                dest="job_name",
                type=str,
                help="The name of the pipeline job to be run.",
                required=True,
            )

        parser.add_argument(
            "--region",
            type=str,
            help="The Google Cloud region to run the job on (e.g. us-west1).",
            default="us-west1",
        )

        parser.add_argument(
            "--service_account_email",
            type=str,
            help="The service account under which to use BigQuery capacity for.",
            required=False,
        )

        if sandbox_pipeline:
            username = run_command("git config user.name", timeout_sec=300)
            username = username.replace(" ", "").strip().lower()
            if not username:
                raise ValueError("Found no configured git username")

            template_metadata_subdir = f"template_metadata_dev/{username}"
            parser.add_argument(
                "--template_metadata_subdir",
                dest="template_metadata_subdir",
                type=str,
                help="The template metadata subdirectory to store Flex template launchers.",
                required=False,
                default=template_metadata_subdir,
            )

        with open(
            os.path.join(
                os.path.dirname(inspect.getabsfile(cls)), "template_metadata.json"
            ),
            mode="r",
            encoding="utf-8",
        ) as json_file:
            template_metadata = json.load(json_file)
            for parameter in template_metadata["parameters"]:
                name = parameter["name"]
                is_optional = (
                    parameter["isOptional"] if "isOptional" in parameter else False
                )
                help_text = f"{parameter['label']}. {parameter['helpText']}"

                arg_type: Union[Callable[[Any], str], Type] = (
                    str_matches_regex_type(one(parameter["regexes"]))
                    if "regexes" in parameter
                    else str
                )

                if sandbox_pipeline and name.endswith("output"):
                    parser.add_argument(
                        f"--sandbox_{name}_dataset",
                        # Change output name to match what pipeline args expect
                        dest=name,
                        type=arg_type,
                        help=f"{name} dataset where results should be written to for test jobs.",
                        required=True,
                        action=ValidateSandboxDataset,
                    )
                else:
                    parser.add_argument(
                        f"--{name}",
                        dest=name,
                        type=arg_type,
                        required=not is_optional,
                        help=help_text,
                    )
        return parser.parse_known_args(argv)

    @property
    def template_parameters(self) -> Dict[str, Any]:
        with open(
            os.path.join(
                os.path.dirname(inspect.getabsfile(self.__class__)),
                "template_metadata.json",
            ),
            mode="r",
            encoding="utf-8",
        ) as json_file:
            template_metadata = json.load(json_file)
            parameter_keys = [
                parameter["name"] for parameter in template_metadata["parameters"]
            ]
            parameters = {key: getattr(self, key) for key in parameter_keys}

        # The Flex template expects all parameter values to be strings, so cast all non-null values to strings.
        return {k: str(v) for k, v in parameters.items() if v is not None}

    def update_with_sandbox_prefix(self, sandbox_prefix: str) -> "PipelineParameters":
        # Need to convert underscores to dashes for pipeline job names to be valid
        converted_sandbox_prefix = sandbox_prefix.replace("_", "-")
        return attr.evolve(
            self,
            **{
                dataset_param_name: BigQueryAddressOverrides.format_sandbox_dataset(
                    sandbox_prefix,
                    getattr(self, dataset_param_name),
                )
                for dataset_param_name in self.get_sandboxable_dataset_param_names()
            },
            # Add -test suffix to avoid firing Pagerduty alerts
            job_name=f"{converted_sandbox_prefix}-{self.job_name}-test",
        )

    def template_gcs_path(self, project_id: str) -> str:
        return GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name=f"{project_id}-dataflow-flex-templates",
            blob_name=os.path.join(
                self.template_metadata_subdir, f"{self.flex_template_name}.json"
            ),
        ).uri()

    def flex_template_launch_body(self) -> Dict[str, Any]:
        project_id = self.project
        return {
            "launchParameter": {
                "containerSpecGcsPath": self.template_gcs_path(project_id),
                "jobName": self.job_name,
                "parameters": self.template_parameters,
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
                        # TODO(#27823): add this flag back if it isn't the cause of worker failures or if google resolves the issue
                        # "enable_google_cloud_profiler",
                    ],
                    "network": "default",
                    "subnetwork": f"https://www.googleapis.com/compute/v1/projects/{project_id}/regions/{self.region}/subnetworks/default",
                    "ipConfiguration": "WORKER_IP_PRIVATE",
                    "serviceAccountEmail": self.service_account_email or "",
                },
            }
        }
