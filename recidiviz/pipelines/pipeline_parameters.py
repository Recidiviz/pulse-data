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
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, TypeVar, Union

import attr
import attrs
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    WorkerOptions,
)
from attr import Attribute
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common import attr_validators
from recidiviz.common.attr_converters import optional_json_str_to_dict
from recidiviz.tools.utils.script_helpers import run_command
from recidiviz.utils.environment import GCP_PROJECTS, in_test
from recidiviz.utils.params import str_matches_regex_type
from recidiviz.utils.types import assert_type

PIPELINE_INPUT_DATASET_OVERRIDES_JSON_ARG_NAME = "input_dataset_overrides_json"
PIPELINE_OUTPUT_SANDBOX_PREFIX_ARG_NAME = "output_sandbox_prefix"

PipelineParametersT = TypeVar("PipelineParametersT", bound="PipelineParameters")


@attr.define(kw_only=True)
class PipelineParameters:
    """Parent class with base template parameters."""

    # Args passed through to pipeline template
    project: str = attr.ib(validator=attr_validators.is_str)
    state_code: str = attr.ib(validator=attr_validators.is_str)
    pipeline: str = attr.ib(validator=attr_validators.is_str)
    output_sandbox_prefix: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str
    )

    input_dataset_overrides_json: Optional[Dict[str, Any]] = attr.ib(
        default=None,
        converter=optional_json_str_to_dict,
        validator=attr_validators.is_opt_dict,
    )

    @property
    def input_dataset_overrides(self) -> Optional[BigQueryAddressOverrides]:
        if self.input_dataset_overrides_json is None:
            return None

        if not self.input_dataset_overrides_json:
            raise ValueError(
                "The input_dataset_overrides_json param was set with an empty "
                "dictionary. Set input_dataset_overrides_json to None if there are "
                "no overrides."
            )

        builder = BigQueryAddressOverrides.Builder(sandbox_prefix=None)
        for (
            original_dataset,
            override_dataset,
        ) in self.input_dataset_overrides_json.items():
            if original_dataset == override_dataset:
                raise ValueError(
                    f"Input dataset override for [{original_dataset}] must be "
                    f"different than the original dataset."
                )
            builder.register_custom_dataset_override(
                original_dataset, override_dataset, force_allow_custom=True
            )
        return builder.build()

    @property
    def output_dataset_overrides(self) -> Optional[BigQueryAddressOverrides]:
        if not self.is_sandbox_pipeline:
            return None

        if not self.output_sandbox_prefix:
            raise ValueError(
                "Found sandbox pipeline where output_sandbox_prefix is not set."
            )

        builder = BigQueryAddressOverrides.Builder(
            sandbox_prefix=self.output_sandbox_prefix
        )
        defaults_params = self._without_overrides()
        for dataset_field in self.get_output_dataset_property_names():
            default_dataset = getattr(defaults_params, dataset_field)
            builder.register_sandbox_override_for_entire_dataset(default_dataset)
        return builder.build()

    def get_input_dataset(self, default_dataset_id: str) -> str:
        if not (overrides := self.input_dataset_overrides):
            return default_dataset_id
        return overrides.get_dataset(default_dataset_id)

    def get_output_dataset(self, default_dataset_id: str) -> str:
        if not self.output_sandbox_prefix:
            return default_dataset_id
        if not (overrides := self.output_dataset_overrides):
            raise ValueError(
                f"Expected output dataset overrides when "
                f"[{self.output_sandbox_prefix}] is set."
            )
        return overrides.get_dataset(default_dataset_id)

    # Args used for job configuration
    region: str = attr.ib(validator=attr_validators.is_str)
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
    def get_input_dataset_property_names(cls) -> List[str]:
        """Returns a list of parameter names that contain dataset_ids for datasets used
        as input to this pipeline
        """

    @classmethod
    @abc.abstractmethod
    def get_output_dataset_property_names(cls) -> List[str]:
        """Returns a list of parameter names that contain dataset_ids for datasets used
        as input to this pipeline
        """

    @property
    @abc.abstractmethod
    def flex_template_name(self) -> str:
        pass

    @abc.abstractmethod
    def _get_base_job_name(self) -> str:
        """Returns the job name that should be used for this pipeline if it were not a
        sandbox run.
        """

    @property
    def job_name(self) -> str:
        base_job_name = self._get_base_job_name()

        if not self.is_sandbox_pipeline:
            return base_job_name

        # Adjust sandbox job name so that it won't trigger alerting
        return self._get_job_name_for_sandbox_job(
            base_job_name, assert_type(self.output_sandbox_prefix, str)
        )

    def __attrs_post_init__(self) -> None:
        if self.is_sandbox_pipeline and not self.output_sandbox_prefix:
            raise ValueError(
                f"This sandbox pipeline must define an output_sandbox_prefix. "
                f"Found non-default values for these fields: "
                f"{self._get_non_default_sandbox_indicator_parameters()}"
            )

    @staticmethod
    def _to_job_name_friendly(s: str) -> str:
        """Converts a string to a version that can be used as a Dataflow job name."""
        return s.lower().replace("_", "-")

    @classmethod
    def _get_job_name_for_sandbox_job(
        cls, original_job_name: str, output_sandbox_prefix: str
    ) -> str:
        job_name = original_job_name
        job_name_friendly_prefix = cls._to_job_name_friendly(output_sandbox_prefix)
        if not job_name.startswith(job_name_friendly_prefix):
            job_name = f"{job_name_friendly_prefix}-{job_name}"
        if not job_name.endswith("-test"):
            logging.info(
                "Appending -test to the job_name because this is a test job: [%s]",
                original_job_name,
            )
            job_name = f"{job_name}-test"
        return job_name

    @classmethod
    @abc.abstractmethod
    def custom_sandbox_indicator_parameters(cls) -> Set[str]:
        """The names of parameters specific to this pipeline subclass which,
        when any are set to non-default values, indicate that this is a sandbox run of
        the pipeline and output must be directed to sandbox datasets. Must be overridden
        by subclass datasets.
        """

    @classmethod
    def all_sandbox_indicator_parameters(cls) -> Set[str]:
        """The names of all parameters this pipeline can be instantiated with which,
        when any are set to non-default values, indicate that this is a sandbox run of
        the pipeline and output must be directed to sandbox datasets.
        """

        return {
            PIPELINE_INPUT_DATASET_OVERRIDES_JSON_ARG_NAME,
            PIPELINE_OUTPUT_SANDBOX_PREFIX_ARG_NAME,
        } | cls.custom_sandbox_indicator_parameters()

    def _get_non_default_sandbox_indicator_parameters(self) -> Set[str]:
        non_default_fields = set()
        for field_name, attribute in attr.fields_dict(type(self)).items():
            if field_name not in self.all_sandbox_indicator_parameters():
                continue
            if attribute.default == attrs.NOTHING:
                raise ValueError(
                    f"Sandbox indicator parameter [{field_name}] must have a default "
                    f"value defined."
                )

            if attribute.default != getattr(self, field_name):
                non_default_fields.add(field_name)
        return non_default_fields

    @property
    def is_sandbox_pipeline(self) -> bool:
        """Returns true if the parameters were instantiated with any values that
        indicate this is a sandbox run of the pipeline.
        """
        return bool(self._get_non_default_sandbox_indicator_parameters())

    def _without_overrides(self: PipelineParametersT) -> PipelineParametersT:
        """Returns a pipeline parameters instance with all values restored to defaults,
        if applicable.
        """
        params_cls = type(self)
        kwargs = {}
        sandbox_indicator_params = self.all_sandbox_indicator_parameters()
        for field_name in attr.fields_dict(params_cls):
            if field_name in sandbox_indicator_params:
                continue
            kwargs[field_name] = getattr(self, field_name)
        return params_cls(**kwargs)

    def check_for_valid_input_dataset_overrides(
        self, reference_query_input_datasets: Set[str]
    ) -> None:
        """Checks that the set of overridden input datasets in
        input_dataset_overrides_json only contains datasets that are actual inputs to
        this pipeline. Must provide a set of |reference_query_input_datasets| which
        contains all datasets that any reference queries run by pipeline read from.
        """
        if not self.input_dataset_overrides_json:
            return

        without_overrides = self._without_overrides()
        all_standard_input_datasets = {
            getattr(without_overrides, input_property)
            for input_property in self.get_input_dataset_property_names()
        } | reference_query_input_datasets

        for original_dataset in self.input_dataset_overrides_json:
            if original_dataset not in all_standard_input_datasets:
                raise ValueError(
                    f"Found original dataset [{original_dataset}] in overrides "
                    f"which is not a dataset this pipeline reads from. "
                    f"Datasets you can override: "
                    f"{sorted(all_standard_input_datasets)}."
                )

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
                if sandbox_pipeline and name == PIPELINE_OUTPUT_SANDBOX_PREFIX_ARG_NAME:
                    # If this pipeline is being run locally, the output sandbox arg
                    # must be set.
                    is_optional = False

                help_text = f"{parameter['label']}. {parameter['helpText']}"

                arg_type: Union[Callable[[Any], str], Type] = (
                    str_matches_regex_type(one(parameter["regexes"]))
                    if "regexes" in parameter
                    else str
                )

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
        return {
            k: (
                json.dumps(v)
                # This arg is parsed into a dictionary via a converter so needs to be
                # dumped back to JSON.
                if k in {PIPELINE_INPUT_DATASET_OVERRIDES_JSON_ARG_NAME}
                else str(v)
            )
            for k, v in parameters.items()
            if v is not None
        }

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
