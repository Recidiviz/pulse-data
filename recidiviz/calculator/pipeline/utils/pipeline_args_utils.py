# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Helpers for parsing command-line arguments shared between multiple pipelines."""
import argparse
from typing import List, Optional

from apache_beam.options.pipeline_options import PipelineOptions

from recidiviz.calculator.pipeline.utils.execution_utils import (
    calculation_month_count_arg,
    calculation_end_month_arg,
)
from recidiviz.calculator.query.state.dataset_config import (
    REFERENCE_VIEWS_DATASET,
    DATAFLOW_METRICS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)


def add_shared_pipeline_arguments(
    parser: argparse.ArgumentParser, include_calculation_limit_args: bool = False
) -> None:
    """Adds argument configs to the |parser| for shared pipeline args that do not get passed through to Apache Beam."""

    parser.add_argument(
        "--data_input",
        type=str,
        help="BigQuery dataset to query.",
        default=STATE_BASE_DATASET,
    )

    parser.add_argument(
        "--reference_view_input",
        type=str,
        help="BigQuery reference view dataset to query.",
        default=REFERENCE_VIEWS_DATASET,
    )

    parser.add_argument(
        "--static_reference_input",
        type=str,
        help="BigQuery static reference table dataset to query.",
        default=STATIC_REFERENCE_TABLES_DATASET,
    )

    parser.add_argument(
        "--state_code",
        dest="state_code",
        type=str,
        help="The state_code to include in the calculations.",
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Output dataset to write results to.",
        default=DATAFLOW_METRICS_DATASET,
    )

    parser.add_argument(
        "--person_filter_ids",
        type=int,
        nargs="+",
        help="An optional list of DB person_id values. When present, the pipeline will only calculate "
        "metrics for these people and will not output to BQ.",
    )

    if include_calculation_limit_args:
        # Only for pipelines that may receive these arguments
        parser.add_argument(
            "--calculation_end_month",
            dest="calculation_end_month",
            type=calculation_end_month_arg,
            help="The year and month, formatted in YYYY-MM, specifying the last month for which metrics"
            " should be calculated. If unset, defaults to the current month. Cannot be a month in"
            " the future.",
        )

        parser.add_argument(
            "--calculation_month_count",
            dest="calculation_month_count",
            type=calculation_month_count_arg,
            help="The number of months (including this one) to limit the monthly calculation output to."
            " If set to -1, does not limit the calculations.",
            default=1,
        )


def _add_base_apache_beam_args(parser: argparse.ArgumentParser) -> None:
    """Adds argument configs to the |parser| for pipeline args that get passed through to Apache Beam and may be used to
    derive other args."""

    parser.add_argument(
        "--job_name",
        type=str,
        help="Name of the pipeline calculation job.",
        required=True,
    )

    parser.add_argument(
        "--project", type=str, help="ID of the GCP project.", required=True
    )

    parser.add_argument(
        "--setup_file",
        type=str,
        help="Path to the setup.py file.",
        default="./setup.py",
    )

    parser.add_argument(
        "--region",
        type=str,
        help="The Google Cloud region to run the job on (e.g. us-west1).",
        default="us-west1",
    )

    parser.add_argument(
        "--runner",
        type=str,
        choices=["DirectRunner", "DataflowRunner"],
        default="DataflowRunner",
        help="The pipeline runner that will parse the program"
        " and construct the pipeline",
    )


def _get_parsed_base_apache_beam_args(argv: List[str]) -> argparse.Namespace:
    """Parses and returns the pipeline args that may be used to derive other args."""
    parser = argparse.ArgumentParser()
    _add_base_apache_beam_args(parser)

    # These args are just used interally to derive other args and will not be passed through to Apache Beam
    parser.add_argument(
        "--bucket",
        type=str,
        help="Google Cloud Storage Bucket for staging code and temporary job files created during the "
        "execution of the pipeline. When not set, will be {PROJECT}-dataflow-templates.",
        required=False,
    )

    parser.add_argument(
        "--save_as_template",
        action="store_true",
        help="When set, indicates that this version of the pipeline will be saved as a template in the "
        "templates subdir of the specified |bucket|, or [PROJECT]-dataflow-templates if no bucket "
        "is specified.",
        required=False,
    )

    base_args = parser.parse_args(argv)

    return base_args


def _get_parsed_full_apache_beam_args(
    argv: List[str],
    parsed_project: str,
    parsed_job_name: str,
    parsed_region: str,
    parsed_bucket: Optional[str],
    save_as_template: bool,
) -> argparse.Namespace:
    """Parses and returns the full set of args to pass through to Apache Beam, in the form of an argparse.Namespace
    object."""

    bucket = parsed_bucket if parsed_bucket else f"{parsed_project}-dataflow-templates"

    parser = argparse.ArgumentParser()

    # Must add the base args since we still want these to be parsed and returned in the known_args
    _add_base_apache_beam_args(parser)

    # These args can be passed in, but generally will not and we will generate sane defaults if not
    parser.add_argument(
        "--staging_location",
        type=str,
        help="A Cloud Storage path for Cloud Dataflow to stage"
        " code packages needed by workers executing the"
        " job.",
        default=f"gs://{bucket}/staging/",
    )

    parser.add_argument(
        "--temp_location",
        type=str,
        help="A Cloud Storage path for Cloud Dataflow to stage"
        " temporary job files created during the execution"
        " of the pipeline.",
        default=f"gs://{bucket}/temp/",
    )

    if save_as_template:
        parser.add_argument(
            "--template_location",
            type=str,
            help="A Cloud Storage path for Cloud Dataflow to stage"
            " temporary job files created during the execution"
            " of the pipeline.",
            default=f"gs://{bucket}/templates/{parsed_job_name}",
        )

    parser.add_argument(
        "--subnetwork",
        help="The Compute Engine subnetwork for launching Compute Engine instances to run the "
        "pipeline.",
        default=f"https://www.googleapis.com/compute/v1/projects/{parsed_project}/regions/"
        f"{parsed_region}/subnetworks/default",
    )

    parser.add_argument(
        "--worker_machine_type",
        type=str,
        default="n1-standard-4",
        help="The machine type for all job workers to use. See"
        " available machine types here: https://cloud.google.com/compute/docs/machine-types",
    )

    # NOTE: We set the Compute Engine disk size to 50GB, up from the default of 25GB for shuffle mode. At 25GB we were
    # running out of disk space on certain VMs and Dataflow/Compute Engine did not always handle scaling gracefully. In
    # some cases, jobs would fail with a `EOFError: marshal data too short` error, which we think was a result of some
    # python byte code getting deleted out from under us.
    parser.add_argument(
        "--disk_size_gb",
        type=str,
        default="50",
        help="Disk size for each machine, see "
        "https://cloud.google.com/dataflow/docs/guides/specifying-exec-params"
        "#setting-other-cloud-dataflow-pipeline-options",
    )

    parser.add_argument(
        "--experiments=shuffle_mode=service",
        action="store_true",
        help="Enables service-based Dataflow Shuffle in the pipeline.",
        default=True,
    )

    parser.add_argument(
        "--experiments=use_beam_bq_sink",
        action="store_true",
        help="Uses the new BigQuery sink to utilize the FILE_LOADS write option for batch pipelines"
        " like ours. This avoids hitting a 'Too many sources provided' error for pipelines with"
        " large outputs.",
        default=True,
    )

    parser.add_argument(
        "--network=default",
        action="store_true",
        help="The Compute Engine network for launching Compute Engine instances to run the pipeline. "
        "Ignored when --no_use_public_ips is set and a subnetwork is specified.",
        default=True,
    )

    parser.add_argument(
        "--no_use_public_ips",
        action="store_true",
        help="Specifies that Dataflow workers use private IP addresses for all communication.",
        default=True,
    )

    parsed_args, _ = parser.parse_known_args(argv)

    return parsed_args


def _derive_apache_beam_pipeline_args(argv: List[str]) -> List[str]:
    """Apache Beam pipelines require that we generate a PipelineOptions object using command-line arguments in the same
    format they come in when you look at sys.argv. This is convenient if you're ok passing in all the arguments
    separately from the command line. However, many of the argument values can be derived from other args. In order to
    simplify the number of args we pass in to our scripts, this function takes the base required arguments, and fleshes
    out the args lists with all the extra arguments we can derive from those.
    """

    # First, validate and generate parsed options for arguments that may be used to derive other arguments
    base_args = _get_parsed_base_apache_beam_args(argv)

    # Next, use the base args to derive the rest of the args and generate the full set of apache beam args
    parsed_args = _get_parsed_full_apache_beam_args(
        argv,
        parsed_project=base_args.project,
        parsed_job_name=base_args.job_name,
        parsed_region=base_args.region,
        parsed_bucket=base_args.bucket,
        save_as_template=base_args.save_as_template,
    )

    # Take the parsed args with all defaults added and transform back to a list of strings
    args = []
    for key, value in parsed_args.__dict__.items():
        if isinstance(value, bool):
            args.append(f"--{key}")
        elif isinstance(value, str):
            args.extend([f"--{key}", value])
        elif value is None:
            pass
        else:
            raise ValueError(f"Unknown type [{type(value)}] for key [{key}]")

    return args


def get_apache_beam_pipeline_options_from_args(argv: List[str]) -> PipelineOptions:
    """Generates a PipelineOptions object from a list of command-line args, adding any missing args that can be derived
    from those passed in."""
    return PipelineOptions(_derive_apache_beam_pipeline_args(argv))
