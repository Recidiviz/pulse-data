"""Parses enum errors from logs.

usage: read_enum_errors.py [--project PROJECT] [--region REGION]
optional arguments:
  --project PROJECT  The gcloud project to search logs.
  --region REGION    The region or comma-separated list of regions to read
                     errors for. Leave empty to read all regions.

This script runs the following to generate an input from production logs:
$ gcloud logging read \
'resource.type="gae_app"
logName="projects/recidiviz-123/logs/app"
"could not parse"' --project recidiviz-123 --format json

Note to look at staging errors replace all 'recidiviz-123's with
'recidiviz-staging'
"""
import argparse
import json
import logging
import pprint
import subprocess
from typing import List, Tuple, Optional


def enum_errors_from_logs(
    log_output: bytes, region: Optional[str]
) -> List[Tuple[str, str, str]]:
    """Extract unique enum errors from the log file"""
    regions = region.split(",") if region else []
    errors: List[Tuple[str, str, str]] = []
    for log in json.loads(log_output):
        enum_type, enum_string = extract_enum_string_type(log["jsonPayload"]["message"])
        region = log["labels"]["region"]

        assert region and enum_type and enum_string
        error = (region, enum_type, enum_string)
        if error not in errors and (not regions or region in regions):
            errors.append(error)
    return errors


def extract_enum_string_type(text: str) -> Tuple[str, str]:
    try:
        assert text.startswith("Could not parse")
        text = text[len("Could not parse") :].strip()
        enum_string = text[: text.find(" when building ")].strip()
        enum_type = text.split("'")[-2].strip()
        return enum_type, enum_string
    except ValueError:
        logging.error("Enum text is off: %s", text)
        return "", ""


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project",
        required=False,
        default="recidiviz-123",
        help="The gcloud project to search logs. " "Defaults to recidiviz-123.",
    )
    parser.add_argument(
        "--region",
        required=False,
        help="The region or comma-separated list of regions "
        "to read errors for. Leave empty to read all "
        "regions.",
    )
    args = parser.parse_args()

    logs = subprocess.check_output(
        [
            "gcloud",
            "logging",
            "read",
            'resource.type="gae_app" '
            f'logName="projects/{args.project}/logs/app" "could not parse"',
            "--freshness=7d",
            "--project",
            args.project,
            "--format",
            "json",
        ]
    )

    enum_errors = enum_errors_from_logs(logs, args.region)
    pprint.pprint(enum_errors)
