# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

"""Generates a CSV with data about all agencies with Publisher accounts.
Includes fields like "num_record_with_data", "num_metrics_configured", etc.

python -m recidiviz.tools.justice_counts.generate_agency_summary_csv \
  --project-id=PROJECT_ID \
"""

import argparse
from collections import defaultdict
from itertools import groupby
from typing import Dict, List

import pandas as pd

from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

NUM_RECORDS_WITH_DATA = "num_records_with_data"
NUM_METRICS_WITH_DATA = "num_metrics_with_data"
NUM_METRICS_CONFIGURED = "num_metrics_configured"
NUM_METRICS_AVAILABLE = "num_metrics_available"
NUM_METRICS_UNAVAILABLE = "num_metrics_unavailable"
IS_ALPHA_PARTNER = "is_alpha_partner"

# To add:
# NUM_METRICS_UNCONFIGURED = "num_metrics_unconfigured"
# NUM_METRICS_DEFINED = "num_metrics_defined"
# LAST_LOGIN = "last_login"

# These are the IDs of our alpha partner agencies in production
ALPHA_PARTNER_IDS = {74, 75, 73, 77, 79, 80, 81, 83, 84, 85}


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )

    return parser


def summarize(datapoints: List[schema.Datapoint]) -> Dict[str, int]:
    """Given a list of Datapoints belonging to a particular agency,
    return a dictionary containing summary statistics.
    """
    report_id_to_datapoints = defaultdict(list)
    metric_key_to_datapoints = defaultdict(list)
    metrics_configured = set()
    metrics_available = set()
    metrics_unavailable = set()

    for datapoint in datapoints:
        # Process report datapoints (i.e. those that contain data for a time period)
        if datapoint["is_report_datapoint"] and datapoint["value"] is not None:
            # Group datapoints by report (i.e. time period)
            report_id_to_datapoints[datapoint["report_id"]].append(datapoint)
            # Group datapoints by metric
            metric_key_to_datapoints[datapoint["metric_definition_key"]].append(
                datapoint
            )
        # Process non-report datapoints (i.e. tthose that contain info about metric configuration)
        elif (
            not datapoint["is_report_datapoint"]
            # The following filters to non-report datapoints that contain information about whether
            # the top-level metric is turned on or off
            and not datapoint["dimension_identifier_to_member"]
            and not datapoint["context_key"]
            and not datapoint["includes_excludes_key"]
            and datapoint["enabled"] is not None
        ):
            # We consider a metric configured if it is either turned on or off
            metrics_configured.add(datapoint["metric_definition_key"])
            if datapoint["enabled"] is True:
                metrics_available.add(datapoint["metric_definition_key"])
            elif datapoint["enabled"] is False:
                metrics_unavailable.add(datapoint["metric_definition_key"])

    return {
        NUM_RECORDS_WITH_DATA: len(report_id_to_datapoints),
        NUM_METRICS_WITH_DATA: len(metric_key_to_datapoints),
        NUM_METRICS_CONFIGURED: len(metrics_configured),
        NUM_METRICS_AVAILABLE: len(metrics_available),
        NUM_METRICS_UNAVAILABLE: len(metrics_unavailable),
    }


def generate_agency_summary_csv() -> None:
    """Generates a CSV with data about all agencies with Publisher accounts."""
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(schema_type=schema_type):
        with SessionFactory.for_proxy(database_key) as session:
            agencies = session.execute(
                "select * from source where type = 'agency'"
            ).all()
            datapoints = session.execute("select * from datapoint").all()

            # We have some test agencies in production, currently distinguished only in name
            test_agency_ids = {
                a["id"] for a in agencies if "TEST" in a["name"] or "Test" in a["name"]
            }
            non_test_agencies = [
                dict(a) for a in agencies if a["id"] not in test_agency_ids
            ]
            agency_id_to_agency = {a["id"]: a for a in non_test_agencies}

            print(f"Number of non-test agencies: {len(non_test_agencies)}")

            for agency in non_test_agencies:
                agency[NUM_RECORDS_WITH_DATA] = 0
                agency[NUM_METRICS_WITH_DATA] = 0
                agency[NUM_METRICS_CONFIGURED] = 0
                agency[NUM_METRICS_AVAILABLE] = 0
                agency[NUM_METRICS_UNAVAILABLE] = 0
                agency[IS_ALPHA_PARTNER] = agency["id"] in ALPHA_PARTNER_IDS

            agency_id_to_datapoints_groupby = groupby(
                sorted(datapoints, key=lambda x: x["source_id"]),
                key=lambda x: x["source_id"],
            )
            agency_id_to_datapoints = {
                k: list(v) for k, v in agency_id_to_datapoints_groupby
            }

            for agency_id, datapoints in agency_id_to_datapoints.items():
                if agency_id not in agency_id_to_agency:
                    continue

                data = summarize(datapoints=datapoints)
                agency_id_to_agency[agency_id] = dict(
                    agency_id_to_agency[agency_id], **data
                )

            agencies = list(agency_id_to_agency.values())

            df = (
                pd.DataFrame.from_records(agencies)
                # Sort agencies alphabetically by name
                .set_index("name")
                .sort_index()
                .rename(columns={"state_code": "state"})
                # Put columns in desired order
                .reindex(
                    columns=[
                        "state",
                        NUM_RECORDS_WITH_DATA,
                        NUM_METRICS_WITH_DATA,
                        NUM_METRICS_CONFIGURED,
                        NUM_METRICS_AVAILABLE,
                        NUM_METRICS_UNAVAILABLE,
                        IS_ALPHA_PARTNER,
                        "systems",
                    ]
                )
            )

            df.to_csv("justice_counts.csv")


if __name__ == "__main__":
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        generate_agency_summary_csv()
