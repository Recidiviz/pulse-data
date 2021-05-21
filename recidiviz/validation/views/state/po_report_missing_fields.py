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

"""A view revealing when any of the required columns in the PO report table are missing."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PO_REPORT_MISSING_FIELDS_VIEW_NAME = "po_report_missing_fields"

PO_REPORT_MISSING_FIELDS_DESCRIPTION = """
  All PO report rows with missing data for required columns
  """

PO_REPORT_REQUIRED_FIELDS = [
    # report metadata
    "officer_given_name",
    "email_address",
    # pos discharges
    "pos_discharges",
    "pos_discharges_last_month",
    "pos_discharges_district_average",
    "pos_discharges_state_average",
    # supervision downgrades
    "supervision_downgrades",
    "supervision_downgrades_last_month",
    "supervision_downgrade_clients",
    "supervision_downgrade_clients_last_month",
    "supervision_downgrades_district_average",
    "supervision_downgrades_state_average",
    # earned discharges
    "earned_discharges",
    "earned_discharges_last_month",
    "earned_discharges_district_average",
    "earned_discharges_state_average",
    # technical revocations
    "technical_revocations",
    "technical_revocations_last_month",
    "technical_revocations_district_average",
    "technical_revocations_state_average",
    # absconsions
    "absconsions",
    "absconsions_last_month",
    "absconsions_district_average",
    "absconsions_state_average",
    # crime revocations
    "crime_revocations",
    "crime_revocations_last_month",
    "crime_revocations_district_average",
    "crime_revocations_state_average",
    # assessments
    "assessments",
    "assessment_percent",
    # face to face contact
    "facetoface",
    "facetoface_percent",
]
PO_REPORT_COMPARISON_COLUMNS = PO_REPORT_REQUIRED_FIELDS + ["total_rows"]

PO_REPORT_MISSING_FIELDS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      state_code as region_code, review_month,
      COUNT(*) AS total_rows,
      {non_null_column_count}
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    GROUP BY state_code, review_month
    """

PO_REPORT_MISSING_FIELDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PO_REPORT_MISSING_FIELDS_VIEW_NAME,
    view_query_template=PO_REPORT_MISSING_FIELDS_QUERY_TEMPLATE,
    description=PO_REPORT_MISSING_FIELDS_DESCRIPTION,
    po_report_dataset=state_dataset_config.PO_REPORT_DATASET,
    non_null_column_count=",\n  ".join(
        [
            f"SUM(IF({col} IS NOT NULL, 1, 0)) AS {col}"
            for col in PO_REPORT_REQUIRED_FIELDS
        ]
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_REPORT_MISSING_FIELDS_VIEW_BUILDER.build_and_print()
