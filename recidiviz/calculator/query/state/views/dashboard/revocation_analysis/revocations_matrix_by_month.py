# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Revocations Matrix by month."""
# pylint: disable=trailing-whitespace

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_BY_MONTH_VIEW_NAME = "revocations_matrix_by_month"

REVOCATIONS_MATRIX_BY_MONTH_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation and the most severe violation.
 """

REVOCATIONS_MATRIX_BY_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        state_code,
        EXTRACT(YEAR FROM revocation_admission_date) as year,
        EXTRACT(MONTH FROM revocation_admission_date) as month,
        admission_type,
        violation_type,
        reported_violations,
        supervision_type,
        supervision_level,
        charge_category,
        level_1_supervision_location,
        level_2_supervision_location,
        COUNT(DISTINCT person_id) AS total_revocations
    FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized`
    -- We want MoM revocation admissions for the last 36 months
    WHERE revocation_admission_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH), INTERVAL 35 MONTH)
        -- Filter out any rows that don't have a specified violation_type
        AND violation_type != 'NO_VIOLATION_TYPE'
    GROUP BY state_code, year, month, violation_type, reported_violations, supervision_type, supervision_level,
        charge_category, level_1_supervision_location, level_2_supervision_location, admission_type
    """

REVOCATIONS_MATRIX_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_BY_MONTH_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_BY_MONTH_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "year",
        "month",
        "level_1_supervision_location",
        "level_2_supervision_location",
        "admission_type",
        "supervision_type",
        "supervision_level",
        "violation_type",
        "reported_violations",
        "charge_category",
    ),
    description=REVOCATIONS_MATRIX_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_BY_MONTH_VIEW_BUILDER.build_and_print()
