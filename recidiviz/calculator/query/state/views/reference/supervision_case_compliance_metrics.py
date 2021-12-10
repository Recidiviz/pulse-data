# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Case compliance metrics with supervising_officer_external_id pulled directly from raw data table for US_ID."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    hack_us_id_supervising_officer_external_id,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_CASE_COMPLIANCE_METRICS_VIEW_NAME = "supervision_case_compliance_metrics"

SUPERVISION_CASE_COMPLIANCE_METRICS_DESCRIPTION = """
Case compliance metrics with supervising_officer_external_id pulled directly from raw data table for US_ID.
"""

SUPERVISION_CASE_COMPLIANCE_METRICS_TEMPLATE = f"""
/*{{description}}*/
    {hack_us_id_supervising_officer_external_id('most_recent_supervision_case_compliance_metrics_materialized')}
"""

SUPERVISION_CASE_COMPLIANCE_METRICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_CASE_COMPLIANCE_METRICS_VIEW_NAME,
    view_query_template=SUPERVISION_CASE_COMPLIANCE_METRICS_TEMPLATE,
    description=SUPERVISION_CASE_COMPLIANCE_METRICS_DESCRIPTION,
    should_materialize=True,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_CASE_COMPLIANCE_METRICS_VIEW_BUILDER.build_and_print()
