# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Logic for calculating big query storage costs by dataset"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.source_tables.externally_managed.datasets import ALL_BILLING_DATA_DATASET
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

BQ_MONTHLY_COSTS_BY_DATASET = "bq_monthly_costs_by_dataset"
BQ_MONTHLY_COSTS_BY_DATASET_DESCRIPTION = "Costs for big query resources "

DETAILED_EXPORT_TABLE_ID = "gcp_billing_export_resource_v1_01338E_BE3FD6_363B4C"
# the service.id associated with BigQuery
BQ_SERVICE_ID = "24E6-581D-38E5"

VIEW_QUERY = f"""
SELECT 
  project.id as gcp_project,
  PARSE_DATE("%Y%m", invoice.month) as invoice_month,
  -- storage SKUs are billed on the dataset-level, so resource.name is the BQ dataset
  resource.name as dataset_id,
  -- credits are negative values so we add!
  SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c),0)) AS total_cost_in_dollars
FROM `{{project_id}}.{{all_billing_data}}.{{detailed_export_table_id}}` 
WHERE 
    _PARTITIONTIME > CAST(DATETIME_SUB(CURRENT_DATETIME('US/Eastern'), INTERVAL 1 YEAR) AS TIMESTAMP)
    AND service.id = '{BQ_SERVICE_ID}'
    AND REGEXP_CONTAINS(sku.description, "Storage")
GROUP BY gcp_project, invoice_month, dataset_id
ORDER BY total_cost_in_dollars DESC
"""

BQ_MONTHLY_COSTS_BY_DATASET_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=BQ_MONTHLY_COSTS_BY_DATASET,
    description=BQ_MONTHLY_COSTS_BY_DATASET_DESCRIPTION,
    all_billing_data=ALL_BILLING_DATA_DATASET,
    detailed_export_table_id=DETAILED_EXPORT_TABLE_ID,
    projects_to_deploy={GCP_PROJECT_PRODUCTION},
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        BQ_MONTHLY_COSTS_BY_DATASET_VIEW_BUILDER.build_and_print()
