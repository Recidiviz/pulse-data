# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Daily burndown of our Carahsoft GCP minimum commitment.

We buy GCP through Carahsoft rather than direct from Google, under agreement
56409240. That agreement commits us to spend a minimum amount over 36 months, and
bills us at a discount off list. Two separate 10% discounts apply, and only one of
them reaches this billing export -- see PROGRAM_DISCOUNT_FACTOR below.

This view answers one question: how much of the minimum commitment have we paid,
and are we ahead of the pace needed to clear it.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import Date, Float
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.source_tables.externally_managed.datasets import ALL_BILLING_DATA_DATASET
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

GCP_COMMITMENT_BURNDOWN = "gcp_commitment_burndown"
GCP_COMMITMENT_BURNDOWN_DESCRIPTION = (
    "Daily burndown of the Carahsoft GCP minimum commitment (agreement 56409240)"
)

EXPORT_TABLE_ID = "gcp_billing_export_v1_01338E_BE3FD6_363B4C"

# Agreement 56409240 section 3: the Minimum Commitment for Commitment Period 1.
MINIMUM_COMMITMENT_USD = 2027643.30

# Section 2.3 sets a 36-month term from the Implementation Date, which Google picks
# within five business days of signature. Recidiviz signed on 2025-07-10, so these
# dates ASSUME 2025-07-15. Confirm against the first Carahsoft invoice under the new
# agreement, then correct both.
TERM_START_DATE = "2025-07-15"
TERM_END_DATE = "2028-07-14"

# The commitment counts what we PAY Carahsoft, net of credits and discounts. Two 10%
# discounts stack, and they multiply rather than add, for 19% cumulative:
#   * The Enterprise Discount (section 5.1) is already applied in `cost` below.
#   * The Program Discount (section 2.8) is granted by Google to us as the reseller-
#     side "Partner" and applied by Carahsoft on the monthly invoice. It never
#     appears in this export, so we apply it here.
# Section 7.3 withholds the Program Discount from third-party services and software,
# so it applies to the Google leg only. Third-party lines (Anthropic Claude via
# Vertex, for example) receive neither discount and are counted at full price.
PROGRAM_DISCOUNT_FACTOR = 0.9

VIEW_QUERY = f"""
WITH daily AS (
  SELECT
    DATE(usage_start_time) AS usage_date,
    SUM(IF(IFNULL(transaction_type, 'GOOGLE') = 'GOOGLE', cost, 0))
        * {PROGRAM_DISCOUNT_FACTOR}
      + SUM(IF(IFNULL(transaction_type, 'GOOGLE') = 'GOOGLE', 0, cost))
      + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) AS paid,
    SUM(IFNULL(cost_at_list, cost)) AS list_price,
    SUM(IF(IFNULL(transaction_type, 'GOOGLE') = 'GOOGLE', 0, cost)) AS third_party_paid
  FROM `{{project_id}}.{{all_billing_data}}.{{export_table_id}}`
  WHERE
    -- Ingestion time always follows usage time, so this floor cannot drop any usage
    -- inside the term. It exists to bound the partitions we scan.
    _PARTITIONTIME >= TIMESTAMP('2025-07-01')
    AND DATE(usage_start_time)
        BETWEEN DATE '{TERM_START_DATE}' AND DATE '{TERM_END_DATE}'
  GROUP BY usage_date
)
SELECT
  usage_date,
  paid,
  list_price,
  third_party_paid,
  SUM(paid) OVER w AS paid_cumulative,
  {MINIMUM_COMMITMENT_USD} AS minimum_commitment,
  SUM(paid) OVER w / {MINIMUM_COMMITMENT_USD} AS pct_of_commitment,
  DATE_DIFF(usage_date, DATE '{TERM_START_DATE}', DAY)
    / DATE_DIFF(DATE '{TERM_END_DATE}', DATE '{TERM_START_DATE}', DAY) AS pct_of_term,
  {MINIMUM_COMMITMENT_USD} * DATE_DIFF(usage_date, DATE '{TERM_START_DATE}', DAY)
    / DATE_DIFF(DATE '{TERM_END_DATE}', DATE '{TERM_START_DATE}', DAY) AS pace_target,
  SUM(paid) OVER w
    - {MINIMUM_COMMITMENT_USD} * DATE_DIFF(usage_date, DATE '{TERM_START_DATE}', DAY)
      / DATE_DIFF(DATE '{TERM_END_DATE}', DATE '{TERM_START_DATE}', DAY)
    AS ahead_of_pace,
  DATE '{TERM_START_DATE}' AS term_start_date,
  DATE '{TERM_END_DATE}' AS term_end_date
FROM daily
WINDOW w AS (ORDER BY usage_date)
ORDER BY usage_date
"""

GCP_COMMITMENT_BURNDOWN_SCHEMA = [
    Date(
        name="usage_date",
        description="Day the usage was incurred.",
        mode="NULLABLE",
    ),
    Float(
        name="paid",
        description="Amount paid to Carahsoft for this day, net of credits and both "
        "discounts.",
        mode="NULLABLE",
    ),
    Float(
        name="list_price",
        description="Google list price for this day, before either discount.",
        mode="NULLABLE",
    ),
    Float(
        name="third_party_paid",
        description="Third-party spend for this day, such as Anthropic Claude via "
        "Vertex. Receives neither discount.",
        mode="NULLABLE",
    ),
    Float(
        name="paid_cumulative",
        description="Running total paid since the start of the commitment term.",
        mode="NULLABLE",
    ),
    Float(
        name="minimum_commitment",
        description="The contractual Minimum Commitment. Constant on every row.",
        mode="NULLABLE",
    ),
    Float(
        name="pct_of_commitment",
        description="Running total as a share of the Minimum Commitment.",
        mode="NULLABLE",
    ),
    Float(
        name="pct_of_term",
        description="Share of the 36-month term elapsed by this day.",
        mode="NULLABLE",
    ),
    Float(
        name="pace_target",
        description="Straight-line spend the commitment needs by this day.",
        mode="NULLABLE",
    ),
    Float(
        name="ahead_of_pace",
        description="Running total minus the straight-line pace target. Negative "
        "means behind pace.",
        mode="NULLABLE",
    ),
    Date(
        name="term_start_date",
        description="Assumed Implementation Date, the first day of the term.",
        mode="NULLABLE",
    ),
    Date(
        name="term_end_date",
        description="Assumed final day of the 36-month term.",
        mode="NULLABLE",
    ),
]

GCP_COMMITMENT_BURNDOWN_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_query_template=VIEW_QUERY,
    dataset_id=PLATFORM_KPIS_DATASET,
    view_id=GCP_COMMITMENT_BURNDOWN,
    description=GCP_COMMITMENT_BURNDOWN_DESCRIPTION,
    schema=GCP_COMMITMENT_BURNDOWN_SCHEMA,
    all_billing_data=ALL_BILLING_DATA_DATASET,
    export_table_id=EXPORT_TABLE_ID,
    # The billing export only exists in production, so the view can only build there.
    # This matches bq_monthly_costs_by_dataset.
    projects_to_deploy={GCP_PROJECT_PRODUCTION},
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        GCP_COMMITMENT_BURNDOWN_VIEW_BUILDER.build_and_print()
