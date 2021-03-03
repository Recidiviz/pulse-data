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

"""A view which provides a comparison of total FTR referral counts per period for all views that support the
Free Through Recovery page."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.validation.views import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FTR_REFERRALS_COMPARISON_VIEW_NAME = "ftr_referrals_comparison"

FTR_REFERRALS_COMPARISON_DESCRIPTION = (
    """FTR program referral count comparison per metric period"""
)

FTR_REFERRALS_COMPARISON_QUERY_TEMPLATE = """
    /*{description}*/
    WITH by_age as (
      SELECT state_code as region_code, metric_period_months, SUM(count) as referral_count
      FROM `{project_id}.{view_dataset}.ftr_referrals_by_age_by_period`
      GROUP BY state_code, metric_period_months
    ),
    by_risk_level as (
      SELECT state_code as region_code, metric_period_months, SUM(count) as referral_count
      FROM `{project_id}.{view_dataset}.ftr_referrals_by_lsir_by_period`
      GROUP BY state_code, metric_period_months
    ),
    by_gender as (
      SELECT state_code as region_code, metric_period_months, SUM(count) as referral_count
      FROM `{project_id}.{view_dataset}.ftr_referrals_by_gender_by_period`
      GROUP BY state_code, metric_period_months
    ),
    by_race as (
      SELECT state_code as region_code, metric_period_months, SUM(count) as referral_count
      FROM `{project_id}.{view_dataset}.ftr_referrals_by_race_and_ethnicity_by_period`
      GROUP BY state_code, metric_period_months
    )
    SELECT region_code, metric_period_months,
           ba.referral_count as age_bucket_sum,
           brl.referral_count as risk_level_sum,
           bg.referral_count as gender_sum,
           br.referral_count as race_sum
    FROM by_age ba
    JOIN by_risk_level brl USING(region_code, metric_period_months)
    JOIN by_gender bg USING(region_code, metric_period_months)
    JOIN by_race br USING(region_code, metric_period_months)
"""

FTR_REFERRALS_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=FTR_REFERRALS_COMPARISON_VIEW_NAME,
    view_query_template=FTR_REFERRALS_COMPARISON_QUERY_TEMPLATE,
    description=FTR_REFERRALS_COMPARISON_DESCRIPTION,
    view_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        FTR_REFERRALS_COMPARISON_VIEW_BUILDER.build_and_print()
