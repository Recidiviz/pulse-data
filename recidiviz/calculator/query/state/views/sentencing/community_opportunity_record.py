#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View to prepare community opportunities for PSI tools for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.sentencing.us_ix.sentencing_community_opportunity_template import (
    US_IX_SENTENCING_COMMUNITY_OPPORTUNITY_TEMPLATE,
)
from recidiviz.calculator.query.state.views.sentencing.us_nd.sentencing_community_opportunity_template import (
    US_ND_SENTENCING_COMMUNITY_OPPORTUNITY_TEMPLATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCING_COMMUNITY_OPPORTUNITY_VIEW_NAME = "sentencing_community_opportunity_record"

SENTENCING_COMMUNITY_OPPORTUNITY_DESCRIPTION = """
    Community opportunity records to be exported to frontend to power PSI tools.
    """

SENTENCING_COMMUNITY_OPPORTUNITY_QUERY_TEMPLATE = f"""
WITH
  ix_opportunities AS ({US_IX_SENTENCING_COMMUNITY_OPPORTUNITY_TEMPLATE}),
  nd_opportunities AS ({US_ND_SENTENCING_COMMUNITY_OPPORTUNITY_TEMPLATE}),
  -- full_query serves as a template for when Sentencing expands to other states and we union other views
  full_query AS (
  SELECT
    *
  FROM
    ix_opportunities
  UNION ALL
  SELECT
    *
  FROM
    nd_opportunities)
SELECT
  {{columns}}
FROM
  full_query
"""

SENTENCING_COMMUNITY_OPPORTUNITY_RECORD_VIEW_BUILDER = (
    SelectedColumnsBigQueryViewBuilder(
        view_id=SENTENCING_COMMUNITY_OPPORTUNITY_VIEW_NAME,
        dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
        view_query_template=SENTENCING_COMMUNITY_OPPORTUNITY_QUERY_TEMPLATE,
        description=SENTENCING_COMMUNITY_OPPORTUNITY_DESCRIPTION,
        us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
        ),
        us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
        ),
        should_materialize=True,
        columns=[
            "state_code",
            "OpportunityName",
            "Description",
            "ProviderName",
            "CleanedProviderPhoneNumber",
            "ProviderWebsite",
            "ProviderAddress",
            "NeedsAddressed",
            "developmentalDisabilityDiagnosisCriterion",
            "noCurrentOrPriorSexOffenseCriterion",
            "noCurrentOrPriorViolentOffenseCriterion",
            "noPendingFelonyChargesInAnotherCountyOrStateCriterion",
            "entryOfGuiltyPleaCriterion",
            "veteranStatusCriterion",
            "priorCriminalHistoryCriterion",
            "diagnosedMentalHealthDiagnosisCriterion",
            "asamLevelOfCareRecommendationCriterion",
            "diagnosedSubstanceUseDisorderCriterion",
            "minLsirScoreCriterion",
            "maxLsirScoreCriterion",
            "minAge",
            "maxAge",
            "district",
            "additionalNotes",
            "lastUpdatedDate",
            "genders",
            "genericDescription",
            "counties",
            "status",
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCING_COMMUNITY_OPPORTUNITY_RECORD_VIEW_BUILDER.build_and_print()
