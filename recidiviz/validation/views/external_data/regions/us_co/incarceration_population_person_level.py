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
"""A view containing the incarceration population person-level for CODOC"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
  SELECT 
    'US_CO' as region_code,
    offenderid AS person_external_id, 
    EXTRACT(DATE FROM UPDATE_DATETIME) AS date_of_stay, 
    CASE 
        WHEN FAC_CD ='AC' THEN 'ACC'
        WHEN FAC_CD ='AV' THEN 'AVCF'
        WHEN FAC_CD = 'AT' THEN 'ALLCTYJAIL'
        WHEN FAC_CD ='LF' THEN 'LCF'
        WHEN FAC_CD ='LV' THEN 'LVCF'
        WHEN FAC_CD ='CM' THEN 'COMMUNITY CORRECTIONS'
        WHEN FAC_CD = 'CR' THEN 'JAIL BCKLG'
        WHEN FAC_CD ='BF' THEN 'BCCF'
        WHEN FAC_CD ='BM' THEN 'BVCF' # Buena Vista Minimum Center to General BV in new system
        WHEN FAC_CD ='DU' THEN 'DRDC'
        WHEN FAC_CD ='DW' THEN 'DWCF'
        WHEN FAC_CD ='BV' THEN 'BVCF'
        WHEN FAC_CD ='BW' THEN 'BVCF' # Buena Vista Work Center to General BV in new system
        WHEN FAC_CD ='C3' THEN 'CM YOS PH3'
        WHEN FAC_CD ='CF' THEN 'CCF'
        WHEN FAC_CD ='CL' THEN 'CCCF'
        WHEN FAC_CD ='DC' THEN 'DCC'
        WHEN FAC_CD ='FF' THEN 'FCF'
        WHEN FAC_CD ='FI' THEN 'FUG-INMATE'
        WHEN FAC_CD ='IS' THEN 'ISP-INMATE'
        WHEN FAC_CD ='JB' THEN 'JAIL BCKLG'
        WHEN FAC_CD ='RC' THEN 'RCC'
        WHEN FAC_CD ='SA' THEN 'SCCF'
        WHEN FAC_CD ='SF' THEN 'SCF'
        WHEN FAC_CD ='YP' THEN 'YOS'
        WHEN FAC_CD ='CS' THEN 'CSP'
        WHEN FAC_CD ='CT' THEN 'CTCF'
        WHEN FAC_CD ='FC' THEN 'FMCC'
        WHEN FAC_CD ='TF' THEN 'TCF'
    ELSE FAC_CD 
    END AS facility, 
  FROM `{project_id}.{us_co_raw_data_dataset}.Base_Curr_Off_Pop`
"""

US_CO_INCARCERATION_POPULATION_PERSON_LEVEL_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_CO),
    view_id="incarceration_population_person_level",
    description="A view detailing the incarceration population at the person level for CODOC.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_co_raw_data_dataset=raw_tables_dataset_for_region(StateCode.US_CO.value),
    # us_co_raw_data_dataset="us_co_raw_data",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CO_INCARCERATION_POPULATION_PERSON_LEVEL_BUILDER.build_and_print()
