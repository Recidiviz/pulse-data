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
"""Reference view that creates a mapping from numerous offense_type categories to a smaller set of offense categories"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OFFENSE_TYPE_MAPPING_VIEW_NAME = "offense_type_mapping"

OFFENSE_TYPE_MAPPING_VIEW_DESCRIPTION = "Reference view that creates a mapping from numerous offense_type categories to a smaller set of offense categories"

OFFENSE_TYPE_MAPPING_QUERY_TEMPLATE = """
    /*{description}*/
    # TODO(#10651): Create / hydrate is_violent flag for these states
    WITH us_id_offenses AS (
        SELECT DISTINCT state_code, 
                offense_type,
                description,
                CASE
                    WHEN description LIKE '%ROBBERY%' THEN 'PROPERTY'
                    WHEN offense_type LIKE '%MURDER%' THEN 'MURDER_HOMICIDE'
                    WHEN offense_type LIKE '%MISC%' THEN 'OTHER'
                    WHEN offense_type = 'SEX' THEN 'SEX'
                    WHEN offense_type = 'DRUG' THEN 'ALCOHOL_DRUG'
                    -- This is placed here because there are some instances of "obtaining controlled substance through fraud" that should get labeled
                    -- "DRUG" with the previous line
                    WHEN description LIKE '%FORGERY%' OR description LIKE '%FRAUD%' THEN 'FRAUD'
                    WHEN offense_type = 'ALCOHOL' THEN 'ALCOHOL_DRUG'
                    WHEN offense_type = 'ASSAULT' THEN 'ASSAULT'
                    WHEN offense_type = 'PROPERTY' THEN 'PROPERTY'
                    WHEN offense_type IS NOT NULL THEN 'UNCATEGORIZED'
                END AS offense_type_short
        FROM `{project_id}.{base_dataset}.state_charge`
        WHERE state_code = 'US_ID'
    ),
    us_pa_offenses AS (
        SELECT DISTINCT state_code, 
                offense_type,
                CASE
                    WHEN offense_type LIKE '%AGGRAVATED ASSAULT-ASSAULT%' THEN 'ASSAULT'
                    WHEN offense_type LIKE '%AGGRAVATED ASSAULT-SEX OFFENSE%' THEN 'SEX'
                    WHEN offense_type LIKE '%VIOLENT-PROPERTY%' THEN 'PROPERTY'
                    WHEN offense_type LIKE '%ASSAULT%' THEN 'ASSAULT'
                    WHEN offense_type LIKE '%DRUG%' THEN 'ALCOHOL_DRUG'
                    WHEN offense_type LIKE '%ALCOHOL%' THEN 'ALCOHOL_DRUG'
                    WHEN offense_type LIKE '%DUMMY%' THEN 'UNKNOWN'
                    WHEN offense_type IN ('FORGERY','FRAUD') THEN 'FRAUD'
                    WHEN offense_type LIKE '%HOMICIDE%' THEN 'MURDER_HOMICIDE'
                    WHEN offense_type LIKE '%MURDER%' THEN 'MURDER_HOMICIDE'
                    WHEN offense_type LIKE '%MANSLAUGHTER%' THEN 'MURDER_HOMICIDE'
                    WHEN offense_type LIKE '%OTHER ASSAULT-ASSAULT%' THEN 'ASSAULT'
                    WHEN offense_type LIKE '%OTHER ASSAULT-SEX OFFENSE%' THEN 'SEX'
                    WHEN offense_type LIKE '%OTHER SEXUAL CRIMES%' THEN 'SEX'
                    WHEN offense_type LIKE '%OTHER SEXUAL CRIMES-SEX OFFENSE%' THEN 'SEX'
                    WHEN offense_type LIKE '%PART II OTHER%' THEN 'OTHER'
                    WHEN offense_type LIKE '%PART II OTHER-SEX OFFENSE%' THEN 'SEX'
                    WHEN offense_type LIKE '%PART II OTHER-WEAPONS%' THEN 'ASSAULT'
                    WHEN offense_type LIKE '%PRISON BREACH-ESCAPE%' THEN 'OTHER'
                    WHEN offense_type LIKE 'RAPE-SEX OFFENSE' THEN 'SEX'
                    WHEN offense_type LIKE 'RECEIVING STOLEN PROPERTY' THEN 'PROPERTY'
                    WHEN offense_type LIKE 'STATUTORY RAPE-SEX OFFENSE' THEN 'SEX'
                    WHEN offense_type LIKE 'THEFT' THEN 'PROPERTY'
                    WHEN offense_type LIKE 'WEAPONS-WEAPONS' THEN 'ASSAULT'
                    WHEN offense_type IS NOT NULL THEN 'UNCATEGORIZED'
                END AS offense_type_short
        FROM `{project_id}.{base_dataset}.state_charge`
        WHERE state_code = 'US_PA'
        ORDER BY 3
    ),
    us_tn_offenses AS (
        SELECT   'US_TN' AS state_code,
                 OffenseDescription AS offense_type,
                  CASE 
                    WHEN OffenseDescription LIKE '%FORGERY%' THEN 'FRAUD'
                    WHEN OffenseDescription LIKE '%THEFT%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%AGGR%BURGLARY%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%SEXUAL ASSAULT%' THEN 'SEX'
                    WHEN OffenseDescription LIKE '%ASSAULT%' THEN 'ASSAULT'
                    WHEN OffenseDescription LIKE '%DRUG%' AND OffenseDescription NOT LIKE '%ADULTERATION%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%BURGLARY%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%ROBBERY%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%DUI%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%METH%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%IDENTITY THEFT%' THEN 'FRAUD'
                    WHEN OffenseDescription LIKE '%LARCENY%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%RAPE%' THEN 'SEX'
                    WHEN OffenseDescription LIKE '%MURDER%' THEN 'MURDER_HOMICIDE'
                    WHEN OffenseDescription LIKE '%HOMICIDE%' THEN 'MURDER_HOMICIDE'
                    WHEN OffenseDescription LIKE '%SEXUAL BATTERY%' THEN 'SEX'
                    WHEN OffenseDescription LIKE '%POSS%METH%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%SIMPLE POSS%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%POSS%WEAPON%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%POSS%FIREARM%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%STOLEN PROPERTY RECEIVED%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%COCAINE%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%VANDALISM%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%FALSE REPORTS%' THEN 'FRAUD'
                    WHEN OffenseDescription LIKE '%CONT. SUBSTANCE%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%MANSLAUGHTER%' THEN 'MURDER_HOMICIDE'
                    WHEN OffenseDescription LIKE '%AGGR%ARSON%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%ARSON%' THEN 'PROPERTY'
                    WHEN OffenseDescription LIKE '%FORGED%' THEN 'FRAUD'
                    WHEN OffenseDescription LIKE '%PROHIB DRIV W/U INFLUENCE OF INTOXICANT%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%WORTHLESS CHECKS%' THEN 'FRAUD'
                    WHEN OffenseDescription LIKE '%VEHICULAR HOM.%' THEN 'MURDER_HOMICIDE'
                    WHEN OffenseDescription LIKE '%HABITUAL TRAFFIC OFFENDER%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%FAILURE TO APPEAR (FELONY)%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%RECKLESS ENDANGERMENT -DEADLY WEAPON INVOLVED%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%EVADING ARREST%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%CONTRA. IN PENAL FACILITY%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%ESCAPE%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%TAMPERING WITH EVIDENCE%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%CRIMINAL SIMULATION%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%ACCESSORY AFTER THE FACT%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%CASUAL EXCHANGE (FINE NOT GREATER THAN $500)%' THEN 'OTHER'
                    WHEN OffenseDescription LIKE '%VIOL OF SEX OFFENDER REGIS/MONITORING ACT%' THEN 'SEX'
                    WHEN OffenseDescription LIKE '%SEX OFFENDER REGIST%' THEN 'SEX'
                    WHEN OffenseDescription LIKE '%FRAUD%' THEN 'FRAUD'
                    WHEN OffenseDescription LIKE '%MARIJUANA%' THEN 'ALCOHOL_DRUG'
                    WHEN OffenseDescription LIKE '%BAD CHECK%' THEN 'FRAUD'
                    WHEN OffenseDescription LIKE '%KIDNAP%' THEN 'ASSAULT'
                    WHEN OffenseDescription IS NOT NULL THEN 'UNCATEGORIZED'
                END AS offense_type_short
        # TODO(#10628): Switch to using state_charge once US_TN is ingested
        FROM `{project_id}.{raw_dataset}.OffenderStatute_latest`
    )
    SELECT DISTINCT * EXCEPT(description)
    FROM us_id_offenses 
    WHERE offense_type IS NOT NULL
    
    UNION ALL

    SELECT *
    FROM us_pa_offenses
    WHERE offense_type IS NOT NULL
    
    UNION ALL 

    SELECT *
    FROM us_tn_offenses 
    WHERE offense_type IS NOT NULL
    """

OFFENSE_TYPE_MAPPING_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=OFFENSE_TYPE_MAPPING_VIEW_NAME,
    view_query_template=OFFENSE_TYPE_MAPPING_QUERY_TEMPLATE,
    description=OFFENSE_TYPE_MAPPING_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    raw_dataset=US_TN_RAW_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFENSE_TYPE_MAPPING_VIEW_BUILDER.build_and_print()
