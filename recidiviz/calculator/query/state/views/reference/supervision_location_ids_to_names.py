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
"""Reference table for supervision location names."""

# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME = 'supervision_location_ids_to_names'

SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION = """Reference table for supervision location names"""

SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH
    pa_location_names AS (
        SELECT 
            'US_PA' AS state_code,
            Region_Code AS level_3_supervision_location_external_id,
            Region AS level_3_supervision_location_name,
            RelDO AS level_2_supervision_location_external_id,
            DistrictOfficeName AS level_2_supervision_location_name,
            Org_cd AS level_1_supervision_location_external_id,
            Org_Name AS level_1_supervision_location_name
        FROM 
            `{project_id}.us_pa_raw_data_up_to_date_views.dbo_LU_PBPP_Organization_latest`
        JOIN
            `{project_id}.us_pa_raw_data_up_to_date_views.dbo_LU_RelDo_latest`
        ON DistrictOfficeCode = RelDO
    ),
    mo_location_names AS (
        WITH
        supervision_district_to_name AS (
            SELECT level_1_supervision_location_external_id, level_1_supervision_location_name
            FROM UNNEST([
                STRUCT('01' AS level_1_supervision_location_external_id,
                       'St. Joseph Community Supervision Center' AS level_1_supervision_location_name),
                STRUCT('02', 'Cameron'),
                STRUCT('03', 'Hannibal Community Supervision Center'),
                STRUCT('04', 'Kansas City'),
                STRUCT('04B', 'Kansas City'),
                STRUCT('04C', 'Kansas City'),
                STRUCT('04R', 'Kansas City'),
                STRUCT('04W', 'Kansas City'),
                STRUCT('05', 'Nevada'),
                STRUCT('05B', 'Belton'),
                STRUCT('05W', 'Warrensburg'),
                STRUCT('06', 'Columbia'),
                STRUCT('07', 'St. Louis'),
                STRUCT('07B', 'St. Louis'),
                STRUCT('07C', 'St. Louis'),
                STRUCT('07S', 'St. Louis'),
                STRUCT('08C', 'St. Louis'),
                STRUCT('08E', 'St. Louis'),
                STRUCT('08N', 'St. Louis'),
                STRUCT('08S', 'St. Louis'),
                STRUCT('09', 'St. Louis'),
                STRUCT('10', 'Springfield'),
                STRUCT('10N', 'Springfield'),
                STRUCT('10R', 'Springfield'),
                STRUCT('11', 'Rolla'),
                STRUCT('11S', 'Steelville'),
                STRUCT('12', 'Farmington Community Supervision Center'),
                STRUCT('13', 'West Plains'),
                STRUCT('14', 'Sikeston'),
                STRUCT('14A', 'Charleston'),
                STRUCT('14B', 'New Madrid'),
                STRUCT('15', 'Hillsboro'),
                STRUCT('16', 'Union'),
                STRUCT('17', 'St. Charles'),
                STRUCT('18', 'Macon'),
                STRUCT('18SK', 'Kirksville'),
                STRUCT('18SM', 'Moberly'),
                STRUCT('19', 'Liberty'),
                STRUCT('20', 'Camdenton'),
                STRUCT('21', 'Branson'),
                STRUCT('22', 'Cape Girardeau'),
                STRUCT('23', 'Kennett Community Supervision Center'),
                STRUCT('24', 'Independence'),
                STRUCT('25', 'Poplar Bluff Community Supervision Center'),
                STRUCT('26', 'Fulton Community Supervision Center'),
                STRUCT('27', 'Jefferson City'),
                STRUCT('28', 'Belton'),
                STRUCT('29', 'Sedalia'),
                STRUCT('30', 'Nevada'),
                STRUCT('31', 'Caruthersville'),
                STRUCT('32', 'Lexington'),
                STRUCT('32S', 'Marshall'),
                STRUCT('33', 'Neosho'),
                STRUCT('34', 'Lake Ozark'),
                STRUCT('35', 'Hartville'),
                STRUCT('36', 'Potosi'),
                STRUCT('37', 'Dexter'),
                STRUCT('38', 'Troy'),
                STRUCT('39', 'Trenton'),
                STRUCT('41', 'Mississippi County'),
                STRUCT('42', 'Nixa'),
                STRUCT('43', 'Aurora'),
                STRUCT('44', 'Cassville'),
                STRUCT('EC', 'St. Louis'),
                STRUCT('EP', 'St. Louis'),
                STRUCT('ERA', 'St. Louis'),
                STRUCT('ERV', 'St. Louis'),
                STRUCT('KCCRC', 'Kansas City Community Release Center'),
                STRUCT('PPCMDCTR', 'Parole and Probation Command Center'),
                STRUCT('SLCRC', 'St. Louis Community Release Center'),
                STRUCT('TCSTL', 'Transition Center of St. Louis')
            ])
        ),
        supervision_district_to_region AS (
            SELECT level_1_supervision_location_external_id, level_2_supervision_location_external_id
             FROM UNNEST([
                STRUCT('01' AS level_1_supervision_location_external_id,
                       'WESTERN' AS level_2_supervision_location_external_id),
                STRUCT('02', 'NORTH CENTRAL'),
                STRUCT('03', 'NORTHEAST'),
                STRUCT('04', 'WESTERN'),
                STRUCT('04A', 'WESTERN'),
                STRUCT('04B', 'WESTERN'),
                STRUCT('04C', 'WESTERN'),
                STRUCT('04R', 'WESTERN'),
                STRUCT('04W', 'WESTERN'),
                STRUCT('05', 'NORTH CENTRAL'),
                STRUCT('06', 'NORTH CENTRAL'),
                STRUCT('07B', 'EASTERN'),
                STRUCT('07C', 'EASTERN'),
                STRUCT('07S', 'EASTERN'),
                STRUCT('08C', 'EASTERN'),
                STRUCT('08E', 'EASTERN'),
                STRUCT('08N', 'EASTERN'),
                STRUCT('08S', 'EASTERN'),
                STRUCT('09', 'SOUTHWEST'),
                STRUCT('10', 'SOUTHWEST'),
                STRUCT('10N', 'SOUTHWEST'),
                STRUCT('10R', 'SOUTHWEST'),
                STRUCT('11', 'NORTHEAST'),
                STRUCT('12', 'SOUTHEAST'),
                STRUCT('13', 'SOUTHWEST'),
                STRUCT('14', 'SOUTHEAST'),
                STRUCT('15', 'SOUTHEAST'),
                STRUCT('16', 'NORTHEAST'),
                STRUCT('17', 'NORTHEAST'),
                STRUCT('18', 'NORTHEAST'),
                STRUCT('19', 'WESTERN'),
                STRUCT('20', 'NORTH CENTRAL'),
                STRUCT('21', 'SOUTHWEST'),
                STRUCT('22', 'SOUTHEAST'),
                STRUCT('23', 'SOUTHEAST'),
                STRUCT('24', 'WESTERN'),
                STRUCT('25', 'SOUTHEAST'),
                STRUCT('26', 'NORTHEAST'),
                STRUCT('27', 'NORTH CENTRAL'),
                STRUCT('28', 'WESTERN'),
                STRUCT('29', 'NORTH CENTRAL'),
                STRUCT('30', 'SOUTHWEST'),
                STRUCT('31', 'SOUTHEAST'),
                STRUCT('32', 'NORTH CENTRAL'),
                STRUCT('33', 'SOUTHWEST'),
                STRUCT('34', 'NORTH CENTRAL'),
                STRUCT('35', 'NORTH CENTRAL'),
                STRUCT('36', 'SOUTHEAST'),
                STRUCT('37', 'SOUTHEAST'),
                STRUCT('38', 'NORTHEAST'),
                STRUCT('39', 'NORTH CENTRAL'),
                STRUCT('40', 'UNCLASSIFIED_REGION'),
                STRUCT('41', 'SOUTHEAST'),
                STRUCT('42', 'SOUTHWEST'),
                STRUCT('43', 'SOUTHWEST'),
                STRUCT('44', 'SOUTHWEST'),
                STRUCT('COPP', 'UNCLASSIFIED_REGION'),
                STRUCT('EC', 'EASTERN'),
                STRUCT('EP', 'EASTERN'),
                STRUCT('ER', 'UNCLASSIFIED_REGION'),
                STRUCT('ERA', 'EASTERN'),
                STRUCT('ERV', 'UNCLASSIFIED_REGION'),
                STRUCT('KCCRC', 'UNCLASSIFIED_REGION'),
                STRUCT('PACC', 'UNCLASSIFIED_REGION'),
                STRUCT('PBCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PCCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PCMCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PCRCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PERDCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PFCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PFRDC', 'UNCLASSIFIED_REGION'),
                STRUCT('PJCCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PKCCRC', 'UNCLASSIFIED_REGION'),
                STRUCT('PKCRC', 'UNCLASSIFIED_REGION'),
                STRUCT('PMCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PMECC', 'UNCLASSIFIED_REGION'),
                STRUCT('PMSP', 'UNCLASSIFIED_REGION'),
                STRUCT('PMTC', 'UNCLASSIFIED_REGION'),
                STRUCT('PNECC', 'UNCLASSIFIED_REGION'),
                STRUCT('POCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PPBOARD', 'UNCLASSIFIED_REGION'),
                STRUCT('PPCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PPCCU', 'UNCLASSIFIED_REGION'),
                STRUCT('PPCMDCTR', 'CENTRAL OFFICE'),
                STRUCT('PPFUGUNT', 'UNCLASSIFIED_REGION'),
                STRUCT('PPICU', 'UNCLASSIFIED_REGION'),
                STRUCT('PSCCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PSECC', 'UNCLASSIFIED_REGION'),
                STRUCT('PSLCRC', 'UNCLASSIFIED_REGION'),
                STRUCT('PTCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PWERDCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PWMCC', 'UNCLASSIFIED_REGION'),
                STRUCT('PWRDCC', 'UNCLASSIFIED_REGION'),
                STRUCT('SLCRC', 'TCSTL'),
                STRUCT('TCSTL', 'TCSTL'),
                STRUCT('WMCC', 'UNCLASSIFIED_REGION')
            ])
        )
        SELECT
            DISTINCT
                'US_MO' AS state_code,
                'NOT_APPLICABLE' AS level_3_supervision_location_external_id,
                'NOT_APPLICABLE' AS level_3_supervision_location_name,
                level_2_supervision_location_external_id,
                level_2_supervision_location_external_id  AS level_2_supervision_location_name,
                level_1_supervision_location_external_id,
                level_1_supervision_location_name,
        FROM (
            SELECT DISTINCT CE_PLN AS level_1_supervision_location_external_id
            FROM `{project_id}.us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK034_latest`
        )
        LEFT OUTER JOIN
            -- TODO(#4054): Once MO has shipped SQL-preprocessing to prod, update this to query from 
            --   us_mo_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_to_name instead of building
            --   the table as a subquery.
            supervision_district_to_name
        USING (level_1_supervision_location_external_id)
        LEFT OUTER JOIN
            -- TODO(#4054): Once MO has shipped SQL-preprocessing to prod, update this to query from 
            --   us_mo_raw_data_up_to_date_views.RECIDIVIZ_REFERENCE_supervision_district_to_region instead of building
            --   the table as a subquery.
            supervision_district_to_region
        USING (level_1_supervision_location_external_id)
    )
    SELECT * FROM mo_location_names
    UNION ALL
    SELECT * FROM pa_location_names;
    """

SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_NAME,
    view_query_template=SUPERVISION_LOCATION_IDS_TO_NAMES_QUERY_TEMPLATE,
    description=SUPERVISION_LOCATION_IDS_TO_NAMES_DESCRIPTION,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER.build_and_print()
