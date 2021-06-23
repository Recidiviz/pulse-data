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
"""Encodes policy-mandated requirements for supervision level based on assessment score and other criteria"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_VIEW_NAME = (
    "supervision_level_compliance_requirements"
)

SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_VIEW_DESCRIPTION = "Logic determining supervision levels and associated risk score thresholds, to check for level mismatches"

SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_QUERY_TEMPLATE = """
    /*{description}*/
    # TODO(#7988): Remove this view once historical compliance standards are supported through case triage and/or dataflow.
    SELECT
        'US_ID' as state_code,
        DATE_SUB(CURRENT_DATE(), INTERVAL 20 YEAR) start_date,
        '2020-07-22' end_date,
        supervision_level,
        gender,
        case_type,
        CASE supervision_level
            WHEN 'MINIMUM' THEN 0 
            WHEN 'MEDIUM' THEN 16
            WHEN 'HIGH' THEN 24
            WHEN 'MAXIMUM' THEN 31
            END assessment_score_threshold
    FROM 
        UNNEST(['MINIMUM', 'MEDIUM', 'HIGH', 'MAXIMUM']) as supervision_level,
        UNNEST(['MALE','FEMALE','TRANS_FEMALE','TRANS_MALE']) as gender,
        UNNEST(['GENERAL']) as case_type
    UNION ALL 
    SELECT
        'US_ID' as state_code,
        '2020-07-23' start_date,
        CURRENT_DATE() end_date,
        supervision_level,
        gender,
        case_type,
        CASE 
            WHEN gender in ('MALE', 'TRANS_MALE')
            THEN CASE supervision_level
                WHEN 'MINIMUM' THEN 0 
                WHEN 'MEDIUM' THEN 21
                WHEN 'HIGH' THEN 29
                END
            WHEN gender in ('FEMALE', 'TRANS_FEMALE')
            THEN CASE supervision_level
                WHEN 'MINIMUM' THEN 0 
                WHEN 'MEDIUM' THEN 23
                WHEN 'HIGH' THEN 31
                END
            END as assessment_score_threshold
    FROM 
        UNNEST(['MINIMUM', 'MEDIUM', 'HIGH']) as supervision_level,
        UNNEST(['MALE','FEMALE','TRANS_FEMALE','TRANS_MALE']) as gender,
        UNNEST(['GENERAL']) as case_type
    UNION ALL 
    SELECT
        'US_PA' as state_code,
        '2011-01-04' start_date,
        CURRENT_DATE() end_date,
        supervision_level,
        gender,
        case_type,
        CASE supervision_level
            WHEN 'MINIMUM' THEN 0 
            WHEN 'MEDIUM' THEN 20
            WHEN 'MAXIMUM' THEN 28
        END as assessment_score_threshold
    FROM 
        UNNEST(['MINIMUM', 'MEDIUM', 'MAXIMUM']) as supervision_level,
        UNNEST(['MALE','FEMALE','TRANS_FEMALE','TRANS_MALE']) as gender,
        UNNEST(['GENERAL']) as case_type
    """

SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_VIEW_NAME,
    view_query_template=SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_QUERY_TEMPLATE,
    description=SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_VIEW_BUILDER.build_and_print()
