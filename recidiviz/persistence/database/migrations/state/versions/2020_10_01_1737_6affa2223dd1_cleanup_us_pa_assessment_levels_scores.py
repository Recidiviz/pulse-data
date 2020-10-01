# pylint: skip-file
"""cleanup_us_pa_assessment_levels_scores

Revision ID: 6affa2223dd1
Revises: 7795ab9ba9e2
Create Date: 2020-10-01 17:37:28.502507

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6affa2223dd1'
down_revision = '7795ab9ba9e2'
branch_labels = None
depends_on = None

UPGRADE_55_TO_54_QUERY = "SELECT assessment_id FROM state_assessment " \
                         "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' " \
                         "AND assessment_score = 55"

# NOTE: This change cannot be downgraded.
UPGRADE_UPDATE_55_TO_54_QUERY = "UPDATE state_assessment " \
                             "SET assessment_score = 54 " \
                             "WHERE assessment_id IN ({ids_query});"

# Migration dd71a6394322 did not set SCORE_OUT_OF_RANGE assessment levels for out of range scores with
# unset assessment_dates
UPGRADE_SCORE_OUT_OF_RANGE_QUERY = "SELECT assessment_id FROM state_assessment " \
                                   "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' " \
                                   "AND assessment_date IS NULL AND assessment_score > 55 " \
                                   "AND assessment_score NOT IN (60, 70)"

UPGRADE_UPDATE_LEVEL_CLEAR_SCORE_QUERY = \
    "UPDATE state_assessment " \
    "SET assessment_level = '{assessment_level_value}', " \
    "assessment_level_raw_text = FORMAT('UNKNOWN (%%s-SCORE_OUT_OF_RANGE)', assessment_score), " \
    "assessment_score = NULL " \
    "WHERE assessment_id IN ({ids_query});"

DOWNGRADE_UNKNOWN_UNSET_SCORES_QUERY = "SELECT assessment_id FROM state_assessment " \
                                       "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' " \
                                       "AND assessment_level_raw_text LIKE '%%SCORE_OUT_OF_RANGE%%' " \
                                       "AND assessment_score IS NULL AND assessment_date IS NULL"

# This extracts the score from the assessment_level_raw_text field and re-sets it as the assessment_score
DOWNGRADE_UPDATE_LEVEL_EXTRACT_SCORE_QUERY = \
    "UPDATE state_assessment " \
    "SET assessment_level = NULL, assessment_level_raw_text = NULL, " \
    "assessment_score = CAST(SUBSTRING(assessment_level_raw_text, 'UNKNOWN \((\d+)') AS INTEGER) " \
    "WHERE assessment_id IN ({ids_query});"


def upgrade():
    connection = op.get_bind()

    change_55_to_54_query = UPGRADE_UPDATE_55_TO_54_QUERY.format(
        ids_query=UPGRADE_55_TO_54_QUERY
    )

    out_of_range_query = UPGRADE_UPDATE_LEVEL_CLEAR_SCORE_QUERY.format(
        assessment_level_value='EXTERNAL_UNKNOWN',
        ids_query=UPGRADE_SCORE_OUT_OF_RANGE_QUERY
    )

    connection.execute(change_55_to_54_query)
    connection.execute(out_of_range_query)


def downgrade():
    connection = op.get_bind()

    downgrade_extract_invalid_score_query = DOWNGRADE_UPDATE_LEVEL_EXTRACT_SCORE_QUERY.format(
        ids_query=DOWNGRADE_UNKNOWN_UNSET_SCORES_QUERY
    )

    connection.execute(downgrade_extract_invalid_score_query)
