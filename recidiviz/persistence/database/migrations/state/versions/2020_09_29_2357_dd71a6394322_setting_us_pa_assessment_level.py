# pylint: skip-file
"""setting_us_pa_assessment_level

Revision ID: dd71a6394322
Revises: a6181898067d
Create Date: 2020-09-29 23:57:52.844913

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "dd71a6394322"
down_revision = "a6181898067d"
branch_labels = None
depends_on = None


UPGRADE_LOW_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND ((assessment_date <= '2008-12-31' AND assessment_score <= 20) "
    "OR (assessment_date <= '2014-12-03' AND assessment_score <= 17) "
    "OR (assessment_date > '2014-12-03' AND assessment_score <= 19)) "
)

UPGRADE_MEDIUM_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND ((assessment_date <= '2008-12-31' AND 20 < assessment_score AND assessment_score <= 28) "
    "OR (assessment_date <= '2014-12-03' AND 17 < assessment_score AND assessment_score <= 26) "
    "OR (assessment_date > '2014-12-03' AND 19 < assessment_score AND assessment_score <= 27)) "
)

UPGRADE_HIGH_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND ((assessment_date <= '2008-12-31' AND 28 < assessment_score AND assessment_score <= 55) "
    "OR (assessment_date <= '2014-12-03' AND 26 < assessment_score AND assessment_score <= 55) "
    "OR (assessment_date > '2014-12-03' AND 27 < assessment_score AND assessment_score <= 55)) "
)


UPGRADE_ATTEMPTED_INCOMPLETE_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND assessment_score = 60"
)

UPGRADE_REFUSED_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND assessment_score = 70"
)

UPGRADE_NO_DATE_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND assessment_score <= 54 AND assessment_date IS NULL"
)

UPGRADE_SCORE_OUT_OF_RANGE_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND assessment_date IS NOT NULL AND assessment_score > 55 "
    "AND assessment_score NOT IN (60, 70)"
)


UPGRADE_UPDATE_LEVEL_QUERY = (
    "UPDATE state_assessment "
    "SET assessment_level = '{assessment_level_value}', "
    "assessment_level_raw_text = '{assessment_level_raw_text_value}' "
    "WHERE assessment_id IN ({ids_query});"
)


UPGRADE_UPDATE_LEVEL_CLEAR_SCORE_QUERY = (
    "UPDATE state_assessment "
    "SET assessment_level = '{assessment_level_value}', "
    "assessment_level_raw_text = FORMAT('UNKNOWN (%%s-{unknown_level_identifier})', assessment_score), "
    "assessment_score = NULL "
    "WHERE assessment_id IN ({ids_query});"
)

# There are no LSIR assessments in US_PA with a set assessment_level right now, so our downgrade can set all
# US_PA LSIR assessment_level and assessment_level_raw_text values to NULL for assessments that did not have their
# scores erased
DOWNGRADE_VALID_SCORES_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND assessment_level IS NOT NULL AND assessment_score IS NOT NULL"
)

DOWNGRADE_UPDATE_LEVEL_VALID_SCORES_QUERY = (
    "UPDATE state_assessment "
    "SET assessment_level = NULL, assessment_level_raw_text = NULL "
    "WHERE assessment_id IN ({ids_query});"
)

DOWNGRADE_UNKNOWN_UNSET_SCORES_QUERY = (
    "SELECT assessment_id FROM state_assessment "
    "WHERE state_code = 'US_PA' AND assessment_type = 'LSIR' "
    "AND assessment_level = 'EXTERNAL_UNKNOWN' AND assessment_score IS NULL"
)

# This extracts the score from the assessment_level_raw_text field and re-sets it as the assessment_score
DOWNGRADE_UPDATE_LEVEL_EXTRACT_SCORE_QUERY = (
    "UPDATE state_assessment "
    "SET assessment_level = NULL, assessment_level_raw_text = NULL, "
    "assessment_score = CAST(SUBSTRING(assessment_level_raw_text, 'UNKNOWN \\((\\d+)') AS INTEGER) "
    "WHERE assessment_id IN ({ids_query});"
)


def upgrade() -> None:
    connection = op.get_bind()

    low_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_LEVEL_QUERY,
        assessment_level_value="LOW",
        assessment_level_raw_text_value="LOW",
        ids_query=UPGRADE_LOW_QUERY,
    )

    medium_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_LEVEL_QUERY,
        assessment_level_value="MEDIUM",
        assessment_level_raw_text_value="MEDIUM",
        ids_query=UPGRADE_MEDIUM_QUERY,
    )

    high_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_LEVEL_QUERY,
        assessment_level_value="HIGH",
        assessment_level_raw_text_value="HIGH",
        ids_query=UPGRADE_HIGH_QUERY,
    )

    no_date_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_LEVEL_QUERY,
        assessment_level_value="EXTERNAL_UNKNOWN",
        assessment_level_raw_text_value="UNKNOWN (NO_DATE)",
        ids_query=UPGRADE_NO_DATE_QUERY,
    )

    incomplete_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_LEVEL_CLEAR_SCORE_QUERY,
        assessment_level_value="EXTERNAL_UNKNOWN",
        unknown_level_identifier="ATTEMPTED_INCOMPLETE",
        ids_query=UPGRADE_ATTEMPTED_INCOMPLETE_QUERY,
    )

    refused_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_LEVEL_CLEAR_SCORE_QUERY,
        assessment_level_value="EXTERNAL_UNKNOWN",
        unknown_level_identifier="REFUSED",
        ids_query=UPGRADE_REFUSED_QUERY,
    )

    out_of_range_query = StrictStringFormatter().format(
        UPGRADE_UPDATE_LEVEL_CLEAR_SCORE_QUERY,
        assessment_level_value="EXTERNAL_UNKNOWN",
        unknown_level_identifier="SCORE_OUT_OF_RANGE",
        ids_query=UPGRADE_SCORE_OUT_OF_RANGE_QUERY,
    )

    connection.execute(low_query)
    connection.execute(medium_query)
    connection.execute(high_query)
    connection.execute(no_date_query)
    connection.execute(incomplete_query)
    connection.execute(refused_query)
    connection.execute(out_of_range_query)


def downgrade() -> None:
    connection = op.get_bind()

    downgrade_valid_scores_query = StrictStringFormatter().format(
        DOWNGRADE_UPDATE_LEVEL_VALID_SCORES_QUERY,
        ids_query=DOWNGRADE_VALID_SCORES_QUERY,
    )

    downgrade_extract_invalid_score_query = StrictStringFormatter().format(
        DOWNGRADE_UPDATE_LEVEL_EXTRACT_SCORE_QUERY,
        ids_query=DOWNGRADE_UNKNOWN_UNSET_SCORES_QUERY,
    )

    connection.execute(downgrade_valid_scores_query)
    connection.execute(downgrade_extract_invalid_score_query)
