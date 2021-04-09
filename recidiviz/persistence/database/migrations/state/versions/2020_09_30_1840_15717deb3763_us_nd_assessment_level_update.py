# pylint: skip-file
"""us_nd_assessment_level_update

Revision ID: 15717deb3763
Revises: dd71a6394322
Create Date: 2020-09-30 18:40:53.495693

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "15717deb3763"
down_revision = "dd71a6394322"
branch_labels = None
depends_on = None

UPGRADE_DEPRECATED_ASSESSMENT_LEVELS_QUERY = (
    "SELECT assessment_id FROM {table_name} "
    "WHERE state_code = 'US_ND' AND "
    "assessment_level IN ('UNDETERMINED', 'NOT_APPLICABLE')"
)

DOWNGRADE_DEPRECATED_LEVEL_QUERY = (
    "SELECT assessment_id FROM {table_name} "
    "WHERE state_code = 'US_ND' AND "
    "assessment_level_raw_text = '{assessment_level_raw_text_value}'"
)


UPDATE_ASSESSMENT_LEVEL_QUERY = (
    "UPDATE {table_name} "
    "SET assessment_level = '{updated_assessment_level}' "
    "WHERE assessment_id IN ({ids_query});"
)

TABLE_NAME = "state_assessment"
HISTORICAL_TABLE_NAME = "state_assessment_history"


def upgrade() -> None:
    connection = op.get_bind()

    updated_assessment_level = "EXTERNAL_UNKNOWN"

    update_assessment_level_query = UPDATE_ASSESSMENT_LEVEL_QUERY.format(
        table_name=TABLE_NAME,
        updated_assessment_level=updated_assessment_level,
        ids_query=UPGRADE_DEPRECATED_ASSESSMENT_LEVELS_QUERY.format(
            table_name=TABLE_NAME,
        ),
    )

    update_assessment_level_historical_query = UPDATE_ASSESSMENT_LEVEL_QUERY.format(
        table_name=HISTORICAL_TABLE_NAME,
        updated_assessment_level=updated_assessment_level,
        ids_query=UPGRADE_DEPRECATED_ASSESSMENT_LEVELS_QUERY.format(
            table_name=HISTORICAL_TABLE_NAME,
        ),
    )

    connection.execute(update_assessment_level_query)
    connection.execute(update_assessment_level_historical_query)


def downgrade() -> None:
    connection = op.get_bind()

    undetermined_assessment_level_raw_text = "UNDETERMINED"
    undetermined_assessment_level = "UNDETERMINED"

    update_undetermined_level_query = UPDATE_ASSESSMENT_LEVEL_QUERY.format(
        table_name=TABLE_NAME,
        updated_assessment_level=undetermined_assessment_level,
        ids_query=DOWNGRADE_DEPRECATED_LEVEL_QUERY.format(
            table_name=TABLE_NAME,
            assessment_level_raw_text_value=undetermined_assessment_level_raw_text,
        ),
    )

    update_undetermined_level_historical_query = UPDATE_ASSESSMENT_LEVEL_QUERY.format(
        table_name=HISTORICAL_TABLE_NAME,
        updated_assessment_level=undetermined_assessment_level,
        ids_query=DOWNGRADE_DEPRECATED_LEVEL_QUERY.format(
            table_name=HISTORICAL_TABLE_NAME,
            assessment_level_raw_text_value=undetermined_assessment_level_raw_text,
        ),
    )

    not_applicable_assessment_level_raw_text = "NOT APPLICABLE"
    not_applicable_assessment_level = "NOT_APPLICABLE"

    update_not_applicable_level_query = UPDATE_ASSESSMENT_LEVEL_QUERY.format(
        table_name=TABLE_NAME,
        updated_assessment_level=not_applicable_assessment_level,
        ids_query=DOWNGRADE_DEPRECATED_LEVEL_QUERY.format(
            table_name=TABLE_NAME,
            assessment_level_raw_text_value=not_applicable_assessment_level_raw_text,
        ),
    )

    update_not_applicable_level_historical_query = UPDATE_ASSESSMENT_LEVEL_QUERY.format(
        table_name=HISTORICAL_TABLE_NAME,
        updated_assessment_level=not_applicable_assessment_level,
        ids_query=DOWNGRADE_DEPRECATED_LEVEL_QUERY.format(
            table_name=HISTORICAL_TABLE_NAME,
            assessment_level_raw_text_value=not_applicable_assessment_level_raw_text,
        ),
    )

    connection.execute(update_undetermined_level_query)
    connection.execute(update_undetermined_level_historical_query)
    connection.execute(update_not_applicable_level_query)
    connection.execute(update_not_applicable_level_historical_query)
