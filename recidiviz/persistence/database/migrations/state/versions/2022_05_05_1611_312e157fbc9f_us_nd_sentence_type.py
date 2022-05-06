# pylint: skip-file
"""us_nd_sentence_type

Revision ID: 312e157fbc9f
Revises: dbf7688df462
Create Date: 2022-05-05 16:11:42.390628

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "312e157fbc9f"
down_revision = "09c40b704975"
branch_labels = None
depends_on = None


UPDATE_QUERY = (
    "UPDATE {table_name} SET supervision_type = '{new_value}'"
    " WHERE state_code = 'US_ND'"
    " AND supervision_type = '{old_value}'{optional_filter};"
)

TABLES = [
    "state_supervision_sentence",
    "state_supervision_sentence_history",
]


def upgrade() -> None:
    # Migrate HALFWAY_HOUSE => COMMUNITY_CORRECTIONS
    old_value = "HALFWAY_HOUSE"
    new_value = "COMMUNITY_CORRECTIONS"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                new_value=new_value,
                old_value=old_value,
                optional_filter="",
            )
        )

    # Migrate PRE_CONFINEMENT => INTERNAL_UNKNOWN
    old_value = "PRE_CONFINEMENT"
    new_value = "INTERNAL_UNKNOWN"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                new_value=new_value,
                old_value=old_value,
                optional_filter="",
            )
        )


def downgrade() -> None:
    # Migrate COMMUNITY_CORRECTIONS => HALFWAY_HOUSE
    old_value = "COMMUNITY_CORRECTIONS"
    new_value = "HALFWAY_HOUSE"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                new_value=new_value,
                old_value=old_value,
                optional_filter="",
            )
        )

    # Migrate INTERNAL_UNKNOWN => PRE_CONFINEMENT
    old_value = "INTERNAL_UNKNOWN"
    new_value = "PRE_CONFINEMENT"
    optional_filter = " AND supervision_type_raw_text = 'PRE-TRIAL'"

    for table in TABLES:
        op.execute(
            StrictStringFormatter().format(
                UPDATE_QUERY,
                table_name=table,
                new_value=new_value,
                old_value=old_value,
                optional_filter=optional_filter,
            )
        )
