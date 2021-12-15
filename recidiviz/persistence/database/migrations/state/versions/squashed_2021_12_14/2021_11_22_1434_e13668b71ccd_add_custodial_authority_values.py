# pylint: skip-file
"""add_custodial_authority_values

Revision ID: e13668b71ccd
Revises: 13d81476169d
Create Date: 2021-11-22 14:34:39.622233

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e13668b71ccd"
down_revision = "13d81476169d"
branch_labels = None
depends_on = None


# Without new value
old_values = [
    "FEDERAL",
    "OTHER_COUNTRY",
    "OTHER_STATE",
    "SUPERVISION_AUTHORITY",
    "STATE_PRISON",
]

# With new value
new_values = [
    "COURT",
    "EXTERNAL_UNKNOWN",
    "FEDERAL",
    "INTERNAL_UNKNOWN",
    "OTHER_COUNTRY",
    "OTHER_STATE",
    "SUPERVISION_AUTHORITY",
    "STATE_PRISON",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_custodial_authority RENAME TO state_custodial_authority_old;"
    )
    sa.Enum(*new_values, name="state_custodial_authority").create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="custodial_authority",
        type_=sa.Enum(*new_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="custodial_authority",
        type_=sa.Enum(*new_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.alter_column(
        "state_supervision_period",
        column_name="custodial_authority",
        type_=sa.Enum(*new_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="custodial_authority",
        type_=sa.Enum(*new_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.execute("DROP TYPE state_custodial_authority_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_custodial_authority RENAME TO state_custodial_authority_old;"
    )
    sa.Enum(*old_values, name="state_custodial_authority").create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_period",
        column_name="custodial_authority",
        type_=sa.Enum(*old_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="custodial_authority",
        type_=sa.Enum(*old_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.alter_column(
        "state_supervision_period",
        column_name="custodial_authority",
        type_=sa.Enum(*old_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="custodial_authority",
        type_=sa.Enum(*old_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )
    op.execute("DROP TYPE state_custodial_authority_old;")
