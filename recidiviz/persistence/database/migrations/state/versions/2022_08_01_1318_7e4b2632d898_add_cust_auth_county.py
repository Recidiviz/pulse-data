# pylint: skip-file
"""add_cust_auth_county

Revision ID: 7e4b2632d898
Revises: 952d4126cb5f
Create Date: 2022-08-01 13:18:09.130482

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7e4b2632d898"
down_revision = "952d4126cb5f"
branch_labels = None
depends_on = None

# Without COUNTY
old_values = [
    "COURT",
    "FEDERAL",
    "OTHER_COUNTRY",
    "OTHER_STATE",
    "SUPERVISION_AUTHORITY",
    "STATE_PRISON",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# With COUNTY
new_values = [
    "COURT",
    "COUNTY",
    "FEDERAL",
    "OTHER_COUNTRY",
    "OTHER_STATE",
    "SUPERVISION_AUTHORITY",
    "STATE_PRISON",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
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
        "state_supervision_period",
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
        "state_supervision_period",
        column_name="custodial_authority",
        type_=sa.Enum(*old_values, name="state_custodial_authority"),
        postgresql_using="custodial_authority::text::state_custodial_authority",
    )

    op.execute("DROP TYPE state_custodial_authority_old;")
