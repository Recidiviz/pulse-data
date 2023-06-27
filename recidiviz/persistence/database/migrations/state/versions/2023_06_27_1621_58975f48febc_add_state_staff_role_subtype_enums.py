# pylint: skip-file
"""add_state_staff_role_subtype_enums

Revision ID: 58975f48febc
Revises: ae6fcf44f068
Create Date: 2023-06-27 16:21:09.201240

"""
import sqlalchemy as sa
from alembic import op

# With SUPERVISION_DISTRICT_MANAGER and SUPERVISION_STATE_LEADERSHIP
new_values = [
    "SUPERVISION_OFFICER",
    "SUPERVISION_OFFICER_SUPERVISOR",
    "SUPERVISION_DISTRICT_MANAGER",
    "SUPERVISION_REGIONAL_MANAGER",
    "SUPERVISION_STATE_LEADERSHIP",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]


# Without SUPERVISION_DISTRICT_MANAGER and SUPERVISION_STATE_LEADERSHIP
old_values = [
    "SUPERVISION_OFFICER",
    "SUPERVISION_OFFICER_SUPERVISOR",
    "SUPERVISION_REGIONAL_MANAGER",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# revision identifiers, used by Alembic.
revision = "58975f48febc"
down_revision = "ae6fcf44f068"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_staff_role_subtype RENAME TO state_staff_role_subtype_old;"
    )
    sa.Enum(*new_values, name="state_staff_role_subtype").create(bind=op.get_bind())
    op.alter_column(
        "state_staff_role_period",
        column_name="role_subtype",
        type_=sa.Enum(*new_values, name="state_staff_role_subtype"),
        postgresql_using="role_subtype::text::state_staff_role_subtype",
    )
    op.execute("DROP TYPE state_staff_role_subtype_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_staff_role_subtype RENAME TO state_staff_role_subtype_old;"
    )
    sa.Enum(*old_values, name="state_staff_role_subtype").create(bind=op.get_bind())
    op.alter_column(
        "state_staff_role_period",
        column_name="role_subtype",
        type_=sa.Enum(*old_values, name="state_staff_role_subtype"),
        postgresql_using="role_subtype::text::state_staff_role_subtype",
    )
    op.execute("DROP TYPE state_staff_role_subtype_old;")
