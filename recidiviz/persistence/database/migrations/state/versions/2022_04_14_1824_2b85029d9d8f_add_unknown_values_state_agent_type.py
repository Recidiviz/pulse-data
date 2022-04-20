# pylint: skip-file
"""add_unknown_values_state_agent_type

Revision ID: 2b85029d9d8f
Revises: 0c13d193fd54
Create Date: 2022-04-14 18:24:08.747994

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2b85029d9d8f"
down_revision = "0c13d193fd54"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "PRESENT_WITHOUT_INFO",
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "PAROLE_BOARD_MEMBER",
    "SUPERVISION_OFFICER",
    "UNIT_SUPERVISOR",
    "INTERNAL_UNKNOWN",
    "JUSTICE",
]

# With new value
new_values = [
    "PRESENT_WITHOUT_INFO",
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "PAROLE_BOARD_MEMBER",
    "SUPERVISION_OFFICER",
    "UNIT_SUPERVISOR",
    "INTERNAL_UNKNOWN",
    "JUSTICE",
    "EXTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_agent_type RENAME TO state_agent_type_old;")
    sa.Enum(*new_values, name="state_agent_type").create(bind=op.get_bind())
    op.alter_column(
        "state_agent",
        column_name="agent_type",
        type_=sa.Enum(*new_values, name="state_agent_type"),
        postgresql_using="agent_type::text::state_agent_type",
    )
    op.alter_column(
        "state_agent_history",
        column_name="agent_type",
        type_=sa.Enum(*new_values, name="state_agent_type"),
        postgresql_using="agent_type::text::state_agent_type",
    )
    op.execute("DROP TYPE state_agent_type_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_agent_type RENAME TO state_agent_type_old;")
    sa.Enum(*old_values, name="state_agent_type").create(bind=op.get_bind())
    op.alter_column(
        "state_agent",
        column_name="agent_type",
        type_=sa.Enum(*old_values, name="state_agent_type"),
        postgresql_using="agent_type::text::state_agent_type",
    )
    op.alter_column(
        "state_agent_history",
        column_name="agent_type",
        type_=sa.Enum(*old_values, name="state_agent_type"),
        postgresql_using="agent_type::text::state_agent_type",
    )
    op.execute("DROP TYPE state_agent_type_old;")
