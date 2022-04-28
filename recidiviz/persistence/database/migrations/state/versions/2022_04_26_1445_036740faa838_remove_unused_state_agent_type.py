# pylint: skip-file
"""remove_unused_state_agent_type

Revision ID: 036740faa838
Revises: 1142b91484ed
Create Date: 2022-04-26 14:45:57.792795

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "036740faa838"
down_revision = "a29bd2a64aa6"
branch_labels = None
depends_on = None

# Without old values
old_values = [
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

# Without old values
new_values = [
    "PRESENT_WITHOUT_INFO",
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "SUPERVISION_OFFICER",
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
