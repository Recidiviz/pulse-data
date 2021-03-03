"""add_internal_unknown_agent_type

Revision ID: ef2616e5aaa1
Revises: f46410b7d550
Create Date: 2019-12-26 17:54:38.683094

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "ef2616e5aaa1"
down_revision = "f46410b7d550"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "PAROLE_BOARD_MEMBER",
    "SUPERVISION_OFFICER",
    "UNIT_SUPERVISOR",
    "PRESENT_WITHOUT_INFO",
]

# With new value
new_values = [
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "PAROLE_BOARD_MEMBER",
    "SUPERVISION_OFFICER",
    "UNIT_SUPERVISOR",
    "PRESENT_WITHOUT_INFO",
    "INTERNAL_UNKNOWN",
]


def upgrade():
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


def downgrade():
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
