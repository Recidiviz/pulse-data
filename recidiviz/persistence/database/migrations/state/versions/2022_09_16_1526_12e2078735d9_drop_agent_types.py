# pylint: skip-file
"""drop_agent_types

Revision ID: 12e2078735d9
Revises: e1b41fb3caa0
Create Date: 2022-09-16 15:26:07.025446

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "12e2078735d9"
down_revision = "e1b41fb3caa0"
branch_labels = None
depends_on = None


# Without the added values
old_values = [
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "JUSTICE",
    "SUPERVISION_OFFICER",
    "PRESENT_WITHOUT_INFO",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# With added values
new_values = [
    "SUPERVISION_OFFICER",
    "PRESENT_WITHOUT_INFO",
    "INTERNAL_UNKNOWN",
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

    op.execute("DROP TYPE state_agent_type_old;")
