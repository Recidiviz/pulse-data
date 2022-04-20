# pylint: skip-file
"""add_unknown_values_state_court_type

Revision ID: ef114fe1374c
Revises: e4a9ba491fe6
Create Date: 2022-04-14 18:24:44.418460

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ef114fe1374c"
down_revision = "e4a9ba491fe6"
branch_labels = None
depends_on = None

# Without new value
old_values = ["PRESENT_WITHOUT_INFO"]

# With new value
new_values = ["PRESENT_WITHOUT_INFO", "EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN"]


def upgrade() -> None:
    op.execute("ALTER TYPE state_court_type RENAME TO state_court_type_old;")
    sa.Enum(*new_values, name="state_court_type").create(bind=op.get_bind())
    op.alter_column(
        "state_court_case",
        column_name="court_type",
        type_=sa.Enum(*new_values, name="state_court_type"),
        postgresql_using="court_type::text::state_court_type",
    )
    op.alter_column(
        "state_court_case_history",
        column_name="court_type",
        type_=sa.Enum(*new_values, name="state_court_type"),
        postgresql_using="court_type::text::state_court_type",
    )
    op.execute("DROP TYPE state_court_type_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_court_type RENAME TO state_court_type_old;")
    sa.Enum(*old_values, name="state_court_type").create(bind=op.get_bind())
    op.alter_column(
        "state_court_case",
        column_name="court_type",
        type_=sa.Enum(*old_values, name="state_court_type"),
        postgresql_using="court_type::text::state_court_type",
    )
    op.alter_column(
        "state_court_case_history",
        column_name="court_type",
        type_=sa.Enum(*old_values, name="state_court_type"),
        postgresql_using="court_type::text::state_court_type",
    )
    op.execute("DROP TYPE state_court_type_old;")
