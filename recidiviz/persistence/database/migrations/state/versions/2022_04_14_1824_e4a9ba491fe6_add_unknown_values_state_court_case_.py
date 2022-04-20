# pylint: skip-file
"""add_unknown_values_state_court_case_status

Revision ID: e4a9ba491fe6
Revises: b06cea6738c5
Create Date: 2022-04-14 18:24:48.329250

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e4a9ba491fe6"
down_revision = "b06cea6738c5"
branch_labels = None
depends_on = None

# Without new value
old_values = ["EXTERNAL_UNKNOWN", "PRESENT_WITHOUT_INFO"]

# With new value
new_values = ["EXTERNAL_UNKNOWN", "PRESENT_WITHOUT_INFO", "INTERNAL_UNKNOWN"]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_court_case_status RENAME TO state_court_case_status_old;"
    )
    sa.Enum(*new_values, name="state_court_case_status").create(bind=op.get_bind())
    op.alter_column(
        "state_court_case",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_court_case_status"),
        postgresql_using="status::text::state_court_case_status",
    )
    op.alter_column(
        "state_court_case_history",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_court_case_status"),
        postgresql_using="status::text::state_court_case_status",
    )
    op.execute("DROP TYPE state_court_case_status_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_court_case_status RENAME TO state_court_case_status_old;"
    )
    sa.Enum(*old_values, name="state_court_case_status").create(bind=op.get_bind())
    op.alter_column(
        "state_court_case",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_court_case_status"),
        postgresql_using="status::text::state_court_case_status",
    )
    op.alter_column(
        "state_court_case_history",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_court_case_status"),
        postgresql_using="status::text::state_court_case_status",
    )
    op.execute("DROP TYPE state_court_case_status_old;")
