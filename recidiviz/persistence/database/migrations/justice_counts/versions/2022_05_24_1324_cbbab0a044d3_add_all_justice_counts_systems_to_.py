# pylint: skip-file
"""Add all Justice Counts Systems to System enum

Revision ID: cbbab0a044d3
Revises: e1eeb1acfd77
Create Date: 2022-05-24 13:24:20.527257

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "cbbab0a044d3"
down_revision = "e1eeb1acfd77"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "DEFENSE",
    "JAILS",
    "LAW_ENFORCEMENT",
    "COURT_PROCESSES",
    "COMMUNITY_SUPERVISION_AND_REENTRY",
    "CORRECTIONS",
    "PRISONS",
    "PROSECUTION",
    "SUPERVISION",
]

# With new value
new_values = [
    "DEFENSE",
    "JAILS",
    "LAW_ENFORCEMENT",
    "COURT_PROCESSES",
    "COURTS_AND_PRETRIAL",
    "COMMUNITY_SUPERVISION_AND_REENTRY",
    "CORRECTIONS",
    "PRISONS",
    "PROSECUTION",
    "SUPERVISION",
]


def upgrade() -> None:
    op.execute("ALTER TYPE system RENAME TO System_old;")
    sa.Enum(*new_values, name="system").create(bind=op.get_bind())
    op.alter_column(
        "source",
        column_name="system",
        type_=sa.Enum(*new_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.alter_column(
        "report_table_definition",
        column_name="system",
        type_=sa.Enum(*new_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.execute("DROP TYPE System_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE system RENAME TO System_old;")
    sa.Enum(*old_values, name="system").create(bind=op.get_bind())
    op.alter_column(
        "source",
        column_name="system",
        type_=sa.Enum(*old_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.alter_column(
        "report_table_definition",
        column_name="system",
        type_=sa.Enum(*old_values, name="system"),
        postgresql_using="system::text::system",
    )

    op.execute("DROP TYPE System_old;")
