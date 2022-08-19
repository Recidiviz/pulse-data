# pylint: skip-file
"""add post release

Revision ID: fa77a0723657
Revises: b09adf0d1f34
Create Date: 2022-08-17 15:57:22.666518

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fa77a0723657"
down_revision = "b09adf0d1f34"
branch_labels = None
depends_on = None


# Without new value
old_values = [
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
    "PAROLE",
    "PROBATION",
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
    "PAROLE",
    "PROBATION",
    "POST_RELEASE",
]


def upgrade() -> None:
    op.execute("ALTER TYPE system RENAME TO System_old;")
    sa.Enum(*new_values, name="system").create(bind=op.get_bind())
    op.alter_column(
        "spreadsheet",
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
        "spreadsheet",
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
