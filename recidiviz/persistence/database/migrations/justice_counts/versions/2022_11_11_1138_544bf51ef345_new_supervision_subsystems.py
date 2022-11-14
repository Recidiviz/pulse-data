# pylint: skip-file
"""new supervision subsystems

Revision ID: 544bf51ef345
Revises: 807ad876bc7e
Create Date: 2022-11-11 11:38:32.044260

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "544bf51ef345"
down_revision = "807ad876bc7e"
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
    "POST_RELEASE",
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
    "DUAL_SUPERVISION",
    "PRETRIAL_SUPERVISION",
    "OTHER_SUPERVISION",
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
