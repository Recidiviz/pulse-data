# pylint: skip-file
"""add superagency sector

Revision ID: 73ad44e4304e
Revises: d77dcb2be80d
Create Date: 2023-08-09 16:04:55.735821

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "73ad44e4304e"
down_revision = "d77dcb2be80d"
branch_labels = None
depends_on = None


# With old value
old_system_values = [
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

# With new value
new_system_values = [
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
    "SUPERAGENCY",
]


def upgrade() -> None:
    op.execute("ALTER TYPE system RENAME TO system_old;")
    sa.Enum(*new_system_values, name="system").create(bind=op.get_bind())
    op.alter_column(
        "spreadsheet",
        column_name="system",
        type_=sa.Enum(*new_system_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.alter_column(
        "report_table_definition",
        column_name="system",
        type_=sa.Enum(*new_system_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.execute("DROP TYPE system_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE system RENAME TO system_old;")
    sa.Enum(*old_system_values, name="system").create(bind=op.get_bind())
    op.alter_column(
        "spreadsheet",
        column_name="system",
        type_=sa.Enum(*old_system_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.alter_column(
        "report_table_definition",
        column_name="system",
        type_=sa.Enum(*old_system_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.execute("DROP TYPE system_old;")
