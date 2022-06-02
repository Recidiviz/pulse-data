# pylint: skip-file
"""multiple systems per agency

Revision ID: ae3b052406d1
Revises: cbbab0a044d3
Create Date: 2022-06-01 16:53:52.487197

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ae3b052406d1"
down_revision = "cbbab0a044d3"
branch_labels = None
depends_on = None

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
]

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
    op.add_column(
        "source",
        sa.Column(
            "systems",
            sa.ARRAY(sa.Enum(*new_values, name="system")),
            nullable=True,
        ),
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
    op.drop_column("source", "systems")
    op.execute("DROP TYPE System_old;")
