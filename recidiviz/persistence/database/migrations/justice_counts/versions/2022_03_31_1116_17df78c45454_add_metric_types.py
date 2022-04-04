# pylint: skip-file
"""add_metric_types

Revision ID: 17df78c45454
Revises: 783e6cdf45a8
Create Date: 2022-03-31 11:16:39.813191

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "17df78c45454"
down_revision = "783e6cdf45a8"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ADMISSIONS",
    "ARRESTS",
    "POPULATION",
    "REVOCATIONS",
    "RELEASES",
    "SUPERVISION_STARTS",
]

# With new value
new_values = [
    "ADMISSIONS",
    "ARRESTS",
    "POPULATION",
    "REVOCATIONS",
    "RELEASES",
    "SUPERVISION_STARTS",
    "BUDGET",
    "CALLS_FOR_SERVICE",
    "COMPLAINTS_SUSTAINED",
    "REPORTED_CRIME",
    "TOTAL_STAFF",
    "USE_OF_FORCE_INCIDENTS",
]


def upgrade() -> None:
    op.execute("ALTER TYPE metrictype RENAME TO metrictype_old;")
    sa.Enum(*new_values, name="metrictype").create(bind=op.get_bind())
    op.alter_column(
        "report_table_definition",
        column_name="metric_type",
        type_=sa.Enum(*new_values, name="metrictype"),
        postgresql_using="metric_type::text::metrictype",
    )
    op.execute("DROP TYPE metrictype_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE metrictype RENAME TO metrictype_old;")
    sa.Enum(*old_values, name="metrictype").create(bind=op.get_bind())
    op.alter_column(
        "report_table_definition",
        column_name="metric_type",
        type_=sa.Enum(*old_values, name="metrictype"),
        postgresql_using="metric_type::text::metrictype",
    )
    op.execute("DROP TYPE metrictype_old;")
