# pylint: skip-file
"""add_supervision_start_metric

Revision ID: b9e10f9e4cee
Revises: 94fe690bc8ab
Create Date: 2021-06-07 11:19:25.818764

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b9e10f9e4cee"
down_revision = "94fe690bc8ab"
branch_labels = None
depends_on = None

# Without new value
old_values = ["ADMISSIONS", "ARRESTS", "POPULATION", "REVOCATIONS", "RELEASES"]

# With new value
new_values = [
    "ADMISSIONS",
    "ARRESTS",
    "POPULATION",
    "REVOCATIONS",
    "RELEASES",
    "SUPERVISION_STARTS",
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
