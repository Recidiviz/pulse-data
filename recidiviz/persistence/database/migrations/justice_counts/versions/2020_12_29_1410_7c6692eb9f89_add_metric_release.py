# pylint: skip-file
"""add_metric_release

Revision ID: 7c6692eb9f89
Revises: 74dba57725f2
Create Date: 2020-12-29 14:10:01.987769

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7c6692eb9f89"
down_revision = "74dba57725f2"
branch_labels = None
depends_on = None

# Without new value
old_values = ["ADMISSIONS", "ARRESTS", "POPULATION", "REVOCATIONS", "TERMINATIONS"]

# With new value
new_values = ["ADMISSIONS", "ARRESTS", "POPULATION", "REVOCATIONS", "RELEASES"]


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
