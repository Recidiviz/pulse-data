# pylint: skip-file
"""boolean to multiple choice

Revision ID: 0e525a53c526
Revises: ae3b052406d1
Create Date: 2022-06-06 11:23:02.362156

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0e525a53c526"
down_revision = "ae3b052406d1"
branch_labels = None
depends_on = None

# Without new value
old_values = ["TEXT", "BOOLEAN", "NUMBER"]

# With new value
new_values = ["TEXT", "MULTIPLE_CHOICE", "NUMBER"]


def upgrade() -> None:
    op.execute("ALTER TYPE valuetype RENAME TO valuetype_old;")
    sa.Enum(*new_values, name="valuetype").create(bind=op.get_bind())
    op.alter_column(
        "datapoint",
        column_name="value_type",
        type_=sa.Enum(*new_values, name="valuetype"),
        postgresql_using="value_type::text::valuetype",
    )
    op.execute("DROP TYPE valuetype_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE valuetype RENAME TO valuetype_old;")
    sa.Enum(*old_values, name="valuetype").create(bind=op.get_bind())
    op.alter_column(
        "datapoint",
        column_name="value_type",
        type_=sa.Enum(*old_values, name="valuetype"),
        postgresql_using="value_type::text::valuetype",
    )
    op.execute("DROP TYPE valuetype_old;")
