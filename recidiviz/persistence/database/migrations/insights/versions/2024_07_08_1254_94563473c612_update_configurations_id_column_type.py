# pylint: skip-file
"""update_configurations_id_column_type

Revision ID: 94563473c612
Revises: 72c993bcaaca
Create Date: 2024-07-08 12:54:08.578311

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "94563473c612"
down_revision = "72c993bcaaca"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "configurations",
        sa.Column("id_new", sa.Integer(), sa.Identity(), primary_key=True),
    )
    op.drop_column("configurations", "id")
    op.alter_column("configurations", "id_new", nullable=False, new_column_name="id")
    op.create_primary_key("configurations_pkey", "configurations", ["id"])
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "configurations",
        sa.Column("id_original", sa.Integer(), autoincrement=True, nullable=False),
    )
    op.drop_column("configurations", "id")
    op.alter_column(
        "configurations", "id_original", nullable=False, new_column_name="id"
    )
    op.create_primary_key("configurations_pkey", "configurations", ["id"])
    # ### end Alembic commands ###