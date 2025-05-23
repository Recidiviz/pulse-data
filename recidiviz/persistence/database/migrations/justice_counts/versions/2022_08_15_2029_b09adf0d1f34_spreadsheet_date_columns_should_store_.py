# pylint: skip-file
"""spreadsheet date columns should store datetime

Revision ID: b09adf0d1f34
Revises: ae86afc555d2
Create Date: 2022-08-15 20:29:54.032003

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b09adf0d1f34"
down_revision = "ae86afc555d2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "spreadsheet",
        "uploaded_at",
        existing_type=sa.DATE(),
        type_=sa.DateTime(),
        existing_nullable=False,
    )
    op.alter_column(
        "spreadsheet",
        "ingested_at",
        existing_type=sa.DATE(),
        type_=sa.DateTime(),
        existing_nullable=True,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "spreadsheet",
        "ingested_at",
        existing_type=sa.DateTime(),
        type_=sa.DATE(),
        existing_nullable=True,
    )
    op.alter_column(
        "spreadsheet",
        "uploaded_at",
        existing_type=sa.DateTime(),
        type_=sa.DATE(),
        existing_nullable=False,
    )
    # ### end Alembic commands ###
