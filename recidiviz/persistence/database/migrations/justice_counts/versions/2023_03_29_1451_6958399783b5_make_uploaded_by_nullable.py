# pylint: skip-file
"""make uploaded_by nullable

Revision ID: 6958399783b5
Revises: f0c3d8d513d6
Create Date: 2023-03-29 14:51:11.157269

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6958399783b5"
down_revision = "f0c3d8d513d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "spreadsheet", "uploaded_by", existing_type=sa.VARCHAR(), nullable=True
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "spreadsheet", "uploaded_by", existing_type=sa.VARCHAR(), nullable=False
    )
    # ### end Alembic commands ###
