# pylint: skip-file
"""DOC label

Revision ID: efbdc90b6db8
Revises: 643e163e6760
Create Date: 2024-05-23 11:43:25.675531

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "efbdc90b6db8"
down_revision = "643e163e6760"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "configurations",
        sa.Column("doc_label", sa.String(), server_default="DOC", nullable=False),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("configurations", "doc_label")
    # ### end Alembic commands ###