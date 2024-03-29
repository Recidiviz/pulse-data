# pylint: skip-file
"""next_face_to_face

Revision ID: 647ed11c1873
Revises: 9462a38c6110
Create Date: 2021-08-11 13:07:39.721652

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "647ed11c1873"
down_revision = "9462a38c6110"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "etl_clients",
        sa.Column("next_recommended_face_to_face_date", sa.Date(), nullable=True),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("etl_clients", "next_recommended_face_to_face_date")
    # ### end Alembic commands ###
