# pylint: skip-file
"""drop_bday_inferred_from_age

Revision ID: 8866392fed3d
Revises: 5823cf21ee2e
Create Date: 2021-11-02 19:20:03.197893

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8866392fed3d"
down_revision = "5823cf21ee2e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("etl_clients", "birthdate_inferred_from_age")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "etl_clients",
        sa.Column(
            "birthdate_inferred_from_age",
            sa.BOOLEAN(),
            autoincrement=False,
            nullable=True,
        ),
    )
    # ### end Alembic commands ###
