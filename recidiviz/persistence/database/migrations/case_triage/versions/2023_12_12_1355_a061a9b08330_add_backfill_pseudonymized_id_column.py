# pylint: skip-file
"""add/backfill pseudonymized_id column

Revision ID: a061a9b08330
Revises: 9a817e072599
Create Date: 2023-12-12 13:55:08.699824

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "a061a9b08330"
down_revision = "9a817e072599"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "user_override",
        sa.Column("pseudonymized_id", sa.String(length=255), nullable=True),
    )
    # Postgres equivalent of recidiviz.calculator.query.bq_utils.get_pseudonymized_id_query_str("state_code || external_id")
    backfill_pseudo_id_query = """UPDATE {table} SET pseudonymized_id=SUBSTRING(
            REPLACE(
                REPLACE(
                    ENCODE(SHA256(state_code::bytea || external_id::bytea), 'base64'),
                    '+',
                    '-'
                ),
                '/', 
                '_'
            ), 
            1,
            16
        ) WHERE external_id IS NOT NULL
    """
    op.execute(StrictStringFormatter().format(backfill_pseudo_id_query, table="roster"))
    op.execute(
        StrictStringFormatter().format(backfill_pseudo_id_query, table="user_override")
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("user_override", "pseudonymized_id")
    # ### end Alembic commands ###