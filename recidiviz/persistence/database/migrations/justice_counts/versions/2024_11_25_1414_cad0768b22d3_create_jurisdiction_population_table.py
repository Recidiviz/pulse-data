# pylint: skip-file
"""create jurisdiction population table

Revision ID: cad0768b22d3
Revises: e8b2f27aca63
Create Date: 2024-11-25 14:14:40.616950

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "cad0768b22d3"
down_revision = "e8b2f27aca63"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "jurisdiction_population",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("geoid", sa.String(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("race_ethnicity", sa.String(), nullable=True),
        sa.Column("sex", sa.String(), nullable=True),
        sa.Column("population", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_jurisdiction_population_geoid"),
        "jurisdiction_population",
        ["geoid"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        op.f("ix_jurisdiction_population_geoid"), table_name="jurisdiction_population"
    )
    op.drop_table("jurisdiction_population")
    # ### end Alembic commands ###
