# pylint: skip-file
"""Add etl_clients table

Revision ID: 5206e2efa320
Revises: 
Create Date: 2021-01-19 15:38:14.797375

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5206e2efa320"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "etl_clients",
        sa.Column(
            "supervising_officer_external_id",
            sa.String(255),
            nullable=False,
            index=True,
        ),
        sa.Column("person_external_id", sa.String(255), nullable=False, index=True),
        sa.Column("full_name", sa.String(255)),
        sa.Column("current_address", sa.Text),
        sa.Column("birthdate", sa.Date),
        sa.Column("birthdate_inferred_from_age", sa.Boolean),
        sa.Column("supervision_type", sa.String(255), nullable=False),
        sa.Column("case_type", sa.String(255), nullable=False),
        sa.Column("supervision_level", sa.String(255), nullable=False),
        sa.Column("state_code", sa.String(255), nullable=False, index=True),
    )

    op.create_unique_constraint(
        "uniq_etl_clients",
        "etl_clients",
        [
            "state_code",
            "person_external_id",
        ],
    )


def downgrade():
    op.drop_constraint("uniq_etl_clients", "etl_clients")
    op.drop_table("etl_clients")
