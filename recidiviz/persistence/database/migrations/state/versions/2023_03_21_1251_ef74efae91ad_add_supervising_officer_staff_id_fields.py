# pylint: skip-file
"""add_supervising_officer_staff_id_fields

Revision ID: ef74efae91ad
Revises: 7265620b4316
Create Date: 2023-03-21 12:51:27.995570

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ef74efae91ad"
down_revision = "7265620b4316"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "state_supervision_period",
        sa.Column(
            "supervising_officer_staff_external_id",
            sa.String(length=255),
            nullable=True,
            comment="The external id of this person’s supervising officer during this period. This field with the supervising_officer_staff_external_id_type field make up a primary key for the state_staff_external_id table.",
        ),
    )
    op.add_column(
        "state_supervision_period",
        sa.Column(
            "supervising_officer_staff_external_id_type",
            sa.String(length=255),
            nullable=True,
            comment="The ID type associated with the external id of this person’s supervising officer during this period. This field with the supervising_officer_staff_external_id field make up a primary key for the state_staff_external_id table.",
        ),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column(
        "state_supervision_period", "supervising_officer_staff_external_id_type"
    )
    op.drop_column("state_supervision_period", "supervising_officer_staff_external_id")
    # ### end Alembic commands ###
