# pylint: skip-file
"""add_phone_email

Revision ID: 32d171c066ec
Revises: b8bb3d17b512
Create Date: 2022-08-03 15:45:31.855487

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "32d171c066ec"
down_revision = "b8bb3d17b512"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "state_person",
        sa.Column(
            "current_email_address",
            sa.Text(),
            nullable=True,
            comment="The current email address of the person.",
        ),
    )
    op.add_column(
        "state_person",
        sa.Column(
            "current_phone_number",
            sa.Text(),
            nullable=True,
            comment="The current phone number of the person.",
        ),
    )


def downgrade() -> None:
    op.drop_column("state_person", "current_phone_number")
    op.drop_column("state_person", "current_email_address")
