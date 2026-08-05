# pylint: skip-file
"""widen_extractor_collection_description

Revision ID: 7729ae98dbb1
Revises: 9788560d7320
Create Date: 2026-08-04 12:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7729ae98dbb1"
down_revision = "9788560d7320"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "llm_extractor_collection",
        "description",
        existing_type=sa.String(length=255),
        type_=sa.Text(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "llm_extractor_collection",
        "description",
        existing_type=sa.Text(),
        type_=sa.String(length=255),
        existing_nullable=False,
    )
