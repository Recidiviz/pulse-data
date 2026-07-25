# pylint: skip-file
"""make_min_confidence_level_nullable

Revision ID: 4c9d2e8b7a31
Revises: 6fa7a37f12b3
Create Date: 2026-07-22 16:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4c9d2e8b7a31"
down_revision = "6fa7a37f12b3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "llm_extractor_collection",
        "minimum_confidence_level",
        existing_type=sa.String(length=255),
        nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "llm_extractor_collection",
        "minimum_confidence_level",
        existing_type=sa.String(length=255),
        nullable=False,
    )
