# pylint: skip-file
"""rename_char_count_to_document_length_bytes

Revision ID: b3b1ea07b814
Revises: 6fa7a37f12b3
Create Date: 2026-07-22 16:00:23.482771

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "b3b1ea07b814"
down_revision = "6fa7a37f12b3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "llm_extraction_eligible_document_metadata",
        "char_count",
        new_column_name="document_length_bytes",
    )


def downgrade() -> None:
    op.alter_column(
        "llm_extraction_eligible_document_metadata",
        "document_length_bytes",
        new_column_name="char_count",
    )
