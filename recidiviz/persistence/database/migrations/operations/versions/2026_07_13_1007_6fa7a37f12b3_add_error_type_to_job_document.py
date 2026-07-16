# pylint: skip-file
"""add_error_type_to_job_document

Revision ID: 6fa7a37f12b3
Revises: b51842610fe9
Create Date: 2026-07-13 10:07:56.259297

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "6fa7a37f12b3"
down_revision = "b51842610fe9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    error_type_enum = sa.Enum(
        "LLM_REQUEST_MALFORMED_RESPONSE",
        "LLM_REQUEST_EMPTY_RESPONSE",
        "LLM_REQUEST_CONTENT_FILTERED",
        "LLM_REQUEST_TIMEOUT",
        "LLM_REQUEST_RATE_LIMITED",
        "LLM_REQUEST_UNKNOWN_ERROR",
        name="llm_document_extraction_error_type",
    )
    error_type_enum.create(bind=op.get_bind())
    op.add_column(
        "llm_extraction_job_document",
        sa.Column("error_type", error_type_enum, nullable=True),
    )
    op.create_check_constraint(
        "llm_extraction_job_document_error_type_and_message_consistent",
        "llm_extraction_job_document",
        "(error_type IS NULL) = (error_message IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "llm_extraction_job_document_error_type_and_message_consistent",
        "llm_extraction_job_document",
        type_="check",
    )
    op.drop_column("llm_extraction_job_document", "error_type")
    postgresql.ENUM(
        name="llm_document_extraction_error_type",
    ).drop(op.get_bind())
