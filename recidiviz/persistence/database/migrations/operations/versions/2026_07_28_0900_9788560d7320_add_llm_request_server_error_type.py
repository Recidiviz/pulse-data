# pylint: skip-file
"""add_llm_request_server_error_type

Revision ID: 9788560d7320
Revises: b3b1ea07b814
Create Date: 2026-07-28 00:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9788560d7320"
down_revision = "b3b1ea07b814"
branch_labels = None
depends_on = None


old_values = [
    "LLM_REQUEST_MALFORMED_RESPONSE",
    "LLM_REQUEST_EMPTY_RESPONSE",
    "LLM_REQUEST_CONTENT_FILTERED",
    "LLM_REQUEST_TIMEOUT",
    "LLM_REQUEST_RATE_LIMITED",
    "LLM_REQUEST_UNKNOWN_ERROR",
]

# Add LLM_REQUEST_SERVER_ERROR
new_values = [
    "LLM_REQUEST_MALFORMED_RESPONSE",
    "LLM_REQUEST_EMPTY_RESPONSE",
    "LLM_REQUEST_CONTENT_FILTERED",
    "LLM_REQUEST_TIMEOUT",
    "LLM_REQUEST_RATE_LIMITED",
    "LLM_REQUEST_SERVER_ERROR",
    "LLM_REQUEST_UNKNOWN_ERROR",
]

_TABLE = "llm_extraction_job_document"
_COLUMN = "error_type"
_CONSTRAINT = "llm_extraction_job_document_error_type_and_message_consistent"
_CONSTRAINT_CONDITION = "(error_type IS NULL) = (error_message IS NULL)"


def _replace_enum(values: list[str]) -> None:
    op.drop_constraint(_CONSTRAINT, _TABLE, type_="check")
    op.execute(
        "ALTER TYPE llm_document_extraction_error_type "
        "RENAME TO llm_document_extraction_error_type_old;"
    )
    sa.Enum(*values, name="llm_document_extraction_error_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        _TABLE,
        column_name=_COLUMN,
        type_=sa.Enum(*values, name="llm_document_extraction_error_type"),
        postgresql_using=f"{_COLUMN}::text::llm_document_extraction_error_type",
    )
    op.execute("DROP TYPE llm_document_extraction_error_type_old;")
    op.create_check_constraint(_CONSTRAINT, _TABLE, _CONSTRAINT_CONDITION)


def upgrade() -> None:
    _replace_enum(new_values)


def downgrade() -> None:
    _replace_enum(old_values)
