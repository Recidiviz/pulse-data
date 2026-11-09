# pylint: skip-file
"""add_llm_stale_document_result_type

Revision ID: abdc46ce55de
Revises: c7f2a4d18b06
Create Date: 2026-11-06 00:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "abdc46ce55de"
down_revision = "c7f2a4d18b06"
branch_labels = None
depends_on = None


old_values = [
    "SUCCESS",
    "JOB_LEVEL_FAILURE",
    "DOCUMENT_LEVEL_FAILURE_TRANSIENT",
    "DOCUMENT_LEVEL_FAILURE_PERMANENT",
    "DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED",
]

# Add DOCUMENT_LEVEL_FAILURE_STALE_DOCUMENT
new_values = [
    "SUCCESS",
    "JOB_LEVEL_FAILURE",
    "DOCUMENT_LEVEL_FAILURE_TRANSIENT",
    "DOCUMENT_LEVEL_FAILURE_PERMANENT",
    "DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED",
    "DOCUMENT_LEVEL_FAILURE_STALE_DOCUMENT",
]

_TABLE = "llm_extraction_job_document"
_COLUMN = "result_type"

# The check constraints whose condition references result_type. Postgres cannot
# alter the column's enum type while these bind the old type's operators, so they
# are dropped before the swap and recreated after, with their conditions unchanged.
_RESULT_TYPE_CONSTRAINTS: list[tuple[str, str]] = [
    (
        "llm_extraction_job_document_result_datetime_and_type_consistent",
        "(result_datetime_utc IS NULL) = (result_type IS NULL)",
    ),
    (
        "llm_extraction_job_document_error_message_only_for_failure",
        "error_message IS NULL "
        "OR result_type IN ("
        "'JOB_LEVEL_FAILURE',"
        "'DOCUMENT_LEVEL_FAILURE_TRANSIENT',"
        "'DOCUMENT_LEVEL_FAILURE_PERMANENT',"
        "'DOCUMENT_LEVEL_FAILURE_RETRIES_EXHAUSTED'"
        ")",
    ),
    (
        "llm_extraction_job_document_is_relevant_iff_success",
        "(is_relevant IS NOT NULL) = (result_type IS NOT DISTINCT FROM 'SUCCESS')",
    ),
]


def _replace_enum(values: list[str]) -> None:
    for name, _condition in _RESULT_TYPE_CONSTRAINTS:
        op.drop_constraint(name, _TABLE, type_="check")
    op.execute(
        "ALTER TYPE llm_extraction_job_document_result_type "
        "RENAME TO llm_extraction_job_document_result_type_old;"
    )
    sa.Enum(*values, name="llm_extraction_job_document_result_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        _TABLE,
        column_name=_COLUMN,
        type_=sa.Enum(*values, name="llm_extraction_job_document_result_type"),
        postgresql_using=f"{_COLUMN}::text::llm_extraction_job_document_result_type",
    )
    op.execute("DROP TYPE llm_extraction_job_document_result_type_old;")
    for name, condition in _RESULT_TYPE_CONSTRAINTS:
        op.create_check_constraint(name, _TABLE, condition)


def upgrade() -> None:
    _replace_enum(new_values)


def downgrade() -> None:
    # Fails if any row still holds DOCUMENT_LEVEL_FAILURE_STALE_DOCUMENT: the
    # column cast to the old enum rejects the dropped value. Clear those rows
    # before rolling back. The up/down migration test runs on an empty DB, so it
    # does not exercise this.
    _replace_enum(old_values)
