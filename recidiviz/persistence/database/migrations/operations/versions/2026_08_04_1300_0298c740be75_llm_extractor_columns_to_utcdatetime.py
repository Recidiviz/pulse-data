# pylint: skip-file
"""llm_extractor_columns_to_utcdatetime

Revision ID: 0298c740be75
Revises: 7729ae98dbb1
Create Date: 2026-08-04 13:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0298c740be75"
down_revision = "7729ae98dbb1"
branch_labels = None
depends_on = None

# (table, column) pairs whose type moves between timestamptz and a naive
# timestamp (the UTCDateTime application-layer type).
_COLUMNS = [
    ("llm_extractor_collection", "row_creation_datetime_utc"),
    ("llm_extractor", "row_creation_datetime_utc"),
    ("llm_extractor_version", "invalidated_datetime_utc"),
    ("llm_extractor_version", "row_creation_datetime_utc"),
    ("llm_extractor_document_filter", "row_creation_datetime_utc"),
    ("llm_extraction_eligible_document_metadata", "document_update_datetime"),
    ("llm_extraction_eligible_document_metadata", "row_creation_datetime_utc"),
    ("llm_extraction_eligible_document", "row_creation_datetime_utc"),
    ("llm_extraction_job", "start_datetime_utc"),
    ("llm_extraction_job", "completion_datetime_utc"),
    ("llm_extraction_batch_job_metadata", "row_creation_datetime_utc"),
    ("llm_extraction_job_document", "result_datetime_utc"),
    ("llm_extraction_cap_override", "expires_datetime_utc"),
    ("llm_extraction_cap_override", "row_creation_datetime_utc"),
]


def upgrade() -> None:
    for table, column in _COLUMNS:
        op.alter_column(
            table,
            column,
            existing_type=sa.DateTime(timezone=True),
            type_=sa.DateTime(),
            # Read each stored instant as its UTC wall-clock time so the naive
            # column holds UTC regardless of the session timezone.
            postgresql_using=f"{column} AT TIME ZONE 'UTC'",
        )


def downgrade() -> None:
    for table, column in _COLUMNS:
        op.alter_column(
            table,
            column,
            existing_type=sa.DateTime(),
            type_=sa.DateTime(timezone=True),
            # Interpret each naive value as UTC when re-attaching a timezone.
            postgresql_using=f"{column} AT TIME ZONE 'UTC'",
        )
