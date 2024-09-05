# pylint: skip-file
"""Update import status enum

Revision ID: 39506bc45302
Revises: f64925b297f3
Create Date: 2024-08-23 10:47:10.210562

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "39506bc45302"
down_revision = "f64925b297f3"
branch_labels = None
depends_on = None

old_values = [
    "STARTED",
    "SUCCEEDED",
    "FAILED_UNKNOWN",
    "FAILED_PRE_IMPORT_NORMALIZATION_STEP",
    "FAILED_LOAD_STEP",
]

# Add FAILED_VALIDATION_STEP
new_values = [
    "STARTED",
    "SUCCEEDED",
    "FAILED_UNKNOWN",
    "FAILED_PRE_IMPORT_NORMALIZATION_STEP",
    "FAILED_LOAD_STEP",
    "FAILED_VALIDATION_STEP",
]


def upgrade() -> None:
    op.drop_constraint(
        "all_succeeded_imports_must_have_non_null_rows",
        "direct_ingest_raw_file_import",
        type_="check",
    )
    op.drop_constraint(
        "historical_diffs_must_have_non_null_updated_and_deleted",
        "direct_ingest_raw_file_import",
        type_="check",
    )

    op.execute(
        "ALTER TYPE direct_ingest_file_import_status RENAME TO direct_ingest_file_import_status_old;"
    )
    sa.Enum(*new_values, name="direct_ingest_file_import_status").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "direct_ingest_raw_file_import",
        column_name="import_status",
        type_=sa.Enum(*new_values, name="direct_ingest_file_import_status"),
        postgresql_using="import_status::text::direct_ingest_file_import_status",
    )
    op.create_check_constraint(
        "historical_diffs_must_have_non_null_updated_and_deleted",
        "direct_ingest_raw_file_import",
        "(historical_diffs_active IS FALSE OR import_status != 'SUCCEEDED')OR (historical_diffs_active IS TRUE AND import_status = 'SUCCEEDED' AND net_new_or_updated_rows IS NOT NULL AND deleted_rows IS NOT NULL)",
    )
    op.create_check_constraint(
        "all_succeeded_imports_must_have_non_null_rows",
        "direct_ingest_raw_file_import",
        "import_status != 'SUCCEEDED' OR (import_status = 'SUCCEEDED' AND raw_rows IS NOT NULL)",
    )

    op.execute("DROP TYPE direct_ingest_file_import_status_old;")


def downgrade() -> None:
    op.drop_constraint(
        "all_succeeded_imports_must_have_non_null_rows",
        "direct_ingest_raw_file_import",
        type_="check",
    )
    op.drop_constraint(
        "historical_diffs_must_have_non_null_updated_and_deleted",
        "direct_ingest_raw_file_import",
        type_="check",
    )

    op.execute(
        "ALTER TYPE direct_ingest_file_import_status RENAME TO direct_ingest_file_import_status_old;"
    )
    sa.Enum(*old_values, name="direct_ingest_file_import_status").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "direct_ingest_raw_file_import",
        column_name="import_status",
        type_=sa.Enum(*old_values, name="direct_ingest_file_import_status"),
        postgresql_using="import_status::text::direct_ingest_file_import_status",
    )

    op.create_check_constraint(
        "historical_diffs_must_have_non_null_updated_and_deleted",
        "direct_ingest_raw_file_import",
        "(historical_diffs_active IS FALSE OR import_status != 'SUCCEEDED')OR (historical_diffs_active IS TRUE AND import_status = 'SUCCEEDED' AND net_new_or_updated_rows IS NOT NULL AND deleted_rows IS NOT NULL)",
    )
    op.create_check_constraint(
        "all_succeeded_imports_must_have_non_null_rows",
        "direct_ingest_raw_file_import",
        "import_status != 'SUCCEEDED' OR (import_status = 'SUCCEEDED' AND raw_rows IS NOT NULL)",
    )

    op.execute("DROP TYPE direct_ingest_file_import_status_old;")
