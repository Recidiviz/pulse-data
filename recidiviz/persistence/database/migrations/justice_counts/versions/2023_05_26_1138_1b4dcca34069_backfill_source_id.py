# pylint: skip-file
"""backfill source_id

Revision ID: 1b4dcca34069
Revises: 9d49a18ef071
Create Date: 2023-05-26 11:38:39.260237

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1b4dcca34069"
down_revision = "9d49a18ef071"
branch_labels = None
depends_on = None


# backfill is_report_datapoint field
UPDATE_QUERY_1 = """
UPDATE datapoint
SET is_report_datapoint = TRUE
WHERE source_id IS NULL;
"""

UPDATE_QUERY_2 = """
UPDATE datapoint
SET is_report_datapoint = FALSE
WHERE source_id IS NOT NULL
AND is_report_datapoint IS NULL;
"""

# backfill source_id field
UPDATE_QUERY_3 = """
WITH ids AS (
    SELECT DISTINCT
    datapoint.report_id,
    report.source_id
    FROM datapoint
    JOIN report ON datapoint.report_id = report.id
)
UPDATE datapoint
SET source_id = ids.source_id
FROM ids
WHERE datapoint.source_id IS NULL
AND datapoint.report_id = ids.report_id
"""


def upgrade() -> None:
    connection = op.get_bind()
    connection.execute(UPDATE_QUERY_1)
    connection.execute(UPDATE_QUERY_2)
    connection.execute(UPDATE_QUERY_3)


def downgrade() -> None:
    return None
