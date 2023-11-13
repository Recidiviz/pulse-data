# pylint: skip-file
"""backfill demo source_id

Revision ID: 6909495c4b03
Revises: e35c6062270a
Create Date: 2023-11-13 14:15:30.719377

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6909495c4b03"
down_revision = "e35c6062270a"
branch_labels = None
depends_on = None

# backfill source_id field
UPDATE_QUERY = """
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
    connection.execute(UPDATE_QUERY)


def downgrade() -> None:
    return None
