# pylint: skip-file
"""delete superagency config datapoints

Revision ID: 64c0067d864e
Revises: 6909495c4b03
Create Date: 2023-11-20 16:42:46.883460

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "64c0067d864e"
down_revision = "6909495c4b03"
branch_labels = None
depends_on = None

DELETE_QUERY = """
DELETE
FROM datapoint
WHERE datapoint.id IN
(
    SELECT datapoint.id
    FROM datapoint
    JOIN source on datapoint.source_id = source.id
    WHERE source.super_agency_id is not null
    AND datapoint.metric_definition_key in ('SUPERAGENCY_EXPENSES', 'SUPERAGENCY_TOTAL_STAFF', 'SUPERAGENCY_FUNDING')
);
"""


def upgrade() -> None:
    connection = op.get_bind()
    connection.execute(DELETE_QUERY)


def downgrade() -> None:
    return None
