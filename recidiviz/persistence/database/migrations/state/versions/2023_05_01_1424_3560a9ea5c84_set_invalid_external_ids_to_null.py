# pylint: skip-file
"""set_invalid_external_ids_to_null

Revision ID: 3560a9ea5c84
Revises: cfbd8b0f96c2
Create Date: 2023-05-01 14:24:59.743057

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3560a9ea5c84"
down_revision = "cfbd8b0f96c2"
branch_labels = None
depends_on = None

UPDATE_QUERY = """
WITH joined AS (
    SELECT
        p.supervision_period_id
    FROM state_supervision_period p
    LEFT OUTER JOIN
        state_staff_external_id eid
    ON
      p.state_code = eid.state_code
      AND p.supervising_officer_staff_external_id = eid.external_id
      AND p.supervising_officer_staff_external_id_type = eid.id_type
    WHERE 
      p.state_code = 'US_ND' 
      AND p.supervising_officer_staff_external_id IS NOT NULL AND eid.external_id IS NULL
)
UPDATE state_supervision_period
SET
    supervising_officer_staff_external_id = NULL,
    supervising_officer_staff_external_id_type = NULL
FROM joined
WHERE
    state_supervision_period.supervision_period_id = joined.supervision_period_id;
"""


def upgrade() -> None:
    op.execute(UPDATE_QUERY)


def downgrade() -> None:
    pass
