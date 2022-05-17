# pylint: skip-file
"""drop_pa_vacantpositions

Revision ID: 30bcc98c3784
Revises: 71700d9efe9d
Create Date: 2022-05-17 13:00:04.987327

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "30bcc98c3784"
down_revision = "71700d9efe9d"
branch_labels = None
depends_on = None

UPDATE_STATE_SUPERVISION_PERIOD_QUERY = """
UPDATE state_supervision_period
SET supervising_officer_id = NULL
WHERE state_code = 'US_PA' AND supervising_officer_id IN (
    SELECT agent_id
    FROM state_agent
    WHERE full_name LIKE '%POSITION%' AND full_name LIKE '%VACANT%'
);
"""

UPDATE_STATE_SUPERVISION_CONTACT_QUERY = """
UPDATE state_supervision_contact
SET contacted_agent_id = NULL
WHERE state_code = 'US_PA' AND contacted_agent_id IN (
    SELECT agent_id
    FROM state_agent
    WHERE full_name LIKE '%POSITION%' AND full_name LIKE '%VACANT%'
);
"""

DROP_STATE_AGENT_QUERY = """
DELETE FROM state_agent
WHERE state_code = 'US_PA' AND full_name LIKE '%POSITION%' AND full_name LIKE '%VACANT%';
"""


def upgrade() -> None:
    op.execute(StrictStringFormatter().format(UPDATE_STATE_SUPERVISION_PERIOD_QUERY))
    op.execute(StrictStringFormatter().format(UPDATE_STATE_SUPERVISION_CONTACT_QUERY))
    op.execute(StrictStringFormatter().format(DROP_STATE_AGENT_QUERY))


def downgrade() -> None:
    # This migration does not have a downgrade
    pass
