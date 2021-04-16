# pylint: skip-file
"""migrate_case_update

Revision ID: d956f7fb3382
Revises: f36aca3eadef
Create Date: 2021-04-16 10:20:36.333370

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d956f7fb3382"
down_revision = "f36aca3eadef"
branch_labels = None
depends_on = None


INSERT_CONTENT_QUERY = """
INSERT INTO case_update_actions (person_external_id, officer_external_id, state_code, action_type, action_ts, last_version, comment)
SELECT
    person_external_id,
    officer_external_id,
    state_code,
    elements.value ->> 'action' AS action_type,
    TO_TIMESTAMP(elements.value ->> 'action_ts', 'YYYY-MM-DD HH24:MI:SS.US') AS action_ts,
    CASE
        WHEN elements.value ->> 'action' IN ('COMPLETED_ASSESSMENT', 'SCHEDULED_FACE_TO_FACE')
            THEN jsonb_build_object('last_recorded_date', elements.value -> 'last_recorded_date')
        WHEN elements.value ->> 'action' = 'DOWNGRADE_INITIATED'
            THEN jsonb_build_object('last_supervision_level', elements.value -> 'last_supervision_level')
        ELSE
            jsonb_build_object()
    END AS last_version,
    update_metadata ->> 'otherText' AS comment
FROM
    case_updates,
    jsonb_array_elements(update_metadata -> 'actions') AS elements
"""


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto"')
    op.execute(
        "ALTER TABLE case_update_actions ALTER COLUMN update_id SET DEFAULT gen_random_uuid()"
    )
    op.execute(INSERT_CONTENT_QUERY)


def downgrade() -> None:
    # Note: we are not un-creating the pgcrypto extension, nor are we winding back the insert query
    op.execute("ALTER TABLE case_update_actions ALTER COLUMN update_id DROP DEFAULT")
