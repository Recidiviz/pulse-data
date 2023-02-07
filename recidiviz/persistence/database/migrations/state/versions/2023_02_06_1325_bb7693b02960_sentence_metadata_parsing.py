# pylint: skip-file
"""sentence_metadata_parsing

Revision ID: bb7693b02960
Revises: 3cf8e0ed8c44
Create Date: 2023-02-06 13:25:43.189846

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from recidiviz.utils.string import StrictStringFormatter

revision = "bb7693b02960"
down_revision = "cc34e32d49e0"
branch_labels = None
depends_on = None

UPDATE_QUERY_TEMPLATE = """UPDATE {table_name}
SET sentence_metadata = REPLACE(sentence_metadata, '"CONSECUTIVE_SENTENCE_ID": NULL,', '"CONSECUTIVE_SENTENCE_ID": "",')
WHERE sentence_metadata LIKE '%"CONSECUTIVE_SENTENCE_ID": NULL,%';
"""


def upgrade() -> None:
    for table_name in ["state_incarceration_sentence", "state_supervision_sentence"]:
        op.execute(
            StrictStringFormatter().format(UPDATE_QUERY_TEMPLATE, table_name=table_name)
        )


def downgrade() -> None:
    # Intentionally do nothing here - we do not want to revert metadata to its
    # non-parsing form.
    pass
