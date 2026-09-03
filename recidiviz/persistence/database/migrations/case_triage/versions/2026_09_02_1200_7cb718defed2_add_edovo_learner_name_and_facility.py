# pylint: skip-file
"""add learner name and facility to edovo_course_completions

Edovo added first_name, last_name and facility to the course-completion payload
so we can verify the person_external_id match and tell which system issued the
id. The columns are NOT NULL because every request now carries all three.

Rows captured before this migration have no name or facility, so the columns are
added with an empty-string default to satisfy NOT NULL; the default is then
dropped so no later insert can omit a value. Only staging holds such rows — the
endpoint is not live in production.

Revision ID: 7cb718defed2
Revises: c4d5e6f7a8b9
Create Date: 2026-09-02 12:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7cb718defed2"
down_revision = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None

_TABLE_NAME = "edovo_course_completions"
_NEW_COLUMNS = ["first_name", "last_name", "facility"]


def upgrade() -> None:
    for column_name in _NEW_COLUMNS:
        op.add_column(
            _TABLE_NAME,
            sa.Column(
                column_name,
                sa.String(length=255),
                server_default="",
                nullable=False,
            ),
        )
        op.alter_column(_TABLE_NAME, column_name, server_default=None)


def downgrade() -> None:
    for column_name in _NEW_COLUMNS:
        op.drop_column(_TABLE_NAME, column_name)
