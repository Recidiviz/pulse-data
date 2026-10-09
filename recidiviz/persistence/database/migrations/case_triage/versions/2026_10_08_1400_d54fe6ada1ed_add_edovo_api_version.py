# pylint: skip-file
"""add api_version to edovo_course_completions

Edovo now sends an integer api_version with every course-completion request.
The column is nullable because completions received before Edovo added the
field have no version, and inventing one would misstate what Edovo sent.

Revision ID: d54fe6ada1ed
Revises: 7cb718defed2
Create Date: 2026-10-08 14:00:00.000000

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d54fe6ada1ed"
down_revision = "7cb718defed2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "edovo_course_completions",
        sa.Column("api_version", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("edovo_course_completions", "api_version")
