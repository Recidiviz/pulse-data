# pylint: skip-file
"""drop-unused-sup-sentence-types

Revision ID: 0bf70c925d11
Revises: 31334aefcd4a
Create Date: 2022-05-06 17:55:56.941097

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0bf70c925d11"
down_revision = "31334aefcd4a"
branch_labels = None
depends_on = None

# With CIVIL_COMMITMENT, HALFWAY_HOUSE, POST_CONFINEMENT, PRE_CONFINEMENT
old_values = [
    "CIVIL_COMMITMENT",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "HALFWAY_HOUSE",
    "PAROLE",
    "POST_CONFINEMENT",
    "PRE_CONFINEMENT",
    "PROBATION",
    "COMMUNITY_CORRECTIONS",
]

# Without deprecated values
new_values = [
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "PAROLE",
    "PROBATION",
    "COMMUNITY_CORRECTIONS",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_sentence_supervision_type RENAME TO state_supervision_sentence_supervision_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_sentence_supervision_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_sentence",
        column_name="supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_sentence_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_sentence_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_sentence_supervision_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_sentence_supervision_type RENAME TO state_supervision_sentence_supervision_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_sentence_supervision_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_sentence",
        column_name="supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_sentence_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_sentence_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_sentence_supervision_type_old;")
