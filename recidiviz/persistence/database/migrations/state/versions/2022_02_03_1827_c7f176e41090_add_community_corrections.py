# pylint: skip-file
"""add_community_corrections

Revision ID: c7f176e41090
Revises: a19f42bb34c4
Create Date: 2022-02-03 18:27:14.100601

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c7f176e41090"
down_revision = "a19f42bb34c4"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CIVIL_COMMITMENT",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "HALFWAY_HOUSE",
    "PAROLE",
    "POST_CONFINEMENT",
    "PRE_CONFINEMENT",
    "PROBATION",
]

# With new value
new_values = [
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
    op.alter_column(
        "state_supervision_sentence_history",
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
    op.alter_column(
        "state_supervision_sentence_history",
        column_name="supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_sentence_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_sentence_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_sentence_supervision_type_old;")
