# pylint: skip-file
"""add_supervision_type_external_unknown

Revision ID: 55e0dc20d315
Revises: fbf7e6528526
Create Date: 2019-12-02 14:28:41.078363

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "55e0dc20d315"
down_revision = "fbf7e6528526"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CIVIL_COMMITMENT",
    "HALFWAY_HOUSE",
    "PAROLE",
    "POST_CONFINEMENT",
    "PRE_CONFINEMENT",
    "PROBATION",
]

# With new value
new_values = [
    "CIVIL_COMMITMENT",
    "EXTERNAL_UNKNOWN",
    "HALFWAY_HOUSE",
    "PAROLE",
    "POST_CONFINEMENT",
    "PRE_CONFINEMENT",
    "PROBATION",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_type RENAME TO state_supervision_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_type").create(bind=op.get_bind())

    op.alter_column(
        "state_supervision_sentence",
        column_name="supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_type",
    )
    op.alter_column(
        "state_supervision_sentence_history",
        column_name="supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_type",
    )

    op.alter_column(
        "state_supervision_period",
        column_name="supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_type",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_type",
    )

    op.execute("DROP TYPE state_supervision_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_type RENAME TO state_supervision_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_type").create(bind=op.get_bind())

    op.alter_column(
        "state_supervision_period",
        column_name="supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_type",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_type",
    )

    op.alter_column(
        "state_supervision_sentence",
        column_name="supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_type",
    )
    op.alter_column(
        "state_supervision_sentence_history",
        column_name="supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_type",
    )

    op.execute("DROP TYPE state_supervision_type_old;")
