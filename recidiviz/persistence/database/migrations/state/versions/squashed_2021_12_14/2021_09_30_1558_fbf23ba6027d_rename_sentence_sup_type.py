# pylint: skip-file
"""rename_sentence_sup_type

Revision ID: fbf23ba6027d
Revises: d759f439bae3
Create Date: 2021-09-30 15:58:00.437174

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "fbf23ba6027d"
down_revision = "d759f439bae3"
branch_labels = None
depends_on = None

ENUM_VALUES = [
    "CIVIL_COMMITMENT",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
    "HALFWAY_HOUSE",
    "PAROLE",
    "POST_CONFINEMENT",
    "PRE_CONFINEMENT",
    "PROBATION",
]


def upgrade() -> None:
    sa.Enum(
        *ENUM_VALUES,
        name="state_supervision_sentence_supervision_type",
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_sentence",
        "supervision_type",
        existing_type=postgresql.ENUM(
            *ENUM_VALUES,
            name="state_supervision_type",
        ),
        type_=sa.Enum(
            *ENUM_VALUES,
            name="state_supervision_sentence_supervision_type",
        ),
        existing_comment="The type of supervision the person is being sentenced to.",
        existing_nullable=True,
        postgresql_using="supervision_type::text::state_supervision_sentence_supervision_type",
    )
    op.alter_column(
        "state_supervision_sentence_history",
        "supervision_type",
        existing_type=postgresql.ENUM(
            *ENUM_VALUES,
            name="state_supervision_type",
        ),
        type_=sa.Enum(
            *ENUM_VALUES,
            name="state_supervision_sentence_supervision_type",
        ),
        existing_comment="The type of supervision the person is being sentenced to.",
        existing_nullable=True,
        postgresql_using="supervision_type::text::state_supervision_sentence_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_type;")


def downgrade() -> None:
    sa.Enum(
        *ENUM_VALUES,
        name="state_supervision_type",
    ).create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_sentence_history",
        "supervision_type",
        existing_type=sa.Enum(
            *ENUM_VALUES,
            name="state_supervision_sentence_supervision_type",
        ),
        type_=postgresql.ENUM(
            *ENUM_VALUES,
            name="state_supervision_type",
        ),
        existing_comment="The type of supervision the person is being sentenced to.",
        existing_nullable=True,
        postgresql_using="supervision_type::text::state_supervision_type",
    )
    op.alter_column(
        "state_supervision_sentence",
        "supervision_type",
        existing_type=sa.Enum(
            *ENUM_VALUES,
            name="state_supervision_sentence_supervision_type",
        ),
        type_=postgresql.ENUM(
            *ENUM_VALUES,
            name="state_supervision_type",
        ),
        existing_comment="The type of supervision the person is being sentenced to.",
        existing_nullable=True,
        postgresql_using="supervision_type::text::state_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_sentence_supervision_type;")
