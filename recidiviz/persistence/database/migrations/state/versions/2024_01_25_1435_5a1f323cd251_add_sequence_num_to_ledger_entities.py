# pylint: skip-file
"""add_sequence_num_to_ledger_entities

Revision ID: 5a1f323cd251
Revises: f095f05c46af
Create Date: 2024-01-25 14:35:42.518465

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5a1f323cd251"
down_revision = "f095f05c46af"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "state_sentence_group",
        sa.Column(
            "sequence_num",
            sa.Integer(),
            nullable=True,
            comment="The ordinal position of this observation in the sequence of StateSentenceGroup observations for this observation's StateSentence",
        ),
    )
    op.add_column(
        "state_sentence_length",
        sa.Column(
            "sequence_num",
            sa.Integer(),
            nullable=True,
            comment="The ordinal position of this observation in the sequence of StateSentenceLength observations for this observation's StateSentence",
        ),
    )
    op.add_column(
        "state_sentence_status_snapshot",
        sa.Column(
            "sequence_num",
            sa.Integer(),
            nullable=True,
            comment="The ordinal position of this observation in the sequence of StateSentenceStatusSnapshot observations for this observation's StateSentence",
        ),
    )
    op.add_column(
        "state_task_deadline",
        sa.Column(
            "sequence_num",
            sa.Integer(),
            nullable=True,
            comment="The ordinal position of this observation in the sequence of StateTaskDeadline observations for this observation's StatePerson",
        ),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("state_task_deadline", "sequence_num")
    op.drop_column("state_sentence_status_snapshot", "sequence_num")
    op.drop_column("state_sentence_length", "sequence_num")
    op.drop_column("state_sentence_group", "sequence_num")
    # ### end Alembic commands ###