# pylint: skip-file
"""add_supervising_officer_to_person

Revision ID: 4616432309a9
Revises: 56a1941a76b7
Create Date: 2019-10-18 20:08:16.171623

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4616432309a9"
down_revision = "56a1941a76b7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "state_person", sa.Column("supervising_officer_id", sa.Integer(), nullable=True)
    )
    op.create_foreign_key(
        "state_person_supervising_officer_id_fkey",
        "state_person",
        "state_agent",
        ["supervising_officer_id"],
        ["agent_id"],
    )
    op.add_column(
        "state_person_history",
        sa.Column("supervising_officer_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "state_person_history_supervising_officer_id_fkey",
        "state_person_history",
        "state_agent",
        ["supervising_officer_id"],
        ["agent_id"],
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        "state_person_history_supervising_officer_id_fkey",
        "state_person_history",
        type_="foreignkey",
    )
    op.drop_column("state_person_history", "supervising_officer_id")
    op.drop_constraint(
        "state_person_supervising_officer_id_fkey", "state_person", type_="foreignkey"
    )
    op.drop_column("state_person", "supervising_officer_id")
    # ### end Alembic commands ###
