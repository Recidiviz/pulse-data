# pylint: skip-file
"""adding user account and agency tables

Revision ID: 71fb90aaa368
Revises: 7a12c8a72f3c
Create Date: 2022-03-23 15:41:31.270758

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "71fb90aaa368"
down_revision = "7a12c8a72f3c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "user_account",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("auth0_user_id", sa.String(length=255), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("auth0_user_id"),
    )
    op.create_table(
        "agency_user_account_association",
        sa.Column("agency_id", sa.Integer(), nullable=False),
        sa.Column("user_account_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["agency_id"],
            ["source.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_account_id"],
            ["user_account.id"],
        ),
        sa.PrimaryKeyConstraint("agency_id", "user_account_id"),
    )
    op.add_column("source", sa.Column("type", sa.String(length=255), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("source", "type")
    op.drop_table("agency_user_account_association")
    op.drop_table("user_account")
    # ### end Alembic commands ###
