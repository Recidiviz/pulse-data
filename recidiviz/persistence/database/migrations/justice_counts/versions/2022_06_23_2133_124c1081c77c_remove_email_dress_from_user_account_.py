# pylint: skip-file
"""remove email dress from user account table

Revision ID: 124c1081c77c
Revises: 0e525a53c526
Create Date: 2022-06-23 21:33:54.998212

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "124c1081c77c"
down_revision = "0e525a53c526"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint("unique_email_address", "user_account", type_="unique")
    op.drop_constraint("user_account_auth0_user_id_key", "user_account", type_="unique")
    op.create_unique_constraint(
        "unique_auth0_user_id", "user_account", ["auth0_user_id"]
    )
    op.drop_column("user_account", "email_address")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "user_account",
        sa.Column(
            "email_address", sa.VARCHAR(length=255), autoincrement=False, nullable=False
        ),
    )
    op.drop_constraint("unique_auth0_user_id", "user_account", type_="unique")
    op.create_unique_constraint(
        "user_account_auth0_user_id_key", "user_account", ["auth0_user_id"]
    )
    op.create_unique_constraint(
        "unique_email_address", "user_account", ["email_address"]
    )
    # ### end Alembic commands ###
