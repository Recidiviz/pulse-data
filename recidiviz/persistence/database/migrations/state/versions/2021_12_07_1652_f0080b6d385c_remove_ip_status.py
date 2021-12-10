# pylint: skip-file
"""remove_ip_status

Revision ID: f0080b6d385c
Revises: 541d210d78cf
Create Date: 2021-12-07 16:52:26.396828

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f0080b6d385c"
down_revision = "541d210d78cf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("state_incarceration_period", "status")
    op.drop_column("state_incarceration_period", "status_raw_text")
    op.drop_column("state_incarceration_period_history", "status")
    op.drop_column("state_incarceration_period_history", "status_raw_text")

    # Drop the enum stored in this field
    op.execute("DROP TYPE state_incarceration_period_status;")


def downgrade() -> None:
    sa.Enum(
        "EXTERNAL_UNKNOWN",
        "IN_CUSTODY",
        "NOT_IN_CUSTODY",
        "PRESENT_WITHOUT_INFO",
        name="state_incarceration_period_status",
    ).create(bind=op.get_bind())

    op.add_column(
        "state_incarceration_period_history",
        sa.Column(
            "status_raw_text",
            sa.VARCHAR(length=255),
            autoincrement=False,
            nullable=True,
            comment="The raw text value of the incarceration period status.",
        ),
    )
    op.add_column(
        "state_incarceration_period_history",
        sa.Column(
            "status",
            postgresql.ENUM(
                "EXTERNAL_UNKNOWN",
                "IN_CUSTODY",
                "NOT_IN_CUSTODY",
                "PRESENT_WITHOUT_INFO",
                name="state_incarceration_period_status",
            ),
            autoincrement=False,
            nullable=False,
            comment="The current status of this incarceration period.",
        ),
    )
    op.add_column(
        "state_incarceration_period",
        sa.Column(
            "status_raw_text",
            sa.VARCHAR(length=255),
            autoincrement=False,
            nullable=True,
            comment="The raw text value of the incarceration period status.",
        ),
    )
    op.add_column(
        "state_incarceration_period",
        sa.Column(
            "status",
            postgresql.ENUM(
                "EXTERNAL_UNKNOWN",
                "IN_CUSTODY",
                "NOT_IN_CUSTODY",
                "PRESENT_WITHOUT_INFO",
                name="state_incarceration_period_status",
            ),
            autoincrement=False,
            nullable=False,
            comment="The current status of this incarceration period.",
        ),
    )
