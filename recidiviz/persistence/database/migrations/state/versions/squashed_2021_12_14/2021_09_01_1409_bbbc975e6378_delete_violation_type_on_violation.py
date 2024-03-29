# pylint: skip-file
"""delete_violation_type_on_violation

Revision ID: bbbc975e6378
Revises: 335d6697e0b9
Create Date: 2021-09-01 14:09:10.336461

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "bbbc975e6378"
down_revision = "335d6697e0b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("state_supervision_violation", "violation_type_raw_text")
    op.drop_column("state_supervision_violation", "violation_type")
    op.drop_column("state_supervision_violation_history", "violation_type_raw_text")
    op.drop_column("state_supervision_violation_history", "violation_type")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "state_supervision_violation_history",
        sa.Column(
            "violation_type",
            postgresql.ENUM(
                "ABSCONDED",
                "ESCAPED",
                "FELONY",
                "LAW",
                "MISDEMEANOR",
                "MUNICIPAL",
                "TECHNICAL",
                name="state_supervision_violation_type",
            ),
            autoincrement=False,
            nullable=True,
            comment="The type of violation.",
        ),
    )
    op.add_column(
        "state_supervision_violation_history",
        sa.Column(
            "violation_type_raw_text",
            sa.VARCHAR(length=255),
            autoincrement=False,
            nullable=True,
            comment="The raw text value of the violation type.",
        ),
    )
    op.add_column(
        "state_supervision_violation",
        sa.Column(
            "violation_type",
            postgresql.ENUM(
                "ABSCONDED",
                "ESCAPED",
                "FELONY",
                "LAW",
                "MISDEMEANOR",
                "MUNICIPAL",
                "TECHNICAL",
                name="state_supervision_violation_type",
            ),
            autoincrement=False,
            nullable=True,
            comment="The type of violation.",
        ),
    )
    op.add_column(
        "state_supervision_violation",
        sa.Column(
            "violation_type_raw_text",
            sa.VARCHAR(length=255),
            autoincrement=False,
            nullable=True,
            comment="The raw text value of the violation type.",
        ),
    )
    # ### end Alembic commands ###
