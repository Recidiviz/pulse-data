# pylint: skip-file
"""add_state_staff_caseload_type_period_table_and_enum

Revision ID: da61d6fdb049
Revises: 8c5fadfa3242
Create Date: 2023-05-12 11:20:23.142234

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "da61d6fdb049"
down_revision = "8c5fadfa3242"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "state_staff_caseload_type_period",
        sa.Column(
            "staff_caseload_type_period_id",
            sa.Integer(),
            nullable=False,
            comment="Unique identifier for a(n) staff member caseload type period, generated automatically by the Recidiviz system. This identifier is not stable over time (it may change if historical data is re-ingested), but should be used within the context of a given dataset to connect this object to others.",
        ),
        sa.Column(
            "external_id",
            sa.String(length=255),
            nullable=False,
            comment="The unique identifier for the StateStaffLocationPeriod, unique within the scope of the source data system.",
        ),
        sa.Column(
            "state_code",
            sa.String(length=255),
            nullable=False,
            comment="The U.S. state or region that provided the source data.",
        ),
        sa.Column(
            "state_staff_specialized_caseload_type",
            sa.Enum(
                "SEX_OFFENSE",
                "ADMINISTRATIVE_SUPERVISION",
                "ALCOHOL_AND_DRUG",
                "INTENSIVE",
                "MENTAL_HEALTH",
                "ELECTRONIC_MONITORING",
                "OTHER_COURT",
                "DRUG_COURT",
                "VETERANS_COURT",
                "COMMUNITY_FACILITY",
                "OTHER",
                "INTERNAL_UNKNOWN",
                "EXTERNAL_UNKNOWN",
                name="state_staff_specialized_caseload_type",
            ),
            nullable=False,
            comment="Indicates the specialized type of the caseload an officer supervises",
        ),
        sa.Column(
            "state_staff_specialized_caseload_type_raw_text",
            sa.String(length=255),
            nullable=True,
            comment="Raw text for the specialized caseload type field.",
        ),
        sa.Column(
            "start_date",
            sa.Date(),
            nullable=False,
            comment="The beginning of the period where this officer had this type of specialized caseload.",
        ),
        sa.Column(
            "end_date",
            sa.Date(),
            nullable=True,
            comment="The end of the period where this officer had this type of specialized caseload.",
        ),
        sa.Column(
            "staff_id",
            sa.Integer(),
            nullable=False,
            comment="Unique identifier for a(n) staff member, generated automatically by the Recidiviz system. This identifier is not stable over time (it may change if historical data is re-ingested), but should be used within the context of a given dataset to connect this object to relevant staff member information.",
        ),
        sa.ForeignKeyConstraint(
            ["staff_id"],
            ["state_staff.staff_id"],
            initially="DEFERRED",
            deferrable=True,
        ),
        sa.PrimaryKeyConstraint("staff_caseload_type_period_id"),
        comment="This table will have one row for each period in which one officer had a particular type of specialized caseload. If the nature of their specialization changes over time, they will have more than one period reflecting the dates of those changes and what specialization corresponded to each period of their employment. Eventually, correctional officers who work in facilities and have specialized caseloads will also be included in this table.",
    )
    op.create_index(
        op.f("ix_state_staff_caseload_type_period_external_id"),
        "state_staff_caseload_type_period",
        ["external_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_state_staff_caseload_type_period_staff_id"),
        "state_staff_caseload_type_period",
        ["staff_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_state_staff_caseload_type_period_state_code"),
        "state_staff_caseload_type_period",
        ["state_code"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        op.f("ix_state_staff_caseload_type_period_state_code"),
        table_name="state_staff_caseload_type_period",
    )
    op.drop_index(
        op.f("ix_state_staff_caseload_type_period_staff_id"),
        table_name="state_staff_caseload_type_period",
    )
    op.drop_index(
        op.f("ix_state_staff_caseload_type_period_external_id"),
        table_name="state_staff_caseload_type_period",
    )
    op.drop_table("state_staff_caseload_type_period")

    op.execute("DROP TYPE state_staff_specialized_caseload_type;")
    # ### end Alembic commands ###
