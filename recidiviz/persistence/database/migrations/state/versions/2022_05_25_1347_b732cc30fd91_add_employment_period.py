# pylint: skip-file
"""add_employment_period

Revision ID: b732cc30fd91
Revises: 8e4ac7a55d59
Create Date: 2022-05-25 13:47:08.639786

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b732cc30fd91"
down_revision = "8e4ac7a55d59"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "state_employment_period",
        sa.Column(
            "employment_period_id",
            sa.Integer(),
            nullable=False,
            comment="Unique identifier for a(n) employment period, generated automatically by the Recidiviz system. This identifier is not stable over time (it may change if historical data is re-ingested), but should be used within the context of a given dataset to connect this object to others.",
        ),
        sa.Column(
            "external_id",
            sa.String(length=255),
            nullable=True,
            comment="The unique identifier for the StateEmploymentPeriod, unique within the scope of the source data system.",
        ),
        sa.Column(
            "state_code",
            sa.String(length=255),
            nullable=False,
            comment="The U.S. state or region that provided the source data.",
        ),
        sa.Column(
            "employment_status",
            sa.Enum(
                "ALTERNATE_INCOME_SOURCE",
                "EMPLOYED",
                "EMPLOYED_FULL_TIME",
                "EMPLOYED_PART_TIME",
                "STUDENT",
                "UNABLE_TO_WORK",
                "UNEMPLOYED",
                "INTERNAL_UNKNOWN",
                "EXTERNAL_UNKNOWN",
                name="state_employment_period_employment_status",
            ),
            nullable=True,
            comment="Indicates the type of the person's employment or unemployment during the given period of time.",
        ),
        sa.Column(
            "employment_status_raw_text",
            sa.String(length=255),
            nullable=True,
            comment="The raw text value of the employment status.",
        ),
        sa.Column(
            "start_date",
            sa.Date(),
            nullable=False,
            comment="Date on which a person’s employment with the given employer and job title started.",
        ),
        sa.Column(
            "end_date",
            sa.Date(),
            nullable=True,
            comment="Date on which a person’s employment with the given employer and job title terminated.",
        ),
        sa.Column(
            "last_verified_date",
            sa.Date(),
            nullable=True,
            comment="Most recent date on which person’s employment with a given employer and job title was verified. Note that this field is only meaningful for open employment periods.",
        ),
        sa.Column(
            "employer_name",
            sa.String(length=255),
            nullable=True,
            comment="The name of the person's employer.",
        ),
        sa.Column(
            "job_title",
            sa.String(length=255),
            nullable=True,
            comment="The name of the person's job position.",
        ),
        sa.Column(
            "end_reason",
            sa.Enum(
                "EMPLOYMENT_STATUS_CHANGE",
                "FIRED",
                "INCARCERATED",
                "LAID_OFF",
                "MEDICAL",
                "MOVED",
                "NEW_JOB",
                "QUIT",
                "RETIRED",
                "INTERNAL_UNKNOWN",
                "EXTERNAL_UNKNOWN",
                name="state_employment_period_end_reason",
            ),
            nullable=True,
            comment="The reason why this period of employment or unemployment was terminated. Should only be set if the `end_date` is nonnull.",
        ),
        sa.Column(
            "end_reason_raw_text",
            sa.String(length=255),
            nullable=True,
            comment="The raw text value of the end reason.",
        ),
        sa.Column(
            "person_id",
            sa.Integer(),
            nullable=False,
            comment="Unique identifier for a(n) person, generated automatically by the Recidiviz system. This identifier is not stable over time (it may change if historical data is re-ingested), but should be used within the context of a given dataset to connect this object to relevant person information.",
        ),
        sa.ForeignKeyConstraint(
            ["person_id"],
            ["state_person.person_id"],
            initially="DEFERRED",
            deferrable=True,
        ),
        sa.PrimaryKeyConstraint("employment_period_id"),
        sa.UniqueConstraint(
            "state_code",
            "external_id",
            deferrable="True",
            initially="DEFERRED",
            name="employment_period_external_ids_unique_within_state",
        ),
        comment="The StateEmploymentPeriod object represents information about a person's employment status during a particular period of time. This object can be used to track employer information, or to track periods of unemployment if we have positive confirmation from the state that a person was unemployed at a given period.",
    )
    op.create_index(
        op.f("ix_state_employment_period_external_id"),
        "state_employment_period",
        ["external_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_state_employment_period_person_id"),
        "state_employment_period",
        ["person_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_state_employment_period_state_code"),
        "state_employment_period",
        ["state_code"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        op.f("ix_state_employment_period_state_code"),
        table_name="state_employment_period",
    )
    op.drop_index(
        op.f("ix_state_employment_period_person_id"),
        table_name="state_employment_period",
    )
    op.drop_index(
        op.f("ix_state_employment_period_external_id"),
        table_name="state_employment_period",
    )
    op.drop_table("state_employment_period")
    # ### end Alembic commands ###
    op.execute("DROP TYPE state_employment_period_end_reason;")
    op.execute("DROP TYPE state_employment_period_employment_status;")
