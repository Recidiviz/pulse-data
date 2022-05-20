# pylint: skip-file
"""extend metric and system enums

Revision ID: b91a05e9c518
Revises: 0111aac99da0
Create Date: 2022-05-19 10:19:04.479200

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b91a05e9c518"
down_revision = "0111aac99da0"
branch_labels = None
depends_on = None

old_metric_type_values = [
    "ADMISSIONS",
    "ARRESTS",
    "POPULATION",
    "REVOCATIONS",
    "RELEASES",
    "SUPERVISION_STARTS",
    "BUDGET",
    "CALLS_FOR_SERVICE",
    "COMPLAINTS_SUSTAINED",
    "REPORTED_CRIME",
    "TOTAL_STAFF",
    "USE_OF_FORCE_INCIDENTS",
]

new_metric_type_values = [
    "ADMISSIONS",
    "ARRESTS",
    "ARRESTS_ON_PRETRIAL_RELEASE",
    "BUDGET",
    "CALLS_FOR_SERVICE",
    "CASES_APPOINTED_COUNSEL",
    "CASES_FILED",
    "CASES_DECLINED",
    "CASES_DISPOSED",
    "CASES_DEFERRED",
    "CASES_REFERRED",
    "CASES_PROSECUTED",
    "CASELOADS",
    "CASES_OVERTURNED_ON_APPEAL",
    "COMPLAINTS_SUSTAINED",
    "GRIEVANCES_UPHELD",
    "POPULATION",
    "PRETRIAL_RELEASES",
    "READMISSIONS",
    "RECONVICTIONS",
    "RELEASES",
    "REPORTED_CRIME",
    "RESIDENTS",
    "REVOCATIONS",
    "SENTENCES",
    "SUPERVISION_STARTS",
    "SUPERVISION_TERMINATIONS",
    "SUPERVISION_VIOLATIONS",
    "TOTAL_STAFF",
    "USE_OF_FORCE_INCIDENTS",
    "VIOLATIONS_WITH_DISCIPLINARY_ACTION",
]

old_system_values = [
    "LAW_ENFORCEMENT",
    "COURT_PROCESSES",
    "CORRECTIONS",
]

new_system_values = [
    "DEFENSE",
    "JAILS",
    "LAW_ENFORCEMENT",
    "COURT_PROCESSES",
    "CORRECTIONS",
    "PRISONS",
    "PROSECUTION",
    "SUPERVISION",
]


def upgrade() -> None:
    op.execute("ALTER TYPE metrictype RENAME TO metrictype_old;")
    sa.Enum(*new_metric_type_values, name="metrictype").create(bind=op.get_bind())
    op.alter_column(
        "report_table_definition",
        column_name="metric_type",
        type_=sa.Enum(*new_metric_type_values, name="metrictype"),
        postgresql_using="metric_type::text::metrictype",
    )
    op.execute("DROP TYPE metrictype_old;")

    op.execute("ALTER TYPE system RENAME TO system_old;")
    sa.Enum(*new_system_values, name="system").create(bind=op.get_bind())
    op.alter_column(
        "report_table_definition",
        column_name="system",
        type_=sa.Enum(*new_system_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.alter_column(
        "source",
        column_name="system",
        type_=sa.Enum(*new_system_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.execute("DROP TYPE system_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE metrictype RENAME TO metrictype_old;")
    sa.Enum(*old_metric_type_values, name="metrictype").create(bind=op.get_bind())
    op.alter_column(
        "report_table_definition",
        column_name="metric_type",
        type_=sa.Enum(*old_metric_type_values, name="metrictype"),
        postgresql_using="metric_type::text::metrictype",
    )
    op.execute("DROP TYPE metrictype_old;")

    op.execute("ALTER TYPE system RENAME TO system_old;")
    sa.Enum(*old_system_values, name="system").create(bind=op.get_bind())
    op.alter_column(
        "report_table_definition",
        column_name="system",
        type_=sa.Enum(*old_system_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.alter_column(
        "source",
        column_name="system",
        type_=sa.Enum(*old_system_values, name="system"),
        postgresql_using="system::text::system",
    )
    op.execute("DROP TYPE system_old;")
