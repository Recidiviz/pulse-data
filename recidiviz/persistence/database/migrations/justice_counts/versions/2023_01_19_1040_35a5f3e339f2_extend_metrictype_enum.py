# pylint: skip-file
"""extend metrictype enum

Revision ID: 35a5f3e339f2
Revises: 9874fafea7fd
Create Date: 2023-01-19 10:40:34.895328

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "35a5f3e339f2"
down_revision = "9874fafea7fd"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ADMISSIONS",
    "ARRESTS",
    "ARRESTS_ON_PRETRIAL_RELEASE",
    "BUDGET",
    "CALLS_FOR_SERVICE",
    "CASELOADS",
    "CASES_APPOINTED_COUNSEL",
    "CASES_DECLINED",
    "CASES_DEFERRED",
    "CASES_DISPOSED",
    "CASES_FILED",
    "CASES_OVERTURNED_ON_APPEAL",
    "CASES_PROSECUTED",
    "CASES_REFERRED",
    "COMPLAINTS_SUSTAINED",
    "EXPENSES",
    "FUNDING",
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

# With new value
new_values = [
    "ADMISSIONS",
    "ARRESTS",
    "ARRESTS_ON_PRETRIAL_RELEASE",
    "BUDGET",
    "CALLS_FOR_SERVICE",
    "CASELOADS",
    "CASELOADS_PEOPLE",
    "CASELOADS_STAFF",
    "CASES_APPOINTED_COUNSEL",
    "CASES_DECLINED",
    "CASES_DEFERRED",
    "CASES_DISPOSED",
    "CASES_FILED",
    "CASES_OVERTURNED_ON_APPEAL",
    "CASES_PROSECUTED",
    "CASES_REFERRED",
    "COMPLAINTS_SUSTAINED",
    "EXPENSES",
    "FUNDING",
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


def upgrade() -> None:
    op.execute("ALTER TYPE metrictype RENAME TO metrictype_old;")
    sa.Enum(*new_values, name="metrictype").create(bind=op.get_bind())
    op.alter_column(
        "report_table_definition",
        column_name="metric_type",
        type_=sa.Enum(*new_values, name="metrictype"),
        postgresql_using="metric_type::text::metrictype",
    )
    op.execute("DROP TYPE metrictype_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE metrictype RENAME TO metrictype_old;")
    sa.Enum(*old_values, name="metrictype").create(bind=op.get_bind())
    op.alter_column(
        "report_table_definition",
        column_name="metric_type",
        type_=sa.Enum(*old_values, name="metrictype"),
        postgresql_using="metric_type::text::metrictype",
    )
    op.execute("DROP TYPE metrictype_old;")
