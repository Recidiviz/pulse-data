# pylint: skip-file
"""add expenses and funding metric type

Revision ID: 2e47d6ab30d0
Revises: 919ba25c05af
Create Date: 2022-11-29 15:43:35.546962

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2e47d6ab30d0"
down_revision = "919ba25c05af"
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
