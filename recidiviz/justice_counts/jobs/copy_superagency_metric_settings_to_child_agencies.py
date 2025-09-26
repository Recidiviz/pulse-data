# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

"""
Cloud Run Job script that copies all metric settings from a super agency to its
child agencies, and sends the requesting user an email confirmation upon the success
of the job.

If child agencies have existing settings that conflict with the super agency,
they will be overwritten. If the child agency has existing settings that do not
conflict with the super agency, then they will not be affected.

The following are a list of metric definition keys grouped by system.
These can be passed into the metric_definition_key_subset flag to
update a subset of metrics.

COURTS:
- COURTS_AND_PRETRIAL_FUNDING
- COURTS_AND_PRETRIAL_EXPENSES
- COURTS_AND_PRETRIAL_TOTAL_STAFF
- COURTS_AND_PRETRIAL_PRETRIAL_RELEASES
- COURTS_AND_PRETRIAL_CASES_FILED
- COURTS_AND_PRETRIAL_CASES_DISPOSED
- COURTS_AND_PRETRIAL_SENTENCES
- COURTS_AND_PRETRIAL_ARRESTS_ON_PRETRIAL_RELEASE

DEFENSE:
- DEFENSE_FUNDING
- DEFENSE_EXPENSES
- DEFENSE_TOTAL_STAFF
- DEFENSE_CASELOADS_PEOPLE
- DEFENSE_CASELOADS_STAFF
- DEFENSE_CASES_APPOINTED_COUNSEL
- DEFENSE_CASES_DISPOSED
- DEFENSE_COMPLAINTS_SUSTAINED

JAILS:
- JAILS_FUNDING
- JAILS_EXPENSES
- JAILS_TOTAL_STAFF
- JAILS_PRE_ADJUDICATION_ADMISSIONS
- JAILS_POST_ADJUDICATION_ADMISSIONS
- JAILS_PRE_ADJUDICATION_POPULATION
- JAILS_POST_ADJUDICATION_POPULATION
- JAILS_PRE_ADJUDICATION_RELEASES
- JAILS_POST_ADJUDICATION_RELEASES
- JAILS_READMISSIONS
- JAILS_USE_OF_FORCE_INCIDENTS
- JAILS_GRIEVANCES_UPHELD

LAW_ENFORCEMENT:
- LAW_ENFORCEMENT_FUNDING
- LAW_ENFORCEMENT_EXPENSES
- LAW_ENFORCEMENT_TOTAL_STAFF
- LAW_ENFORCEMENT_CALLS_FOR_SERVICE
- LAW_ENFORCEMENT_ARRESTS
- LAW_ENFORCEMENT_REPORTED_CRIME
- LAW_ENFORCEMENT_USE_OF_FORCE_INCIDENTS
- LAW_ENFORCEMENT_COMPLAINTS_SUSTAINED

PRISONS:
- PRISONS_FUNDING
- PRISONS_EXPENSES
- PRISONS_TOTAL_STAFF
- PRISONS_ADMISSIONS
- PRISONS_POPULATION
- PRISONS_RELEASES
- PRISONS_READMISSIONS
- PRISONS_USE_OF_FORCE_INCIDENTS
- PRISONS_GRIEVANCES_UPHELD

PROSECUTION:
- PROSECUTION_FUNDING
- PROSECUTION_EXPENSES
- PROSECUTION_TOTAL_STAFF
- PROSECUTION_CASELOADS_PEOPLE
- PROSECUTION_CASELOADS_STAFF
- PROSECUTION_CASES REFERRED
- PROSECUTION_CASES_DECLINED
- PROSECUTION_CASES_DIVERTED
- PROSECUTION_CASES_PROSECUTED
- PROSECUTION_CASES_DISPOSED
- PROSECUTION_VIOLATIONS_WITH_DISCIPLINARY_ACTION

SUPERVISION:
- SUPERVISION_FUNDING
- SUPERVISION_EXPENSES
- SUPERVISION_TOTAL_STAFF
- SUPERVISION_CASELOADS_PEOPLE
- SUPERVISION_CASELOADS_STAFF
- SUPERVISION_SUPERVISION_STARTS
- SUPERVISION_POPULATION
- SUPERVISION_SUPERVISION_TERMINATIONS
- SUPERVISION_SUPERVISION_VIOLATIONS
- SUPERVISION_REVOCATIONS
- SUPERVISION_RECONVICTIONS

Note: If you need to copy over metrics for a supervision subsystem, replace the SUPERVISION
in the metric name with the name of the subsystem (i.e SUPERVISION_FUNDING -> PAROLE_FUNDING).
"""

import logging

import sentry_sdk
from sqlalchemy.orm import Session

from recidiviz.justice_counts.utils.constants import JUSTICE_COUNTS_SENTRY_DSN
from recidiviz.persistence.database.constants import JUSTICE_COUNTS_DB_SECRET_PREFIX
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tools.justice_counts.copy_over_metric_settings_to_child_agencies import (
    copy_metric_settings_and_alert_user,
    create_script_parser,
)
from recidiviz.utils.sendgrid_client_wrapper import SendGridClientWrapper

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sentry_sdk.init(
        dsn=JUSTICE_COUNTS_SENTRY_DSN,
        # Enable performance monitoring
        enable_tracing=True,
    )

    database_key = SQLAlchemyDatabaseKey.for_schema(
        SchemaType.JUSTICE_COUNTS,
    )
    justice_counts_engine = SQLAlchemyEngineManager.init_engine(
        database_key=database_key,
        secret_prefix_override=JUSTICE_COUNTS_DB_SECRET_PREFIX,
    )
    session = Session(bind=justice_counts_engine)

    parser = create_script_parser()
    args = parser.parse_args()

    send_grid_client = SendGridClientWrapper(key_type="justice_counts")
    _child_agency_id_subset = (
        list(map(int, args.child_agency_id_subset))
        if args.child_agency_id_subset
        else []
    )

    copy_metric_settings_and_alert_user(
        dry_run=False,
        super_agency_id=args.super_agency_id,
        metric_definition_key_subset=args.metric_definition_key_subset,
        child_agency_id_subset=_child_agency_id_subset,
        current_session=session,
        agency_name=args.agency_name,
        send_grid_client=send_grid_client,
        user_email=args.user_email,
    )
