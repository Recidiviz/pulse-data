# pylint: skip-file
"""update metric definition key second time

Revision ID: c10b5e6ed94d
Revises: d59ee5722264
Create Date: 2022-10-24 15:48:39.752048

"""
import sqlalchemy as sa
from alembic import op

from recidiviz.utils.string import StrictStringFormatter

# revision identifiers, used by Alembic.
revision = "c10b5e6ed94d"
down_revision = "d59ee5722264"
branch_labels = None
depends_on = None


OLD_KEY_TO_NEW_KEY = {
    "COURTS_AND_PRETRIAL_BUDGET_": "COURTS_AND_PRETRIAL_BUDGET",
    "COURTS_AND_PRETRIAL_BUDGET_metric/courts/staff/type": "COURTS_AND_PRETRIAL_TOTAL_STAFF",
    "COURTS_AND_PRETRIAL_RESIDENTS_global/gender/restricted,global/race_and_ethnicity": "COURTS_AND_PRETRIAL_RESIDENTS",
    "COURTS_AND_PRETRIAL_CASES_DISPOSED_metric/disposition/type": "COURTS_AND_PRETRIAL_CASES_DISPOSED",
    "COURTS_AND_PRETRIAL_CASES_OVERTURNED_ON_APPEAL_": "COURTS_AND_PRETRIAL_CASES_OVERTURNED_ON_APPEAL",
    "COURTS_AND_PRETRIAL_CASES_FILED_metric/courts/case/severity/type": "COURTS_AND_PRETRIAL_CASES_FILED",
    "COURTS_AND_PRETRIAL_ARRESTS_ON_PRETRIAL_RELEASE_metric/courts/case/type": "COURTS_AND_PRETRIAL_ARRESTS_ON_PRETRIAL_RELEASE",
    "COURTS_AND_PRETRIAL_PRETRIAL_RELEASES_metric/courts/release/type": "COURTS_AND_PRETRIAL_PRETRIAL_RELEASES",
    "COURTS_AND_PRETRIAL_SENTENCES_global/gender/restricted,global/race_and_ethnicity,metric/courts/sentence/type": "COURTS_AND_PRETRIAL_SENTENCES",
    "DEFENSE_BUDGET_": "DEFENSE_BUDGET",
    "DEFENSE_TOTAL_STAFF_metric/staff/prosecution_defense/type": "DEFENSE_TOTAL_STAFF",
    "DEFENSE_RESIDENTS_global/gender/restricted,global/race_and_ethnicity": "DEFENSE_RESIDENTS",
    "DEFENSE_CASES_APPOINTED_COUNSEL_metric/severity/prosecution/type": "DEFENSE_CASES_APPOINTED_COUNSEL",
    "DEFENSE_CASELOADS_metric/severity/prosecution/type": "DEFENSE_CASELOADS",
    "DEFENSE_CASES_DISPOSED_global/gender/restricted,global/race_and_ethnicity,metric/disposition/type": "DEFENSE_CASES_DISPOSED",
    "DEFENSE_COMPLAINTS_SUSTAINED_": "DEFENSE_COMPLAINTS_SUSTAINED",
    "JAILS_BUDGET_": "JAILS_BUDGET",
    "JAILS_TOTAL_STAFF_metric/staff/correctional_facility/type": "JAILS_TOTAL_STAFF",
    "JAILS_RESIDENTS_global/gender/restricted,global/race_and_ethnicity": "JAILS_RESIDENTS",
    "JAILS_ADMISSIONS_metric/jails/population/type": "JAILS_ADMISSIONS",
    "JAILS_POPULATION_global/gender/restricted,global/race_and_ethnicity,metric/jails/population/type": "JAILS_POPULATION",
    "JAILS_READMISSIONS_metric/jails/reported_crime/type": "JAILS_READMISSIONS",
    "JAILS_RELEASES_metric/jails/release/type": "JAILS_RELEASES",
    "JAILS_USE_OF_FORCE_INCIDENTS_metric/correctional_facility/force/type": "JAILS_USE_OF_FORCE_INCIDENTS",
    "JAILS_GRIEVANCES_UPHELD_": "JAILS_GRIEVANCES_UPHELD",
    "LAW_ENFORCEMENT_BUDGET_": "LAW_ENFORCEMENT_BUDGET",
    "LAW_ENFORCEMENT_TOTAL_STAFF_": "LAW_ENFORCEMENT_TOTAL_STAFF",
    "LAW_ENFORCEMENT_RESIDENTS_global/gender/restricted,global/race_and_ethnicity": "LAW_ENFORCEMENT_RESIDENTS",
    "LAW_ENFORCEMENT_CALLS_FOR_SERVICE_metric/law_enforcement/calls_for_service/type": "LAW_ENFORCEMENT_CALLS_FOR_SERVICE",
    "LAW_ENFORCEMENT_REPORTED_CRIME_metric/law_enforcement/reported_crime/type": "LAW_ENFORCEMENT_REPORTED_CRIME",
    "LAW_ENFORCEMENT_ARRESTS_global/gender/restricted,global/race_and_ethnicity,metric/law_enforcement/reported_crime/type": "LAW_ENFORCEMENT_ARRESTS",
    "LAW_ENFORCEMENT_USE_OF_FORCE_INCIDENTS_metric/law_enforcement/officer_use_of_force_incidents/type": "LAW_ENFORCEMENT_USE_OF_FORCE_INCIDENTS",
    "LAW_ENFORCEMENT_COMPLAINTS_SUSTAINED_": "LAW_ENFORCEMENT_COMPLAINTS_SUSTAINED",
    "PRISONS_BUDGET_": "PRISONS_BUDGET",
    "PRISONS_BUDGET_metric/staff/correctional_facility/type": "PRISONS_TOTAL_STAFF",
    "PRISONS_RESIDENTS_global/gender/restricted,global/race_and_ethnicity": "PRISONS_RESIDENTS",
    "PRISONS_ADMISSIONS_metric/prison/population/type": "PRISONS_ADMISSIONS",
    "PRISONS_POPULATION_global/gender/restricted,global/race_and_ethnicity,metric/prison/population/type": "PRISONS_POPULATION",
    "PRISONS_READMISSIONS_metric/jails/reported_crime/type": "PRISONS_READMISSIONS",
    "PRISONS_RELEASES_metric/prisons/release/type": "PRISONS_RELEASES",
    "PRISONS_USE_OF_FORCE_INCIDENTS_metric/correctional_facility/force/type": "PRISONS_USE_OF_FORCE_INCIDENTS",
    "PRISONS_GRIEVANCES_UPHELD_": "PRISONS_GRIEVANCES_UPHELD",
    "PROSECUTION_BUDGET_": "PROSECUTION_BUDGET",
    "PROSECUTION_TOTAL_STAFF_metric/staff/prosecution_defense/type": "PROSECUTION_TOTAL_STAFF",
    "PROSECUTION_RESIDENTS_global/gender/restricted,global/race_and_ethnicity": "PROSECUTION_RESIDENTS",
    "PROSECUTION_CASELOADS_metric/severity/prosecution/type": "PROSECUTION_CASELOADS",
    "PROSECUTION_CASES_DISPOSED_metric/disposition/type": "PROSECUTION_CASES_DISPOSED",
    "PROSECUTION_CASES REFERRED_metric/severity/prosecution/type": "PROSECUTION_CASES REFERRED",
    "PROSECUTION_CASES_DECLINED_global/gender/restricted,global/race_and_ethnicity,metric/severity/prosecution/type": "PROSECUTION_CASES_DECLINED",
    "PROSECUTION_VIOLATIONS_WITH_DISCIPLINARY_ACTION_": "PROSECUTION_VIOLATIONS_WITH_DISCIPLINARY_ACTION",
    "SUPERVISION_BUDGET_": "SUPERVISION_BUDGET",
    "SUPERVISION_TOTAL_STAFF_metric/staff/supervision/type": "SUPERVISION_TOTAL_STAFF",
    "SUPERVISION_RESIDENTS_global/gender/restricted,global/race_and_ethnicity": "SUPERVISION_RESIDENTS",
    "SUPERVISION_POPULATION_global/gender/restricted,global/race_and_ethnicity,metric/supervision/individual/type": "SUPERVISION_POPULATION",
    "SUPERVISION_POPULATION_metric/supervision/case/type": "SUPERVISION_SUPERVISION_STARTS",
    "SUPERVISION_RECONVICTIONS_metric/supervision/offense/type": "SUPERVISION_RECONVICTIONS",
    "SUPERVISION_SUPERVISION_TERMINATIONS_metric/supervision/termination/type": "SUPERVISION_SUPERVISION_TERMINATIONS",
    "SUPERVISION_SUPERVISION_VIOLATIONS_metric/violation/supervision/type": "SUPERVISION_SUPERVISION_VIOLATIONS",
    "PAROLE_BUDGET_": "PAROLE_BUDGET",
    "PAROLE_TOTAL_STAFF_metric/staff/supervision/type": "PAROLE_TOTAL_STAFF",
    "PAROLE_POPULATION_global/gender/restricted,global/race_and_ethnicity,metric/supervision/individual/type": "PAROLE_POPULATION",
    "PAROLE_POPULATION_metric/supervision/case/type": "PAROLE_SUPERVISION_STARTS",
    "PAROLE_RECONVICTIONS_metric/supervision/offense/type": "PAROLE_RECONVICTIONS",
    "PAROLE_SUPERVISION_TERMINATIONS_metric/supervision/termination/type": "PAROLE_SUPERVISION_TERMINATIONS",
    "PAROLE_SUPERVISION_VIOLATIONS_metric/violation/supervision/type": "PAROLE_SUPERVISION_VIOLATIONS",
    "PROBATION_BUDGET_": "PROBATION_BUDGET",
    "PROBATION_TOTAL_STAFF_metric/staff/supervision/type": "PROBATION_TOTAL_STAFF",
    "PROBATION_POPULATION_global/gender/restricted,global/race_and_ethnicity,metric/supervision/individual/type": "PROBATION_POPULATION",
    "PROBATION_POPULATION_metric/supervision/case/type": "PROBATION_SUPERVISION_STARTS",
    "PROBATION_RECONVICTIONS_metric/supervision/offense/type": "PROBATION_RECONVICTIONS",
    "PROBATION_SUPERVISION_TERMINATIONS_metric/supervision/termination/type": "PROBATION_SUPERVISION_TERMINATIONS",
    "PROBATION_SUPERVISION_VIOLATIONS_metric/violation/supervision/type": "PROBATION_SUPERVISION_VIOLATIONS",
    "POST_RELEASE_BUDGET_": "POST_RELEASE_BUDGET",
    "POST_RELEASE_TOTAL_STAFF_metric/staff/supervision/type": "POST_RELEASE_TOTAL_STAFF",
    "POST_RELEASE_POPULATION_global/gender/restricted,global/race_and_ethnicity,metric/supervision/individual/type": "POST_RELEASE_POPULATION",
    "POST_RELEASE_POPULATION_metric/supervision/case/type": "POST_RELEASE_SUPERVISION_STARTS",
    "POST_RELEASE_RECONVICTIONS_metric/supervision/offense/type": "POST_RELEASE_RECONVICTIONS",
    "POST_RELEASE_SUPERVISION_TERMINATIONS_metric/supervision/termination/type": "POST_RELEASE_SUPERVISION_TERMINATIONS",
    "POST_RELEASE_SUPERVISION_VIOLATIONS_metric/violation/supervision/type": "POST_RELEASE_SUPERVISION_VIOLATIONS",
}

NEW_KEY_TO_OLD_KEY = {v: k for k, v in OLD_KEY_TO_NEW_KEY.items()}

QUERY = """
UPDATE datapoint SET metric_definition_key = '{new_key}' WHERE metric_definition_key = '{old_key}';
"""


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    for old_key, new_key in OLD_KEY_TO_NEW_KEY.items():
        op.execute(
            StrictStringFormatter().format(
                QUERY,
                old_key=old_key,
                new_key=new_key,
            )
        )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    for new_key, old_key in NEW_KEY_TO_OLD_KEY.items():
        StrictStringFormatter().format(
            QUERY,
            old_key=new_key,
            new_key=old_key,
        )
    # ### end Alembic commands ###
