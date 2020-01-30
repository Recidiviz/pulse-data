# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Reference views used by other views."""
from recidiviz.calculator.query.state.views.reference.incarceration_admissions_deduped import \
    INCARCERATION_ADMISSIONS_DEDUPED_VIEW
from recidiviz.calculator.query.state.views.reference.incarceration_admissions_by_person_and_month import \
    INCARCERATION_ADMISSIONS_BY_PERSON_AND_MONTH_VIEW
from recidiviz.calculator.query.state.views.reference.incarceration_releases_by_person_and_month import \
    INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_VIEW
from recidiviz.calculator.query.state.views.reference.incarceration_releases_deduped import \
    INCARCERATION_RELEASES_DEDUPED_VIEW
from recidiviz.calculator.query.state.views.reference.lsir_scores_by_person_period_and_date_month import \
    LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_VIEW
from recidiviz.calculator.query.state.views.reference.most_recent_calculate_job import \
    MOST_RECENT_CALCULATE_JOB_VIEW
from recidiviz.calculator.query.state.views.reference.incarceration_admissions_by_person_60_days import \
    INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.reference.persons_on_supervision_60_days import \
    PERSONS_ON_SUPERVISION_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.reference.persons_on_supervision_60_days_with_county_and_revocation import \
    PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_VIEW
from recidiviz.calculator.query.state.views.reference.second_and_last_lsir_scores_by_supervision_period import \
    SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_VIEW
from recidiviz.calculator.query.state.views.reference.persons_with_age import \
    PERSONS_WITH_AGE_VIEW
from recidiviz.calculator.query.state.views.reference.persons_with_last_known_address import \
    PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW
from recidiviz.calculator.query.state.views.reference.supervision_termination_earliest_start_latest_end import \
    SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_VIEW
from recidiviz.calculator.query.state.views.reference.supervision_terminations_10_years import \
    SUPERVISION_TERMINATIONS_10_YEARS_VIEW
from recidiviz.calculator.query.state.views.reference.us_nd.ftr_referrals_60_days import \
    FTR_REFERRAL_VIEW
from recidiviz.calculator.query.state.views.reference.state_person_race_and_ethnicity import \
    STATE_PERSON_RACE_AND_ETHNICITY_VIEW
from recidiviz.calculator.query.state.views.reference.supervision_success_by_person import \
    SUPERVISION_SUCCESS_BY_PERSON_VIEW
from recidiviz.calculator.query.state.views.reference.supervision_termination_by_person_and_projected_completion \
    import SUPERVISION_TERMINATION_BY_PERSON_AND_PROJECTED_COMPLETION_VIEW
from recidiviz.calculator.query.state.views.reference.persons_with_most_recent_lsir import \
    PERSONS_WITH_MOST_RECENT_LSIR_VIEW

REF_VIEWS = [
    MOST_RECENT_CALCULATE_JOB_VIEW,
    INCARCERATION_ADMISSIONS_DEDUPED_VIEW,
    INCARCERATION_ADMISSIONS_BY_PERSON_AND_MONTH_VIEW,
    INCARCERATION_ADMISSIONS_BY_PERSON_60_DAYS_VIEW,
    INCARCERATION_RELEASES_DEDUPED_VIEW,
    INCARCERATION_RELEASES_BY_PERSON_AND_MONTH_VIEW,
    STATE_PERSON_RACE_AND_ETHNICITY_VIEW,
    SUPERVISION_TERMINATIONS_10_YEARS_VIEW,
    SUPERVISION_TERMINATION_BY_PERSON_AND_PROJECTED_COMPLETION_VIEW,
    SUPERVISION_TERMINATION_EARLIEST_START_LATEST_END_VIEW,
    SUPERVISION_SUCCESS_BY_PERSON_VIEW,
    PERSONS_ON_SUPERVISION_60_DAYS_VIEW,
    PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW,
    PERSONS_ON_SUPERVISION_60_DAYS_WITH_COUNTY_AND_REVOCATION_VIEW,
    PERSONS_WITH_MOST_RECENT_LSIR_VIEW,
    LSIR_SCORES_BY_PERSON_PERIOD_AND_DATE_MONTH_VIEW,
    SECOND_AND_LAST_LSIR_SCORES_BY_SUPERVISION_PERIOD_VIEW,
    FTR_REFERRAL_VIEW,
    PERSONS_WITH_AGE_VIEW,
]
