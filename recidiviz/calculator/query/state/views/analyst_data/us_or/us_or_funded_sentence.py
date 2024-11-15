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
"""Identifies individuals' supervision sentences in OR that are eligible for
consideration for earned discharge due to their being "funded" sentences, as outlined in
ORS 423.478."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    sentence_attributes,
)
from recidiviz.task_eligibility.utils.us_or_query_fragments import (
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2017_08_15,
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_02_01,
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_07_19,
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2023_07_27,
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2024_09_01,
    OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2025_01_01,
    OR_EARNED_DISCHARGE_DESIGNATED_PERSON_MISDEMEANORS_2022_01_01,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_OR_FUNDED_SENTENCE_VIEW_NAME = "us_or_funded_sentence"

US_OR_FUNDED_SENTENCE_VIEW_DESCRIPTION = """Identifies individuals' supervision
sentences in OR that are eligible for consideration for earned discharge due to their
being "funded" sentences, as outlined in ORS 423.478."""

# TODO(#35095): Account for conviction status (i.e., conditional discharge or diversion)
# and supervision/sentence type (i.e., probation vs. local-control post-prison vs.
# general post-prison vs. other types) within this criterion.
US_OR_FUNDED_SENTENCE_QUERY_TEMPLATE = f"""
    WITH sentences AS (
        /* NB: this query pulls from `sentences_preprocessed` (not `sentence_spans`,
        even though we'll ultimately end up creating spans for eligibility). This has
        been done because if we start from `sentences_preprocessed`, we start with a
        single span and end up with at most two spans per sentence for each
        subcriterion; however, if we started from `sentence_spans`, we might start with
        multiple spans per sentence that we'd then have to work with. Also, we treat
        each sentence separately when evaluating eligibility for OR earned discharge. If
        we decide to change this in the future, we can refactor this subcriterion query
        to rely upon `sentence_spans`. */
        SELECT
            * EXCEPT (offense_date),
            /* In cases where the `offense_date` is missing or is '1901-01-01' (which
            appears frequently in OR and seems to function similarly to a NULL value?),
            we use the sentence start date as a proxy for the `offense_date`. The
            offense date only comes into play when we are looking to identify designated
            drug-related misdemeanors below. Note that we are not currently replacing
            `offense_date` values in cases when the date is entered into the data but is
            mistyped/erroneous; however, among sentences for misdemeanor convictions
            under any statutes that have *ever* been in the list of designated
            drug-related misdemeanors, there do not appear to be any typos in offense
            dates, so it shouldn't present a problem to ignore that possibility in this
            criterion for now. We'll focus just on missing or '1901-01-01' offense
            dates. */
            IF((offense_date IS NULL) OR (offense_date='1901-01-01'),
                start_date,
                offense_date
            ) AS offense_date,
        FROM ({sentence_attributes()})
        WHERE state_code='US_OR'
            AND sentence_type='SUPERVISION'
    )
    SELECT
        state_code,
        person_id,
        sentence_id,
        start_date,
        end_date,
        CASE
            /* The original bill (House Bill 3194 [2013]) that established EDIS was
            effective 2013-07-25 and made eligible any sentences for felony
            convictions. */
            WHEN (classification_type='FELONY') THEN TRUE
            /* Identify designated drug-related misdemeanors as established by House
            Bill 2355 (2017), which took effect on 2017-08-15. */
            WHEN (
                (classification_type='MISDEMEANOR')
                AND (statute IN ({list_to_query_string(OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2017_08_15, quoted=True)}))
                AND (offense_date BETWEEN '2017-08-15' AND '2021-01-31')
                ) THEN TRUE
            /* Identify designated drug-related misdemeanors as defined by Ballot
            Measure 110 (2020), from which relevant changes became effective on
            2021-02-01. */
            WHEN (
                (classification_type='MISDEMEANOR')
                AND (statute IN ({list_to_query_string(OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_02_01, quoted=True)}))
                AND (offense_date BETWEEN '2021-02-01' AND '2021-07-18')
                ) THEN TRUE
            /* Identify designated drug-related misdemeanors as defined by Senate Bill
            755 (2021), which was effective on 2021-07-19. */
            WHEN (
                (classification_type='MISDEMEANOR')
                AND (statute IN ({list_to_query_string(OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2021_07_19, quoted=True)}))
                AND (offense_date BETWEEN '2021-07-19' AND '2023-07-26')
                ) THEN TRUE
            /* Identify designated drug-related misdemeanors as defined by House Bill
            2645 (2023), which was effective on 2023-07-27.
            NB: Oregon's EDIS eligibility code checks that the conviction date (not the
            offense date) is on or after 2023-07-27 for the 475.752 (8)(a) offenses.
            However, given that this specific crime was not defined in statute prior to
            HB 2645 anyways, it shouldn't have been possible for individuals to commit
            this crime or be convicted of it prior to that date. Therefore, we just
            continue to check the offense date for all statutes, rather than using
            conviction date for just 475.752 (8)(a). (Basically, using the offense date
            shouldn't cause any errors in this case, since it wasn't possible to commit
            an offense under that statute until 2023-07-27 anyways.) We'll keep checking
            the offense date for all the designated drug-related misdemeanors. */
            WHEN (
                (classification_type='MISDEMEANOR')
                AND (statute IN ({list_to_query_string(OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2023_07_27, quoted=True)}))
                AND (offense_date BETWEEN '2023-07-27' AND '2024-08-31')
                ) THEN TRUE
            /* Identify designated drug-related misdemeanors as defined by House Bill
            4002 (2024), from which relevant changes became effective on 2024-09-01. */
            WHEN (
                (classification_type='MISDEMEANOR')
                AND (statute IN ({list_to_query_string(OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2024_09_01, quoted=True)}))
                AND (offense_date BETWEEN '2024-09-01' AND '2024-12-31')
                ) THEN TRUE
            /* Identify designated drug-related misdemeanors as defined by Senate Bill
            1553 (2024), which will take effect on 2025-01-01. */
            WHEN (
                (classification_type='MISDEMEANOR')
                AND (statute IN ({list_to_query_string(OR_EARNED_DISCHARGE_DESIGNATED_DRUG_RELATED_MISDEMEANORS_2025_01_01, quoted=True)}))
                AND (offense_date>='2025-01-01')
                ) THEN TRUE
            /* Identify designated person misdemeanors as defined by Senate Bill 497
            (2021), which was effective on 2022-01-01. */
            WHEN (
                (classification_type='MISDEMEANOR')
                AND (statute IN ({list_to_query_string(OR_EARNED_DISCHARGE_DESIGNATED_PERSON_MISDEMEANORS_2022_01_01, quoted=True)}))
                /* NB: we're using the sentence start date here to identify funded
                sentences for designated person misdemeanors, which is what OR has been
                doing in their EDIS eligibility code. */
                AND (start_date>='2022-01-01')
                ) THEN TRUE
            /* Any other sentences (which would be those sentences that are not for
            felonies or designated drug-related or person misdemeanors) have never been
            eligible. */
            ELSE FALSE
        END AS meets_criteria,
    FROM sentences
"""

US_OR_FUNDED_SENTENCE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_OR_FUNDED_SENTENCE_VIEW_NAME,
    description=US_OR_FUNDED_SENTENCE_VIEW_DESCRIPTION,
    view_query_template=US_OR_FUNDED_SENTENCE_QUERY_TEMPLATE,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OR_FUNDED_SENTENCE_VIEW_BUILDER.build_and_print()
