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
"""View of effective opportunity snooze spans exported from Firestore"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    EXPORT_ARCHIVES_DATASET,
    REFERENCE_VIEWS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.pipelines.supplemental.dataset_config import SUPPLEMENTAL_DATA_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SNOOZE_SPANS_VIEW_NAME = "clients_snooze_spans"

SNOOZE_SPANS_VIEW_DESCRIPTION = (
    """View of effective opportunity snooze spans exported from Firestore"""
)


def migrated_snoozes(
    opportunity_type: str, migration_date: str, migrated_length_days: int
) -> str:
    """
    Returns a subquery which runs referral_status_denial_spans through the migration logic:
    If a denial was manually ended before the migration date, it's passed through unchanged.
    If the denial start_date + migrated_length_days is after the migration date, end_date_actual
    is set to that date. Otherwise end_date_actual is set to the migration date.
    """
    return f"""
SELECT
    * EXCEPT(end_date_actual),
    IF(end_date_actual IS NULL,
        GREATEST(
            "{migration_date}",
            DATE_ADD(start_date, INTERVAL {migrated_length_days} DAY)
            ),
        LEAST(
            end_date_actual,
            GREATEST(
                "{migration_date}",
                DATE_ADD(start_date, INTERVAL {migrated_length_days} DAY)
                )
            )
    ) as end_date_actual
FROM referral_status_denial_spans
WHERE
    opportunity_type = "{opportunity_type}"
"""


SNOOZE_SPANS_QUERY_TEMPLATE = f"""
    WITH
    -- archive_snooze_spans captures snoozes active on and since the creation of workflows_snooze_status_archive.
    -- These statuses always reflect what a user would have seen in Workflows, even if a snooze is manually cancelled before it expires
    archive_data AS (
        SELECT
            IF(state_code = "US_ID", "US_IX", state_code) as state_code,
            IF(state_code = "US_MI" AND NOT STARTS_WITH(person_external_id, "0"), "0" || person_external_id, person_external_id) AS person_external_id,
            opportunity_type, 
            snooze_start_date,
            snoozed_by,
            denial_reasons,
            other_reason,
            snooze_end_date,
            as_of
        FROM `{{project_id}}.{{export_archives_dataset}}.workflows_snooze_status_archive`
    )
    , archive_snooze_spans AS (
        SELECT
            -- "archive" as provenance,
            person_id,
            a.state_code,
            a.person_external_id,
            a.opportunity_type, 
            snooze_start_date as start_date, 
            ANY_VALUE(snoozed_by) as snoozed_by,
            ANY_VALUE(denial_reasons) as denial_reasons,
            ANY_VALUE(other_reason) as other_reason,
            ANY_VALUE(snooze_end_date) as end_date_scheduled,
            if(
                MAX(as_of)<(SELECT MAX(as_of) FROM `{{project_id}}.{{export_archives_dataset}}.workflows_snooze_status_archive`),
                max(as_of),
                null) as end_date_actual
        FROM archive_data a
        LEFT JOIN `{{project_id}}.{{reference_dataset}}.workflows_opportunity_configs_materialized` config
            USING (state_code, opportunity_type)
        LEFT JOIN `{{project_id}}.{{workflows_dataset}}.person_id_to_external_id_materialized` pei
            ON pei.state_code = a.state_code
            AND UPPER(pei.person_external_id) = UPPER(a.person_external_id)
            AND pei.system_type = IF(config.person_record_type = "CLIENT", "SUPERVISION", "INCARCERATION")
        GROUP BY person_id, state_code, person_external_id, opportunity_type, start_date
    ),
    -- event_snooze_spans captures snoozes that were set by users after the snooze feature was released (i.e. not migrated denials)
    -- but were no longer active when we started archiving snooze statuses. These are constructed from the analytics event fired when
    -- the snooze was created. We can't know if/when these snoozes were cancelled, so we assume that they ran until expiration.
    event_snooze_spans AS (
        SELECT 
            -- "event" as provenance,
            s.person_id,
            s.state_code,
            s.person_external_id,
            s.opportunity_type,
            DATETIME(timestamp)as start_date,
            email as snoozed_by,
            JSON_EXTRACT_STRING_ARRAY(reasons) as denial_reasons,
            CAST(NULL AS STRING) as other_reason,
            COALESCE(
                DATETIME(snooze_until),
                DATE_ADD(DATETIME(timestamp), INTERVAL snooze_for_days DAY)
            ) as end_date_scheduled,
        FROM `{{project_id}}.{{workflows_dataset}}.clients_opportunity_snoozed_materialized` s
        LEFT JOIN archive_snooze_spans a
            ON s.person_id = a.person_id and s.opportunity_type = a.opportunity_type and DATE(s.timestamp) = a.start_date
        WHERE a.person_id IS NULL  -- exclude snoozes that are already captured above
        AND DATETIME(timestamp) < "{{first_snooze_archive_date}}"
    ),
    -- referral_status_denial_spans captures denials that were created before the snooze feature was released. They may have an end_date_actual
    -- if the person's referral status was subsequently updated making them eligible again. This CTE isn't directly unioned into the final view:
    -- It is referred to by migrated_snoozes(), which adjusts the entries for a particular opportunity to account for how it was migrated to a snooze.
    referral_status_denial_spans AS (
        SELECT
            -- "migrated" as provenance,
            p.person_id,
            p.state_code,
            p.person_external_id,
            p.opportunity_type,
            DATETIME(timestamp) as start_date,
            email as snoozed_by,
            denied_reasons as denial_reasons,
            CAST(NULL AS STRING) as other_reason,
            CAST(NULL AS DATE) as end_date_scheduled,
            DATETIME((
                SELECT MIN(c.timestamp)
                FROM `{{project_id}}.{{workflows_dataset}}.clients_referral_status_updated` c
                WHERE c.person_id=p.person_id
                AND c.opportunity_type=p.opportunity_type
                AND c.timestamp > p.timestamp
            )) as end_date_actual
        FROM `{{project_id}}.{{workflows_dataset}}.clients_referral_status_updated` p
        LEFT JOIN archive_snooze_spans a
            ON p.person_id = a.person_id and p.opportunity_type = a.opportunity_type and DATE(p.timestamp) = a.start_date
        LEFT JOIN event_snooze_spans e
            -- opportunity_snoozed and referral_status_updated are distinct events so their timestamps differ by a tiny bit
            ON p.person_id = e.person_id and p.opportunity_type = e.opportunity_type and DATETIME_DIFF(DATETIME(p.timestamp),e.start_date,MINUTE) = 0
        WHERE status = "DENIED"
        AND a.person_id IS NULL  -- exclude snoozes that are already captured above
        AND e.person_id IS NULL
        AND DATETIME(timestamp) < "{{first_snooze_archive_date}}"
    ),
    -- case_note_snooze_spans captures Maine snoozes recorded in case notes. A new case note for an opportunity supercedes any
    -- snooze that's currently active, which we capture in end_date_scheduled.
    case_note_snooze_spans AS (
        WITH extracted_case_notes AS (
            SELECT
                person_id,
                state_code,
                person_external_id,
                JSON_VALUE(note, '$.opportunity_type') AS opportunity_type,
                PARSE_DATE('%Y-%m-%d', JSON_VALUE(note, '$.start_date')) AS start_date,
                JSON_VALUE(note, '$.officer_email') AS snoozed_by,
                JSON_VALUE_ARRAY(note, '$.denial_reasons') AS denial_reasons,
                JSON_VALUE(note, '$.other_text') AS other_reason,
                PARSE_DATE('%Y-%m-%d', JSON_VALUE(note, '$.end_date')) AS end_date_scheduled,
                LEAD(PARSE_DATE('%Y-%m-%d', JSON_VALUE(note, '$.start_date')))
                    OVER (PARTITION BY person_id, JSON_VALUE(note, '$.opportunity_type')
                          ORDER BY PARSE_DATE('%Y-%m-%d', JSON_VALUE(note, '$.start_date'))) AS next_start_date
            FROM `{{project_id}}.{{supplemental_dataset}}.us_me_snoozed_opportunities`
            WHERE is_valid_snooze_note
        )
        SELECT
            * EXCEPT (next_start_date),
            CASE
                -- If there is another snooze for the same person and opportunity type that starts before this snooze ends
                WHEN next_start_date < end_date_scheduled THEN next_start_date
                -- If the scheduled end date has passed
                WHEN end_date_scheduled < CURRENT_DATE('US/Eastern') THEN end_date_scheduled
                -- Otherwise, end_date_actual is NULL
                ELSE NULL
            END AS end_date_actual
        FROM extracted_case_notes
        WHERE start_date != end_date_scheduled  -- Filter out zero-length "snoozes": these are created to cancel a previous snooze
    )

    SELECT * FROM archive_snooze_spans

    UNION ALL

    SELECT 
        *, LEAST(end_date_scheduled, "{{first_snooze_archive_date}}") as end_date_actual
    FROM event_snooze_spans

    UNION ALL

    SELECT * FROM case_note_snooze_spans
  
    -- migrations per https://github.com/Recidiviz/recidiviz-dashboards/issues/4060  
    UNION ALL
    {migrated_snoozes("pastFTRD", "2024-02-05", 30)}
    UNION ALL
    {migrated_snoozes("usTnExpiration", "2024-02-05", 30)}
    UNION ALL
    {migrated_snoozes("LSU", "2024-02-05", 90)}
    UNION ALL
    {migrated_snoozes("earnedDischarge", "2024-02-05", 90)}
    UNION ALL
    {migrated_snoozes("usIdSupervisionLevelDowngrade", "2024-02-05", 90)}
    UNION ALL
    {migrated_snoozes("compliantReporting", "2024-02-05", 90)}
    UNION ALL
    {migrated_snoozes("supervisionLevelDowngrade", "2024-02-05", 90)}
    UNION ALL
    {migrated_snoozes("usTnCustodyLevelDowngrade", "2024-02-05", 90)}
    UNION ALL
    {migrated_snoozes("usTnAnnualReclassification", "2024-02-05", 90)}
    UNION ALL
    {migrated_snoozes("usMeSCCP", "2024-02-05", 180)}
    UNION ALL
    {migrated_snoozes("usMeEarlyTermination", "2024-02-05", 180)}
    UNION ALL
    {migrated_snoozes("usMeWorkRelease", "2024-02-05", 180)}
    UNION ALL
    {migrated_snoozes("usMeFurloughRelease", "2024-02-05", 180)}

    -- migrations per https://github.com/Recidiviz/recidiviz-dashboards/issues/4776
    UNION ALL
    {migrated_snoozes("usMiPastFTRD", "2024-03-04", 30)}
    UNION ALL
    {migrated_snoozes("usMiEarlyDischarge", "2024-03-04", 90)}
    UNION ALL
    {migrated_snoozes("usMiMinimumTelephoneReporting", "2024-03-04", 90)}
    UNION ALL
    {migrated_snoozes("usMiSupervisionLevelDowngrade", "2024-03-04", 90)}
    UNION ALL
    {migrated_snoozes("usMiClassificationReview", "2024-03-04", 180)}

    -- migrations per https://github.com/Recidiviz/recidiviz-dashboards/issues/4897
    UNION ALL
    {migrated_snoozes("earlyTermination", "2024-03-18", 90)}
"""

CLIENTS_SNOOZE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=SNOOZE_SPANS_VIEW_NAME,
    description=SNOOZE_SPANS_VIEW_DESCRIPTION,
    view_query_template=SNOOZE_SPANS_QUERY_TEMPLATE,
    should_materialize=True,
    export_archives_dataset=EXPORT_ARCHIVES_DATASET,
    workflows_dataset=WORKFLOWS_VIEWS_DATASET,
    reference_dataset=REFERENCE_VIEWS_DATASET,
    supplemental_dataset=SUPPLEMENTAL_DATA_DATASET,
    first_snooze_archive_date="2024-03-27",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_SNOOZE_SPANS_VIEW_BUILDER.build_and_print()
