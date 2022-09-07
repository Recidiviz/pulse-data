# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""View of archived compliant_reporting_referral_record.json exports from GCS"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    EXPORT_ARCHIVES_DATASET,
    STATE_BASE_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.populate_missing_exports_template import (
    populate_missing_export_dates,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_VIEW_NAME = (
    "compliant_reporting_referral_record_archive"
)

COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_VIEW_DESCRIPTION = (
    """View of archived compliant_reporting_referral_record.json exports from GCS"""
)

COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_QUERY_TEMPLATE = """
    /* {description} */
    WITH
    client_to_referral_record_migration_date AS (
        -- This CTE selects the earliest date that `remaining_criteria_needed` is populated in the referral record.
        -- We select this non-nullable field to identify when we migrated all the compliant reporting specific fields
        -- from the client record to the compliant reporting referral record. This archive view selects the compliant
        -- reporting specific fields from the client_record until the date that they were migrated to the referral
        -- record.  
        SELECT 
            MIN(DATE(SPLIT(SUBSTRING(_FILE_NAME, 6), "/")[OFFSET(1)])) AS earliest_date
        FROM `{project_id}.{export_archives_dataset}.workflows_compliant_reporting_referral_record_archive`
        WHERE remaining_criteria_needed IS NOT NULL
    ),
    compliant_reporting_split_path AS (
        SELECT
            *,
            SPLIT(SUBSTRING(_FILE_NAME, 6), "/") AS path_parts,
            DATE(SPLIT(SUBSTRING(_FILE_NAME, 6), "/")[OFFSET(1)]) AS export_date,
            SPLIT(SUBSTRING(_FILE_NAME, 6), "/")[OFFSET(2)] AS state_code,
        FROM `{project_id}.{export_archives_dataset}.workflows_compliant_reporting_referral_record_archive`        
        -- exclude temp files we may have inadvertently archived
        WHERE _FILE_NAME NOT LIKE "%/staging/%"
    )
    , client_record_split_path AS (
        SELECT
            person_external_id,
            compliant_reporting_eligible,
            remaining_criteria_needed,
            almost_eligible_time_on_supervision_level,
            almost_eligible_drug_screen,
            almost_eligible_fines_fees,
            almost_eligible_recent_rejection,
            almost_eligible_serious_sanctions,
            DATE(SPLIT(SUBSTRING(_FILE_NAME, 6), "/")[OFFSET(1)]) AS export_date,
            SPLIT(SUBSTRING(_FILE_NAME, 6), "/")[OFFSET(2)] AS state_code,
        FROM `{project_id}.{export_archives_dataset}.workflows_legacy_client_record_archive`
        -- exclude temp files we may have inadvertently archived
        WHERE _FILE_NAME NOT LIKE "%/staging/%"
    )
    , compliant_reporting_archive_fields AS (
        SELECT
            cr.state_code,
            cr.export_date,
            cr.tdoc_id AS person_external_id,
            -- The compliant_reporting_eligible field started being populated on 4/14/22 on the Compliant Referral
            -- record, however there are some instances where we see a value in the client record that does not
            -- exist on the referral record. This is a fix for those discrepancies.
            COALESCE(cr.compliant_reporting_eligible, client.compliant_reporting_eligible) AS compliant_reporting_eligible,
            -- These fields were migrated from the client_record to the compliant_reporting_referral_record
            CASE WHEN cr.export_date >= (SELECT earliest_date FROM client_to_referral_record_migration_date)
                THEN cr.remaining_criteria_needed
                ELSE client.remaining_criteria_needed
            END AS remaining_criteria_needed,
            CASE WHEN cr.export_date >= (SELECT earliest_date FROM client_to_referral_record_migration_date)
                THEN cr.almost_eligible_time_on_supervision_level
                ELSE client.almost_eligible_time_on_supervision_level
            END AS almost_eligible_time_on_supervision_level,
            CASE WHEN cr.export_date >= (SELECT earliest_date FROM client_to_referral_record_migration_date)
                THEN cr.almost_eligible_drug_screen
                ELSE client.almost_eligible_drug_screen
            END AS almost_eligible_drug_screen,
            CASE WHEN cr.export_date >= (SELECT earliest_date FROM client_to_referral_record_migration_date)
                THEN cr.almost_eligible_fines_fees
                ELSE client.almost_eligible_fines_fees
            END AS almost_eligible_fines_fees,
            CASE WHEN cr.export_date >= (SELECT earliest_date FROM client_to_referral_record_migration_date)
                THEN cr.almost_eligible_recent_rejection
                ELSE client.almost_eligible_recent_rejection
            END AS almost_eligible_recent_rejection,
            CASE WHEN cr.export_date >= (SELECT earliest_date FROM client_to_referral_record_migration_date)
                THEN cr.almost_eligible_serious_sanctions
                ELSE client.almost_eligible_serious_sanctions
            END AS almost_eligible_serious_sanctions,
        FROM compliant_reporting_split_path cr
        LEFT JOIN client_record_split_path client
        ON cr.export_date = client.export_date
        AND cr.state_code = client.state_code
        AND cr.tdoc_id = client.person_external_id
    )
    , records_by_state_by_date AS (
        -- dedupes repeat uploads for the same date
        SELECT DISTINCT
            state_code,
            export_date,
            person_external_id,
            remaining_criteria_needed,
            compliant_reporting_eligible,
            almost_eligible_time_on_supervision_level,
            almost_eligible_drug_screen,
            almost_eligible_fines_fees,
            almost_eligible_recent_rejection,
            almost_eligible_serious_sanctions,
        FROM compliant_reporting_archive_fields
    )
    {populate_missing_export_dates}
    SELECT
        person_id,
        records_by_state_by_date.state_code,
        records_by_state_by_date.export_date,
        generated_export_date AS date_of_supervision,
        records_by_state_by_date.person_external_id,
        remaining_criteria_needed,
        compliant_reporting_eligible,
        almost_eligible_time_on_supervision_level,
        almost_eligible_drug_screen,
        almost_eligible_fines_fees,
        almost_eligible_recent_rejection,
        almost_eligible_serious_sanctions,
    FROM date_to_archive_map
    LEFT JOIN records_by_state_by_date USING (state_code, export_date)
    LEFT JOIN `{project_id}.{state_base_dataset}.state_person_external_id` pei
        ON records_by_state_by_date.state_code = pei.state_code
        AND records_by_state_by_date.person_external_id = pei.external_id
"""

COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_VIEW_NAME,
    description=COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_VIEW_DESCRIPTION,
    view_query_template=COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_QUERY_TEMPLATE,
    should_materialize=True,
    export_archives_dataset=EXPORT_ARCHIVES_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    populate_missing_export_dates=populate_missing_export_dates(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_VIEW_BUILDER.build_and_print()
