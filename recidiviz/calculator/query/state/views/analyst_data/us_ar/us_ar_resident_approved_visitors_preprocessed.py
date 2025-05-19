# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Preprocessed approved visitor information for AR residents."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AR_RESIDENT_APPROVED_VISITORS_PREPROCESSED_VIEW_NAME = (
    "us_ar_resident_approved_visitors_preprocessed"
)

US_AR_RESIDENT_APPROVED_VISITORS_PREPROCESSED_QUERY_TEMPLATE = """
WITH rar_deduped AS (
  -- For a given resident, each relative/associate (abbreviated as 'RA' throughout this query) should have 
  -- a single row with a unique RELASSOCSEQNUM in the RELASSOCRELATION table. Due to inconsistent data
  -- entry, this isn't the case, so we dedup here to get the most recent information for each R/A,
  -- so that a given resident will only have one row and one RELASSOCSEQNUM associated with each R/A.
  SELECT *
  FROM `{project_id}.{us_ar_raw_data_up_to_date_dataset}.RELASSOCRELATION_latest`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY OFFENDERID,RELATIVEID 
    -- Dedup by status date first, then update date, then RELASSOCSEQNUM to ensure determinism since
    -- RELASSOCSEQNUM is part of the primary key for this table.
    ORDER BY RELASSOCSTATUSDATE DESC, DATELASTUPDATE DESC, RELASSOCSEQNUM DESC
  ) = 1
),
vs_deduped AS (
  -- This table also needs to be deduped. Here, we get the most recent visitation status data for a given
  -- R/A. This table only uses RELASSOCSEQNUM, not RELATIVEID, to identify an R/A. Since the deduping for
  -- RELASSOCRELATION leaves only one RELASSOCSEQNUM per R/A, we can't simply join this table with rar_deduped
  -- since the RELASSOCSEQNUM that was kept for a given R/A might not match the RELASSOCSEQNUM used to record visitation
  -- status data. Therefore, we first join VISITATIONSTATUS with the full RELASSOCRELATION table so that RELATIVEID can be used
  -- as a key instead of RELASSOCSEQNUM in future joins.
  SELECT 
    vs.*,
    rar.RELATIVEID 
  FROM `{project_id}.{us_ar_raw_data_up_to_date_dataset}.VISITATIONSTATUS_latest` vs
  LEFT JOIN `{project_id}.{us_ar_raw_data_up_to_date_dataset}.RELASSOCRELATION_latest` rar
  USING(OFFENDERID,RELASSOCSEQNUM)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY OFFENDERID, rar.RELATIVEID 
    ORDER BY VISITATIONREVIEWDT DESC, DATELASTUPDATE DESC
  ) = 1
),
addresses AS (
  -- Addresses can be associated with R/As using the RELATEDPARTYTABLE, in which
  -- an R/A's ID will be shown as PARTYID and the address ID will be shown as the RELATEDPARTYID.
  -- This address ID then needs to be joined against the address table itself. The only relationship
  -- types we're interested in here are 9AA (person to physical address) and 9AB (person to mailing address). 
  SELECT 
    rp.PARTYID,
    PARTYRELTYPE = "9AA" AS is_physical_address,
    PARTYRELTYPE = "9AB" AS is_mailing_address,
    TO_JSON(STRUCT(
      IF (
        a.POBOX IS NOT NULL,
        CONCAT("PO Box ", a.POBOX),
        CONCAT(
          COALESCE(CONCAT(a.STREETNUMBER, ' '), ''),
          COALESCE(CONCAT(a.STREETDIRECTION, ' '), ''), 
          COALESCE(CONCAT(a.STREETNAME, ' '), ''), 
          COALESCE(a.STREETTYPE, ''),
          COALESCE(CONCAT(' ', a.SUITENUMBER), CONCAT(' ', a.APARTMENTNUM), '')
        )
      ) AS address_line_1,
      CONCAT(
        COALESCE(CONCAT(a.CITY, ', '), ''),
        COALESCE(CONCAT(a.STATE, ' '), ''),
        COALESCE(NULLIF(a.ZIPCODE, '00000'), '')
      ) AS address_line_2,
      COALESCE(
        CONCAT(
          NULLIF(county_code.CODEDESCRIPTION, "Other State"),
           " County"
        )
     ) AS county
    )) AS address_json
  FROM `{project_id}.{us_ar_raw_data_up_to_date_dataset}.RELATEDPARTY_latest` rp
  LEFT JOIN `{project_id}.{us_ar_raw_data_up_to_date_dataset}.ADDRESS_latest` a
    ON RELATEDPARTYID = ADDRESSID
  LEFT JOIN `{project_id}.us_ar_raw_data_up_to_date_views.CODEVALUEDESC_latest` county_code
  ON
    county_code.DATANAME = 'COUNTYCD'
    AND a.COUNTY = county_code.CODEVALUE
  -- Only use active addresses, which will have no relationship end date.
  WHERE rp.PARTYRELEND IS NULL
    
)

SELECT 
  pei.state_code,
  pei.person_id,
  r.RELATIVEID as ra_party_id,
  r.RELASSOCSEQNUM as ra_seq_num,
  r.RELATIONSHIPCODE as ra_relationship_type,
  pp.PERSONLASTNAME as ra_last_name,
  pp.PERSONFIRSTNAME as ra_first_name,
  pp.PERSONMIDDLENAME as ra_middle_name,
  pp.PERSONSUFFIX as ra_suffix,
  IF(pp.PERSONDATEOFBIRTH = "1000-01-01", NULL, DATE(pp.PERSONDATEOFBIRTH)) as ra_dob,
  -- The DOB is flagged as approximate if APPROXDOB = Y. If this is null, the DOB is not flagged as approximate.
  IFNULL(pp.APPROXDOB,"N") = "Y" as ra_dob_is_approx,
  pp.PERSONRACE as ra_race,
  pp.PERSONSEX as ra_sex,
  r.RELASSOCSTATUS as relationship_status,
  DATE(r.RELASSOCSTATUSDATE) as relationship_status_date,
  TO_JSON(
    STRUCT(
      IFNULL(r.EMERGENCYNOTIFY, "N") = "Y" AS emergency_notify,
      IFNULL(r.EMERGENCYNOTIFYALT, "N") = "Y" AS emergency_notify_alt,
      IFNULL(r.MEDICALINFOSHARED, "N") = "Y" AS can_share_med_info,
      IFNULL(r.MENTALHEALTHINFOSHARED, "N") = "Y" AS can_share_mh_info,
      IFNULL(r.DENTALINFOSHARED, "N") = "Y" AS can_share_dental_info,
      IFNULL(r.MEDICALDECISION, "N") = "Y" AS can_make_med_decisions,
      IFNULL(r.CLAIMPERSONALPROPERTY, "N") = "Y" AS authorized_to_claim_property,
      IFNULL(r.LIVESWITHOFFENDER, "N") = "Y" AS lives_with_resident,
      IFNULL(r.OFFENSEVICTIMFLAG, "N") = "Y" AS victim_of_resident,
      IFNULL(r.ACCOMPLICEFLAG, "N") = "Y" AS accomplice_of_resident,
      IFNULL(r.CRIMEHISTORYFLAG, "N") = "Y" AS has_criminal_history,
      IFNULL(r.WORKSLAWENFORCE, "N") = "Y" AS works_in_le,
      IFNULL(r.DEPENDENTCAREGUARDIANIND, "N") = "Y" AS is_dep_care_guardian
    )
  ) AS ra_checklist,
  visitor_physical_address.address_json as physical_address,
  visitor_mailing_address.address_json as mailing_address,
  ip.ADCNUMBER as visitation_resident_adc_number,
  CONCAT(ip.INMATELASTNAME,", ",ip.INMATEFIRSTNAME) as visitation_resident_name,
  DATE(v.VISITATIONREVIEWDT) AS visitation_review_date,
  v.DUROFVISITATIONSTATUS as visitation_dur_days,
  v.VISITATIONSPECIALCONDITION as visitation_special_condition_1,
  v.VISITATIONSPECIALCONDITION2 as visitation_special_condition_2,
  -- Unclear if this corresponds to the 'Reason' field in the form, especially since in the form it
  -- seems related to the 'Special condition' field. This is the only column in the data that looks like it fits the bill though.
  v.VISITATIONSTATUSREASON as visitation_status_reason,
  r.RELASSOCCOMMENTS as relationship_comments
FROM rar_deduped r 
LEFT JOIN vs_deduped v 
  USING(OFFENDERID,RELATIVEID)

LEFT JOIN (
    SELECT * 
    FROM addresses 
    WHERE is_physical_address
) visitor_physical_address 
  ON r.RELATIVEID = visitor_physical_address.PARTYID 

LEFT JOIN (
    SELECT * 
    FROM addresses 
    WHERE is_mailing_address
) visitor_mailing_address 
  ON r.RELATIVEID = visitor_mailing_address.PARTYID 

LEFT JOIN `{project_id}.{us_ar_raw_data_up_to_date_dataset}.PERSONPROFILE_latest` pp
  ON r.RELATIVEID = pp.PARTYID

LEFT JOIN `{project_id}.{us_ar_raw_data_up_to_date_dataset}.INMATEPROFILE_latest` ip
  ON r.OFFENDERID = ip.OFFENDERID

LEFT JOIN `{project_id}.normalized_state.state_person_external_id` pei
  ON 
    r.OFFENDERID = pei.external_id 
    AND pei.id_type = 'US_AR_OFFENDERID'
    AND pei.state_code='US_AR'
WHERE v.VISITATIONSTATUS = "A"
"""
US_AR_RESIDENT_APPROVED_VISITORS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AR_RESIDENT_APPROVED_VISITORS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_AR_RESIDENT_APPROVED_VISITORS_PREPROCESSED_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
    us_ar_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AR,
        instance=DirectIngestInstance.PRIMARY,
    ),
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AR_RESIDENT_APPROVED_VISITORS_PREPROCESSED_VIEW_BUILDER.build_and_print()
