# Profiling queries

Run each query against `recidiviz-staging` with `bq query --use_legacy_sql=false
--format=pretty`. Replace `{STATE}` with the state code (e.g. `US_AZ`), `{state}`
with its lowercase form (e.g. `us_az`), and `{INSTRUMENT}` with the custody
classification's `assessment_type` chosen in step 2. `{TODAY}` is today's date.

Every number in the report must come from one of these queries. Never estimate a
number or carry one over from the example report.

## 1. Assessments by class, type, decision, and subtype

Finds the custody classification instrument and the state of every assessment
field.

```sql
SELECT assessment_class, assessment_type, IFNULL(assessment_decision, "NULL") AS decision,
  IFNULL(assessment_subtype, "NULL") AS subtype,
  COUNT(*) n, COUNTIF(assessment_score IS NOT NULL) scored, COUNTIF(assessment_level IS NOT NULL) has_level,
  COUNTIF(assessment_override_level IS NOT NULL) has_override_level,
  COUNTIF(assessment_override_reason IS NOT NULL) has_override_reason,
  MIN(assessment_date) first_date, MAX(assessment_date) last_date
FROM `recidiviz-staging.{state}_normalized_state.state_assessment`
GROUP BY 1, 2, 3, 4 ORDER BY n DESC
```

## 2. Instrument metadata keys

Shows whether override, reclassification, or per-question fields exist in
`assessment_metadata` but aren't mapped to their entity fields.

```sql
SELECT key, COUNT(*) AS n_assessments
FROM `recidiviz-staging.{state}_normalized_state.state_assessment`,
UNNEST(REGEXP_EXTRACT_ALL(assessment_metadata, r"\"([A-Z_a-z0-9]+)\":")) AS key
WHERE assessment_type = "{INSTRUMENT}" AND assessment_date >= DATE_SUB("{TODAY}", INTERVAL 2 YEAR)
GROUP BY 1 ORDER BY 2 DESC
```

If an override flag and an assigned-level key exist, cross-tab them against
`assessment_level` (adapt the key names) to check whether the scored and assigned
levels share a scale and to count overrides:

```sql
SELECT
  JSON_VALUE(assessment_metadata, "$.<OVERRIDE_FLAG_KEY>") AS is_override,
  assessment_level,
  JSON_VALUE(assessment_metadata, "$.<ASSIGNED_LEVEL_KEY>") AS assigned_level,
  COUNT(*) AS n
FROM `recidiviz-staging.{state}_normalized_state.state_assessment`
WHERE assessment_type = "{INSTRUMENT}" AND assessment_date >= DATE_SUB("{TODAY}", INTERVAL 2 YEAR)
GROUP BY 1, 2, 3 ORDER BY n DESC LIMIT 25
```

## 3. Incidents

```sql
SELECT COUNT(*) n, COUNTIF(is_violent IS NOT NULL) has_is_violent, COUNTIF(is_violent) is_violent_true,
  COUNTIF(incident_severity IS NOT NULL) has_severity,
  COUNT(DISTINCT incident_severity) distinct_severity, MIN(incident_date) first_date, MAX(incident_date) last_date
FROM `recidiviz-staging.{state}_normalized_state.state_incarceration_incident`
```

```sql
SELECT outcome_type, COUNT(*) n
FROM `recidiviz-staging.{state}_normalized_state.state_incarceration_incident_outcome`
GROUP BY 1 ORDER BY 2 DESC LIMIT 10
```

```sql
SELECT incident_severity, incident_severity_raw_text, COUNT(*) n
FROM `recidiviz-staging.{state}_normalized_state.state_incarceration_incident`
WHERE incident_date BETWEEN DATE_SUB("{TODAY}", INTERVAL 3 YEAR) AND "{TODAY}"
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
```

## 4. Violence definition inputs

The most common incident types in the most severe class:

```sql
SELECT incident_type, incident_type_raw_text, COUNT(*) AS incidents
FROM `recidiviz-staging.{state}_normalized_state.state_incarceration_incident`
WHERE incident_severity = "HIGHEST" AND incident_date BETWEEN DATE_SUB("{TODAY}", INTERVAL 3 YEAR) AND "{TODAY}"
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 15
```

Violent-looking incident types by severity:

```sql
SELECT incident_severity, incident_type_raw_text, COUNT(*) AS incidents
FROM `recidiviz-staging.{state}_normalized_state.state_incarceration_incident`
WHERE incident_date BETWEEN DATE_SUB("{TODAY}", INTERVAL 3 YEAR) AND "{TODAY}"
  AND (incident_type = "VIOLENCE"
    OR REGEXP_CONTAINS(incident_type_raw_text, r"ASSAULT|FIGHT|MURDER|HOMICIDE|SEXUAL|HOSTAGE|RIOT|STAB|BATTERY|KIDNAP"))
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20
```

If `incident_severity` is empty, rerun both on `incident_severity_raw_text`, and
note in the report that severity isn't mapped.

## 5. Other entities

```sql
SELECT "program_assignment" AS entity, COUNT(*) n, COUNTIF(participation_status IS NOT NULL) c1,
  COUNTIF(participation_status IN ("DISCHARGED_SUCCESSFUL","DISCHARGED_SUCCESSFUL_WITH_DISCRETION")) c2,
  COUNTIF(start_date IS NOT NULL) c3, COUNTIF(discharge_date IS NOT NULL) c4
FROM `recidiviz-staging.{state}_normalized_state.state_program_assignment`
UNION ALL
SELECT "incarceration_period", COUNT(*), COUNTIF(custody_level IS NOT NULL),
  COUNTIF(custodial_authority = "STATE_PRISON"), NULL, NULL
FROM `recidiviz-staging.{state}_normalized_state.state_incarceration_period`
UNION ALL
SELECT "person", COUNT(*), COUNTIF(birthdate IS NOT NULL), NULL, NULL, NULL
FROM `recidiviz-staging.{state}_normalized_state.state_person`
```

```sql
SELECT COUNT(*) n, COUNTIF(is_violent_uniform IS NOT NULL) has_uniform
FROM `recidiviz-staging.sentence_sessions.sentences_and_charges_materialized`
WHERE state_code = "{STATE}"
```

Birthdate coverage among the people actually classified:

```sql
WITH assessed AS (
  SELECT DISTINCT person_id FROM `recidiviz-staging.{state}_normalized_state.state_assessment`
  WHERE assessment_type = "{INSTRUMENT}" AND assessment_date >= DATE_SUB("{TODAY}", INTERVAL 3 YEAR)
)
SELECT COUNT(*) AS assessed_people, COUNTIF(p.birthdate IS NOT NULL) AS with_birthdate
FROM assessed JOIN `recidiviz-staging.{state}_normalized_state.state_person` p USING (person_id)
```
