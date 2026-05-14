## Diagnosing Insights / Supervision Homepage (SHP) Issues

The Supervision Homepage displays officers' data to their supervisors. The
relevant data is stored in the `outliers_views` dataset.

#### I-i: Verify the supervisor exists

Check if the supervisor exists in the system:

```sql
SELECT external_id, full_name, email, supervision_location_for_supervisor_page
FROM `recidiviz-123.outliers_views.supervision_officer_supervisors_materialized`
WHERE state_code = '<STATE_CODE>' AND (LOWER(full_name) LIKE '%<name_keyword>%' OR LOWER(email) LIKE '%<email_keyword>%')
```

If the supervisor is not found, they may not be configured as a supervisor role.

#### I-ii: Find the officers and verify supervisor relationships

Identify the affected officers by name and verify they have the supervisor's
external ID in their `supervisor_external_ids` array:

```sql
SELECT external_id, full_name, email, supervisor_external_ids
FROM `recidiviz-123.outliers_views.supervision_officers_materialized`
WHERE state_code = '<STATE_CODE>' AND (LOWER(full_name) LIKE '%<officer_name_keyword>%' OR external_id = '<OFFICER_ID>')
```

**Key check:** The supervisor's external ID must be in the
`supervisor_external_ids` array. If it's not, the officer won't be returned when
filtering for that supervisor's subordinates.

#### I-iii: Verify officers have required metrics data

Officers must have current metrics in `supervision_officer_metrics_materialized`
to appear:

```sql
SELECT officer_id, COUNT(*) as metric_count
FROM `recidiviz-123.outliers_views.supervision_officer_metrics_materialized`
WHERE state_code = '<STATE_CODE>'
  AND officer_id IN ('<OFFICER_ID_1>', '<OFFICER_ID_2>')
  AND CURRENT_DATE('US/Eastern') BETWEEN start_date AND IFNULL(end_date, '9999-12-31')
GROUP BY officer_id
```

#### I-iv: Check aggregated metrics for task activity

Officers must have current aggregated metrics to appear in opportunities views:

```sql
SELECT
  officer_id,
  period,
  end_date,
  prop_period_with_critical_caseload
FROM `recidiviz-123.outliers_views.supervision_officer_aggregated_metrics_materialized`
WHERE state_code = '<STATE_CODE>'
  AND officer_id IN ('<OFFICER_ID_1>', '<OFFICER_ID_2>')
  AND CURRENT_DATE('US/Eastern') BETWEEN start_date AND IFNULL(end_date, '9999-12-31')
```

**Key insight on `prop_period_with_critical_caseload`:**

- This is a proportion (0.0 to 1.0) representing the fraction of the analysis
  period when the officer had a critical (high) caseload.
- Officers with `prop_period_with_critical_caseload >= 0.75` are actively
  included in outliers analysis.
- If the value is very low (< 0.75), the officer may be deprioritized or
  filtered out.

#### I-v: Check client records and staff records

Verify officers appear in the frontend record views with clients:

```sql
SELECT
  officer_id,
  COUNT(*) as total_clients,
  COUNTIF(ARRAY_LENGTH(all_eligible_opportunities) > 0) as clients_with_opportunities
FROM `recidiviz-123.workflows_views.client_record_materialized`
WHERE state_code = '<STATE_CODE>' AND officer_id IN ('<OFFICER_ID_1>', '<OFFICER_ID_2>')
GROUP BY officer_id
```

And check staff records:

```sql
SELECT email, supervisor_external_ids
FROM `recidiviz-123.workflows_views.supervision_staff_record_materialized`
WHERE state_code = '<STATE_CODE>' AND email IN ('<OFFICER_EMAIL_1>', '<OFFICER_EMAIL_2>')
```
