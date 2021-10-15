/*
This SQL script is used to create materialized versions of the static reference table used by sessions to infer start and end reasons
based on transitions. The non-materialized views are maintained by the Google Sheet "Compartment Sessions Inferred Transition Reasons"
which is located in the DADS shared Google Drive folder. This SQL script then creates tables from these views which get joined to
compartment sessions.
*/


DROP TABLE IF EXISTS `recidiviz-staging.static_reference_tables.session_inferred_start_reasons_materialized`;

CREATE TABLE `recidiviz-staging.static_reference_tables.session_inferred_start_reasons_materialized` AS
    SELECT * FROM `recidiviz-staging.static_reference_tables.session_inferred_start_reasons`;

DROP TABLE IF EXISTS `recidiviz-123.static_reference_tables.session_inferred_start_reasons_materialized`;

CREATE TABLE `recidiviz-123.static_reference_tables.session_inferred_start_reasons_materialized` AS
    SELECT * FROM `recidiviz-123.static_reference_tables.session_inferred_start_reasons`;

DROP TABLE IF EXISTS `recidiviz-staging.static_reference_tables.session_inferred_end_reasons_materialized`;

CREATE TABLE `recidiviz-staging.static_reference_tables.session_inferred_end_reasons_materialized` AS
    SELECT * FROM `recidiviz-staging.static_reference_tables.session_inferred_end_reasons`;

DROP TABLE IF EXISTS `recidiviz-123.static_reference_tables.session_inferred_end_reasons_materialized`;

CREATE TABLE `recidiviz-123.static_reference_tables.session_inferred_end_reasons_materialized` AS
    SELECT * FROM `recidiviz-123.static_reference_tables.session_inferred_end_reasons`;

DROP TABLE IF EXISTS `recidiviz-staging.static_reference_tables.sessions_location_type_lookup_materialized`;

CREATE TABLE`recidiviz-staging.static_reference_tables.sessions_location_type_lookup_materialized` AS
    SELECT * FROM `recidiviz-staging.static_reference_tables.sessions_location_type_lookup`;

DROP TABLE IF EXISTS `recidiviz-123.static_reference_tables.sessions_location_type_lookup_materialized`;

CREATE TABLE`recidiviz-123.static_reference_tables.sessions_location_type_lookup_materialized` AS
    SELECT * FROM `recidiviz-staging.static_reference_tables.sessions_location_type_lookup`;
