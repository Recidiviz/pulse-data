# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Column name constants for the document extraction result BQ tables."""

STATE_CODE_COLUMN_NAME = "state_code"
EXTRACTION_JOB_ID_COLUMN_NAME = "extraction_job_id"
EXTRACTOR_ID_COLUMN_NAME = "extractor_id"
EXTRACTOR_VERSION_ID_COLUMN_NAME = "extractor_version_id"
DOCUMENT_CONTENTS_ID_COLUMN_NAME = "document_contents_id"
RESULT_DATETIME_UTC_COLUMN_NAME = "result_datetime_utc"
RESULT_JSON_COLUMN_NAME = "result_json"
VALIDATION_CONFIG_VERSION_ID_COLUMN_NAME = "validation_config_version_id"
VALIDATION_DATETIME_UTC_COLUMN_NAME = "validation_datetime_utc"
IS_RELEVANT_COLUMN_NAME = "is_relevant"
PASSED_VALIDATION_COLUMN_NAME = "passed_validation"
WILL_RETRY_COLUMN_NAME = "will_retry"
VALIDATION_ISSUES_JSON_COLUMN_NAME = "validation_issues_json"

# Keys within each object of the VALIDATION_ISSUES_JSON_COLUMN_NAME JSON array
VALIDATION_ISSUE_CHECK_NAME_FIELD = "check_name"
VALIDATION_ISSUE_FIELD_NAME_FIELD = "field_name"
VALIDATION_ISSUE_DETAIL_FIELD = "detail"
