# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Contains Justice Counts constants """
import enum
import re

from recidiviz.persistence.database.schema.justice_counts import schema

REPORTING_FREQUENCY_CONTEXT_KEY = "REPORTING_FREQUENCY"

DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS = "DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS"

INVALID_CHILD_AGENCY = "INVALID_CHILD_AGENCY"

UNEXPECTED_ERROR = "UNEXPECTED_ERROR"

AUTOMATIC_UPLOAD_ID = -1

# Unsubscribe group ID number for SendGrid for Justice Counts
UNSUBSCRIBE_GROUP_ID = 26272


class DatapointGetRequestEntryPoint(enum.Enum):
    REPORT_PAGE = "REPORT_PAGE"
    METRICS_TAB = "METRICS_TAB"


class UploadMethod(enum.Enum):
    MANUAL_ENTRY = "MANUAL_ENTRY"
    BULK_UPLOAD = "BULK_UPLOAD"
    AUTOMATED_BULK_UPLOAD = "AUTOMATED_BULK_UPLOAD"


# Column headers for single-page upload.
BREAKDOWN_CATEGORY = "breakdown_category"
BREAKDOWN = "breakdown"

# Used to infer agency_id from bucket name during Automatic Upload
AUTOMATIC_UPLOAD_BUCKET_REGEX = re.compile(r".*-ingest-agency-(?P<agency_id>[0-9]+)")

# Bucket to store Bulk Upload Errors/Warnings JSON
ERRORS_WARNINGS_JSON_BUCKET_STAGING = (
    "justice-counts-staging-bulk-upload-errors-warnings-json"
)
ERRORS_WARNINGS_JSON_BUCKET_PROD = (
    "justice-counts-production-bulk-upload-errors-warnings-json"
)

# Bucket for New Mexico Super Agency Uploads
NEW_MEXICO_SUPERAGENCY_BUCKET_STAGING = "justice-counts-staging-ingest-agency-207"
NEW_MEXICO_SUPERAGENCY_BUCKET_PROD = "justice-counts-production-ingest-agency-534"

# Bucket for Reminder Email HTML files
REMINDER_EMAILS_BUCKET_STAGING = "justice-counts-staging-reminder-emails"
REMINDER_EMAILS_BUCKET_PROD = "justice-counts-production-reminder-emails"

# Justice Counts Sentry Data Source Name. See monitoring page at https://recidiviz-inc.sentry.io/projects/justice-counts/?project=4504532096516096
JUSTICE_COUNTS_SENTRY_DSN = (
    # not a secret!
    "https://3e8c790dbf0c407b8c039b91c7af9abc@o432474.ingest.sentry.io/4504532096516096"
)

# The name of the Cloud Run Job responsible for copying superagency metric settings to its respective child agencies
# Requires a `project_id` - either "justice-counts-staging" or "justice-counts-production" depending on the environment
COPY_SUPERAGENCY_METRIC_SETTINGS_TO_CHILD_AGENCIES_JOB_NAME = "projects/{project_id}/locations/us-central1/jobs/copy-superagency-metric-settings-to-child-agencies"

# This dictionary contains ids and names of agencies that exist in our database, but
# should be excluded from the weekly CSG Data Pull. Agencies are excluded for a number
# of reasons, including if they were an Alpha Partner, the agency has yet to opt-in, etc.
AGENCIES_TO_EXCLUDE = {
    74: "Muskegon County Prosecutor's Office",
    75: "Douglas County District Attorney's Office",
    79: "San Leandro Police Department",
    81: "Yavapai County District Attorney",
    83: "Georgia Department of Community Supervision",
    84: "Indiana Public Defender Commission",
    592: "Cibola County Magistrae Court in Grants",
    593: "Fourth Judicial District Guadalupe County",
    594: "Taos County Magistrate Court in Questa",
    595: "Second Judicial District Bernalillo County",
    596: "Sixth Judicial District Grant County",
    597: "Tenth Judicial District De Baca County",
    598: "Fourth Judicial District San Miguel County",
    599: "Tenth Judicial District Harding County",
    600: "DeBaca County Magistrate Court in Fort Sumner",
    601: "Valencia County Magistrate Court in Belen",
    602: "Lea County Magistrate Court in Hobbs",
    603: "Otero County Magistrate Court in Alamogordo",
    604: "Catron County Magistrate Court in Quemado",
    545: "Thirteenth Judicial District Sandoval County",
    546: "Sandoval County Magistrate Court in Bernalillo",
    547: "San Juan County Magistrate Court in Aztec",
    548: "Valencia County Magistrate Court in Los Lunas",
    549: "Catron County Magistrate Court in Reserve",
    550: "Eleventh Judicial District San Juan County",
    551: "Tenth Judicial District Quay County",
    552: "Sixth Judicial District Luna County",
    553: "Dona Ana County Magistrate Court in Anthony",
    554: "Grant County Magistrate Court in Bayard",
    555: "Lea County Magistrate Court in Lovington",
    556: "Hidalgo County Magistrate Court in Lordsburg",
    557: "Eleventh Judicial District McKinley County",
    558: "Fifth Judicial District Eddy County",
    559: "Thirteenth Judicial District Valencia County",
    560: "Eddy County Magistrate Court in Carlsbad",
    561: "Harding County Magistrate Court in Roy",
    562: "Fourth Judicial District Mora County",
    563: "Sierra County Magistrate Court in T or C",
    564: "Seventh Judicial District Sierra County",
    565: "Taos County Magistrate Court in Taos",
    566: "Rio Arriba County Magistrate Court in Chama",
    567: "Twelfth Judicial District Otero County",
    568: "Fifth Judicial District Lea County",
    569: "Sixth Judicial District Hidalgo County",
    570: "Socorro County Magistrate Court in Socorro",
    571: "Eighth Judicial District Taos County",
    572: "Eighth Judicial District Colfax County",
    573: "Dona Ana County Magistrate Court in Las Cruces",
    574: "Grant County Magistrate Court in Silver City",
    575: "Lincoln County Magistrate Court in Ruidoso",
    576: "Roosevelt County Magistrate Court in Portales",
    577: "Torrance County Magistrate Court in Estancia",
    578: "Eddy County Magistrate Court in Artesia",
    579: "McKinley County Magistrate Court in Gallup",
    580: "Guadalupe County Magistrate Court in Santa Rosa",
    581: "Seventh Judicial District Torrance County",
    582: "Mora County Magistrate Court in Mora",
    583: "Quay County Magistrate Court in Tucumcari",
    584: "Curry County Magistrate Court in Clovis",
    585: "Chaves County Magistrate Court in Roswell",
    586: "Twelfth Judicial District Lincoln County",
    587: "Lincoln County Magistrate Court in Carrizozo",
    588: "First Judicial District Rio Arriba County",
    589: "Eighth Judicial District Union County",
    590: "Thirteenth Judicial District Cibola County",
    591: "San Juan County Magistrate Court in Farmington",
    605: "Torrance County Magistrate Court in Moriarty",
    606: "Seventh Judicial District Socorro County",
    607: "Fifth Judicial District Chaves County",
    608: "Rio Arriba County Magistrate Court in Espanola",
    609: "Colfax County Magistrate Court in Raton",
    610: "Bernalillo County Metropolitan Court",
    611: "Los Alamos County Magistrate Court in Los Alamos",
    612: "Lea County Magistrate Court in Jal",
    613: "Dona Ana County Magistrate Court in Hatch",
    614: "First Judicial District Santa Fe County",
    615: "San Miguel County Magistrate Court in Las Vegas",
    616: "Union County Magistrate Court in Clayton",
    617: "First Judicial District Los Alamos County",
    618: "Third Judicial District Dona Ana County",
    619: "Ninth Judicial District Roosevelt County",
    620: "Luna County Magistrate Court in Deming",
    621: "Santa Fe County Magistrate Court in Santa Fe",
    622: "Ninth Judicial District Curry County",
    623: "Sandoval County Magistrate Court in Cuba",
    624: "Lea County Magistrate Court in Eunice",
    625: "Colfax County Magistrate Court in Springer",
    626: "Seventh Judicial District Catron County",
    1437: "Wisconsin Law Enforcement - All Agencies",
    1455: "Law Enforcement [DEMO]",
    1458: "Courts [DEMO]",
    1459: "Defense [DEMO]",
    1460: "Prosecution [DEMO]",
    1461: "Jails [DEMO]",
    1462: "Prisons [DEMO]",
    1463: "Supervision [DEMO]",
    1464: "Department of Corrections",
}

VALID_SYSTEMS = [
    schema.System.LAW_ENFORCEMENT,
    schema.System.PROSECUTION,
    schema.System.DEFENSE,
    schema.System.COURTS_AND_PRETRIAL,
    schema.System.JAILS,
    schema.System.PRISONS,
    schema.System.SUPERVISION,
    schema.System.PAROLE,
    schema.System.PROBATION,
    schema.System.PRETRIAL_SUPERVISION,
    schema.System.OTHER_SUPERVISION,
]
