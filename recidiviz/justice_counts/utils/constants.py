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

REPORTING_FREQUENCY_CONTEXT_KEY = "REPORTING_FREQUENCY"

DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS = "DISAGGREGATED_BY_SUPERVISION_SUBSYSTEMS"

AUTOMATIC_UPLOAD_ID = -1

# Maps the actual name of the child agency to
# a shorthand used in a the spreadsheet during
# Bulk Upload. We need this because some agencies
# only want to provide shorthands in their spreadsheets.
CHILD_AGENCY_NAME_TO_UPLOAD_NAME = {
    "toledo police department": "toledo",
    "newark division of police": "newark",
    "cleveland police department": "cleveland",
    "columbus police department": "columbus",
}


class DatapointGetRequestEntryPoint(enum.Enum):
    REPORT_PAGE = "REPORT_PAGE"
    METRICS_TAB = "METRICS_TAB"


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

# This dictionary contains ids and names of agencies that exist in our database, but
# should be excluded from the weekly CSG Data Pull. Agencies are excluded for a number
# of reasons, including if they were an Alpha Partner, the agency has yet to opt-in, etc.
AGENCIES_TO_EXCLUDE = {
    74: "Muskegon County Prosecutor's Office",
    75: "Douglas County District Attorney's Office",
    73: "Clackamas County Jail",
    79: "San Leandro Police Department",
    81: "Yavapai County District Attorney",
    77: "[Deprecated] North Carolina Department of Public Safety",
    83: "Georgia Department of Community Supervision",
    84: "Indiana Public Defender Commission",
    80: "Seattle Police Department",
    142: "Oregon Department of Corrections",
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
    627: "[DELETE] Illinois Criminal Justice Information Authority (ICJA)",
    628: "[DELETE] Georgia Judicial Council and Administrative Office of the Courts",
    85: "Washington State Department of Corrections",
    478: "Alabama Department of Corrections",
    479: "Alaska Department of Corrections",
    480: "Arizona Department of Corrections, Rehabilitation & Reentry",
    481: "Arkansas Department of Correction",
    482: "California Department of Corrections and Rehabilitation",
    483: "Colorado Department of Corrections",
    485: "Delaware Department of Correction",
    484: "Connecticut Department of Correction",
    487: "District of Columbia Department of Corrections",
    488: "Florida Department of Corrections",
    489: "Georgia Department of Corrections",
    490: "Hawaii Department of Public Safety",
    491: "Idaho Department of Correction",
    492: "Illinois Department of Corrections",
    493: "Indiana Department of Correction",
    494: "Kansas Department of Corrections",
    495: "Kentucky Department of Corrections",
    497: "Maine Department of Corrections",
    498: "Maryland Department of Public Safety and Correctional Services",
    501: "Michigan Department of Corrections",
    502: "Minnesota Department of Corrections",
    503: "Mississippi Department of Corrections",
    521: "Utah Department of Corrections",
    511: "North Dakota Department of Corrections and Rehabilitation",
    506: "Nevada Department of Corrections",
    508: "New Jersey Department of Corrections",
    512: "Ohio Department of Rehabilitation and Correction",
    522: "Vermont Department of Corrections",
    519: "Tennessee Department of Correction",
    513: "Oklahoma Department of Corrections",
    516: "Rhode Island Department of Corrections",
    520: "Texas Department of Criminal Justice",
    523: "Virginia Department of Corrections",
    517: "South Carolina Department of Corrections",
    518: "South Dakota Department of Corrections",
    525: "West Virginia Division of Corrections and Rehabilitation",
    526: "Wisconsin Department of Corrections",
    527: "Wyoming Department of Corrections",
    1455: "Law Enforcement [DEMO]",
    1458: "Courts [DEMO]",
    1459: "Defense [DEMO]",
    1460: "Prosecution [DEMO]",
    1461: "Jails [DEMO]",
    1462: "Prisons [DEMO]",
    1463: "Supervision [DEMO]",
    1464: "Department of Corrections",
}
