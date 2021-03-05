# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""
Describe how mappings from the database to BQ combined aggregate views should be
defined.
 """

from typing import Optional

import attr
from sqlalchemy import null, Column
from sqlalchemy.orm import Query

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class Mappings:
    """Defines mappings from the database to BQ combined aggregate views.

    BQ Views created from this mappings represent data the can be aggregated by
    combining both the "state-reported aggregate reports" and "scraped website
    data". Any column in this mapping needs to be present from either multiple
    aggregate_reports or available from scraped websites.

    In practice, this means you should only add new mappings here if the column:
        1. Can be found in multiple aggregate reports or a websites
        2. Will be used to answer questions in cross-county analysis
    """

    # Required Primary Key
    fips: Column
    facility_name: Column
    report_date: Column
    aggregation_window: Column

    # Jurisdiction information
    resident_population: Optional[Column]
    jail_capacity: Optional[Column]

    # Totals
    custodial: Optional[Column]
    admissions: Optional[Column]
    jurisdictional: Optional[Column]

    # Gender
    male: Optional[Column]
    female: Optional[Column]

    # Incarceration status
    sentenced: Optional[Column]
    pretrial: Optional[Column]

    # Charge Class
    felony: Optional[Column]
    misdemeanor: Optional[Column]

    # Parole/Probation
    parole_violators: Optional[Column]
    probation_violators: Optional[Column]
    technical_parole_violators: Optional[Column]
    civil: Optional[Column]

    # Hold
    held_for_doc: Optional[Column]
    held_for_federal: Optional[Column]
    total_held_for_other: Optional[Column]
    held_elsewhere: Optional[Column]

    # Combinations (only a subset)
    jail_capacity_male: Optional[Column]
    jail_capacity_female: Optional[Column]

    sentenced_male: Optional[Column]
    sentenced_female: Optional[Column]
    pretrial_male: Optional[Column]
    pretrial_female: Optional[Column]

    felony_male: Optional[Column]
    felony_female: Optional[Column]

    felony_pretrial: Optional[Column]
    felony_sentenced: Optional[Column]

    felony_pretrial_male: Optional[Column]
    felony_pretrial_female: Optional[Column]
    felony_sentenced_male: Optional[Column]
    felony_sentenced_female: Optional[Column]

    misdemeanor_pretrial: Optional[Column]
    misdemeanor_sentenced: Optional[Column]

    misdemeanor_male: Optional[Column]
    misdemeanor_female: Optional[Column]

    misdemeanor_pretrial_male: Optional[Column]
    misdemeanor_pretrial_female: Optional[Column]
    misdemeanor_sentenced_male: Optional[Column]
    misdemeanor_sentenced_female: Optional[Column]

    parole_violators_male: Optional[Column]
    parole_violators_female: Optional[Column]

    held_for_doc_male: Optional[Column]
    held_for_doc_female: Optional[Column]
    held_for_federal_male: Optional[Column]
    held_for_federal_female: Optional[Column]
    total_held_for_other_male: Optional[Column]
    total_held_for_other_female: Optional[Column]

    def to_query(self) -> Query:
        """Create a query to SELECT each column based on the Mapping's name."""
        select_statements = []
        for new_view_column_name, source_column in attr.asdict(self).items():
            # Default unmapped columns to NULL to ensure we SELECT all columns
            select_statement = null().label(new_view_column_name)

            if source_column is not None:
                select_statement = source_column.label(new_view_column_name)

            select_statements.append(select_statement)

        return SessionFactory.for_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS)
        ).query(*select_statements)
