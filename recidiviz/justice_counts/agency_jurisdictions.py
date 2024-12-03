# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.p
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Interface for working with the AgencyJurisdiction model."""
from collections import defaultdict
from typing import Dict, List

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.persistence.database.schema.justice_counts import schema


class AgencyJurisdictionInterface:
    """Contains methods for setting and getting AgencyJurisdictions."""

    @staticmethod
    def create_or_update_agency_jurisdictions(
        session: Session,
        agency_id: int,
        included_geoids: List[str],
        excluded_geoids: List[str],
    ) -> None:
        """For the specified agency, deletes all rows in the AgencyJurisdiction table.
        Then, inserts the updated rows in the AgencyJurisdiction table.
        """
        # first, delete all existing jurisdiction records for the agency
        delete_statement = delete(schema.AgencyJurisdiction).where(
            schema.AgencyJurisdiction.source_id == agency_id
        )
        session.execute(delete_statement)

        # now, insert new included rows
        for geoid in included_geoids:
            insert_statement = insert(schema.AgencyJurisdiction).values(
                source_id=agency_id,
                membership=schema.AgencyJurisdictionType.INCLUDE.value,
                geoid=geoid,
            )
            session.execute(insert_statement)
        # now, insert new excluded rows
        for geoid in excluded_geoids:
            insert_statement = insert(schema.AgencyJurisdiction).values(
                source_id=agency_id,
                membership=schema.AgencyJurisdictionType.EXCLUDE.value,
                geoid=geoid,
            )
            session.execute(insert_statement)

    @staticmethod
    def to_json(
        session: Session,
        agency_id: int,
    ) -> Dict:
        all_agency_jurisdictions = (
            session.query(schema.AgencyJurisdiction).filter(
                schema.AgencyJurisdiction.source_id == agency_id
            )
        ).all()
        included_ids = []
        excluded_ids = []
        for jurisdiction in all_agency_jurisdictions:
            if jurisdiction.membership == "INCLUDE":
                included_ids.append(jurisdiction.geoid)
            else:
                excluded_ids.append(jurisdiction.geoid)
        return {
            "included": included_ids,
            "excluded": excluded_ids,
        }

    @staticmethod
    def get_agency_population(
        session: Session,
        agency: schema.Agency,
    ) -> Dict[str, Dict[str, Dict[int, int]]]:
        """
        Retrieves population data for an agency's state based on race/ethnicity and biological sex.

        This method reads data from two CSV files containing state population breakdowns by
        race/ethnicity and biological sex, filters the data to match the specified state's
        name, and formats the information into nested dictionaries organized by categories.

        Args:
            agency (schema.Agency): An Agency object containing information such as the state code.

        Returns:
            Dict[str, Dict[str, Dict[int, int]]]: A dictionary structured as follows:
                {
                    "race_and_ethnicity": {
                        "<race_eth>": {<year>: <population>, ...},
                        ...
                    },
                    "biological_sex": {
                        "<sex>": {<year>: <population>, ...},
                        ...
                    }
                }
                Each key represents a demographic category, and each inner dictionary maps
                the year to the corresponding population for each category.
        """
        populations_dict: Dict[str, Dict[str, Dict[int, int]]] = {}
        # Fetch all jurisdictions associated with the agency
        all_agency_jurisdictions = (
            session.query(schema.AgencyJurisdiction)
            .filter(schema.AgencyJurisdiction.source_id == agency.id)
            .all()
        )

        # Map jurisdiction GEOIDs to their membership status (INCLUDE/EXCLUDE)
        geoid_to_membership = {
            jurisdiction.geoid: jurisdiction.membership
            for jurisdiction in all_agency_jurisdictions
        }

        # List of GEOIDs associated with the agency
        geoids = list(geoid_to_membership.keys())

        # Fetch population data for all relevant jurisdictions
        jurisdiction_populations = (
            session.query(schema.JurisdictionPopulation)
            .filter(schema.JurisdictionPopulation.geoid.in_(geoids))
            .all()
        )

        # Dictionaries to store aggregated population data
        race_eth_populations_dict: Dict[str, Dict[int, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        bio_sex_populations_dict: Dict[str, Dict[int, int]] = defaultdict(
            lambda: defaultdict(int)
        )

        # Process each jurisdiction's population data
        for jurisdiction_population in jurisdiction_populations:
            membership_status = geoid_to_membership[jurisdiction_population.geoid]

            if membership_status == schema.AgencyJurisdictionType.INCLUDE.value:
                # Include population data (add it)
                if jurisdiction_population.sex is not None:
                    bio_sex_populations_dict[jurisdiction_population.sex][
                        jurisdiction_population.year
                    ] += jurisdiction_population.population
                elif jurisdiction_population.race_ethnicity is not None:
                    race_eth_populations_dict[jurisdiction_population.race_ethnicity][
                        jurisdiction_population.year
                    ] += jurisdiction_population.population
            else:
                # Exclude population data (subtract it)
                if jurisdiction_population.sex is not None:
                    bio_sex_populations_dict[jurisdiction_population.sex][
                        jurisdiction_population.year
                    ] -= jurisdiction_population.population
                elif jurisdiction_population.race_ethnicity is not None:
                    race_eth_populations_dict[jurisdiction_population.race_ethnicity][
                        jurisdiction_population.year
                    ] -= jurisdiction_population.population

            # Structure the final dictionary for output
            populations_dict = {
                "race_and_ethnicity": {
                    key: dict(value) for key, value in race_eth_populations_dict.items()
                },
                "biological_sex": {
                    key: dict(value) for key, value in bio_sex_populations_dict.items()
                },
            }

        return populations_dict
