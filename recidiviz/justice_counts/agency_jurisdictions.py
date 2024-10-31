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
from typing import Dict, Hashable, List, Optional

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.justice_counts import schema


class AgencyJurisdictionInterface:
    """Contains methods for setting and getting AgencyJurisdictions."""

    @staticmethod
    def create_or_update_agency_jurisdictions(
        session: Session,
        agency_id: int,
        included_jurisdiction_ids: List[str],
        excluded_jurisdiction_ids: List[str],
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
        for jurisdiction_id in included_jurisdiction_ids:
            insert_statement = insert(schema.AgencyJurisdiction).values(
                source_id=agency_id,
                membership=schema.AgencyJurisdictionType.INCLUDE.value,
                jurisdiction_id=jurisdiction_id,
            )
            session.execute(insert_statement)
        # now, insert new excluded rows
        for jurisdiction_id in excluded_jurisdiction_ids:
            insert_statement = insert(schema.AgencyJurisdiction).values(
                source_id=agency_id,
                membership=schema.AgencyJurisdictionType.EXCLUDE.value,
                jurisdiction_id=jurisdiction_id,
            )
            session.execute(insert_statement)

    @staticmethod
    def to_json(
        session: Session,
        agency_id: int,
        is_v2: bool = False,
        fips_code_to_geoid: Optional[Dict[str, str]] = None,
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
                included_ids.append(jurisdiction.jurisdiction_id)
            else:
                excluded_ids.append(jurisdiction.jurisdiction_id)

        if is_v2 is True and fips_code_to_geoid is not None:
            included_ids = [
                fips_code_to_geoid.get(id.rstrip("0")) for id in included_ids
            ]
            excluded_ids = [
                fips_code_to_geoid.get(id.rstrip("0")) for id in excluded_ids
            ]

        return {
            "included": included_ids,
            "excluded": excluded_ids,
        }

    @staticmethod
    def get_agency_population(
        agency: schema.Agency,
    ) -> Dict[str, Dict[Hashable, int]]:
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
        state_code = StateCode(agency.state_code.upper())
        state = state_code.get_state()
        state_name = state.name

        populations_dict: Dict[str, Dict[Hashable, int]] = {}

        # Read the CSVs into a DataFrame
        race_eth_df = pd.read_csv(
            "./recidiviz/justice_counts/data_sets/state_adult_pop_by_race_eth.csv"
        )
        bio_sex_df = pd.read_csv(
            "./recidiviz/justice_counts/data_sets/state_adult_pop_by_sex.csv"
        )

        # Filter data by state name
        race_eth_df_filtered = race_eth_df[race_eth_df["state_name"] == state_name]
        bio_sex_df_filtered = bio_sex_df[bio_sex_df["state_name"] == state_name]

        # Group by race_eth and year, aggregate, and convert to nested dictionary
        race_eth_populations_dict = (
            race_eth_df_filtered.groupby(["race_eth", "year"])["n"]
            .first()  # Get the first non-null entry from sheet (there is only one row per race_eth, year so this is safe)
            .unstack()  # Pivot 'year' into columns
            .to_dict(orient="index")
        )

        # Group by biological sex and year, aggregate, and convert to nested dictionary
        bio_sex_populations_dict = (
            bio_sex_df_filtered.groupby(["sex", "year"])["n"]
            .first()
            .unstack()
            .to_dict(orient="index")
        )

        # Structure the final dictionary
        populations_dict = {
            "race_and_ethnicity": race_eth_populations_dict,
            "biological_sex": bio_sex_populations_dict,
        }

        return populations_dict
