# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Data Access Object (DAO) with logic for accessing operations DB information from a SQL Database."""
import logging

import sqlalchemy
from more_itertools import one

from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


def stale_secondary_raw_data(region_code: str) -> bool:
    """Returns whether there is stale raw data in SECONDARY, as defined by:
    a) there exist non-invalidated files that have been added to PRIMARY after the
    timestamp of the start of the current SECONDARY rerun, and
    b) One or more of those files does not exist in SECONDARY.
    """
    region_code_upper = region_code.upper()
    secondary_status_manager = DirectIngestInstanceStatusManager(
        region_code=region_code,
        ingest_instance=DirectIngestInstance.SECONDARY,
    )
    secondary_rerun_start_timestamp = (
        secondary_status_manager.get_current_ingest_rerun_start_timestamp()
    )

    if not secondary_rerun_start_timestamp:
        logging.info(
            "[stale_secondary_raw_data] Could not locate the start of a secondary rerun. "
            "Raw data in secondary cannot be stale. Returning."
        )
        return False

    database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
    with SessionFactory.using_database(database_key) as session:
        logging.info(
            "[stale_secondary_raw_data] Executing query of direct_ingest_raw_file_metadata to determine if"
            "there are stale raw files in SECONDARY. Secondary rerun start timestamp is: %s",
            secondary_rerun_start_timestamp,
        )
        query = f"""
        WITH primary_raw_data AS (
            SELECT * 
            FROM direct_ingest_raw_file_metadata 
            WHERE raw_data_instance = 'PRIMARY' 
            AND region_code = '{region_code_upper}'
            AND is_invalidated IS False
        ), 
        secondary_raw_data AS (
            SELECT * 
            FROM direct_ingest_raw_file_metadata 
            WHERE raw_data_instance = 'SECONDARY' 
            AND region_code = '{region_code_upper}'
            AND is_invalidated IS False
        ) 
        SELECT COUNT(*)
        FROM primary_raw_data
        LEFT OUTER JOIN secondary_raw_data 
        ON primary_raw_data.normalized_file_name=secondary_raw_data.normalized_file_name  
        WHERE secondary_raw_data.normalized_file_name IS NULL 
        AND primary_raw_data.file_discovery_time > '{secondary_rerun_start_timestamp}';
        """
        results = session.execute(sqlalchemy.text(query))
        count = one(results)[0]
        is_stale = count > 0
        if is_stale:
            logging.info(
                "[%s][stale_secondary_raw_data] Found non-invalidated raw files in PRIMARY that were discovered "
                "after=[%s], indicating stale raw data in SECONDARY.",
                region_code,
                secondary_rerun_start_timestamp,
            )
        return is_stale
