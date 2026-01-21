# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""Define the ORM schema objects that map directly to the database, for Public Pathways related entities.
"""
from typing import Any, Dict

from sqlalchemy import BigInteger, Column, Date, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeMeta, declarative_base

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.shared_pathways.schema_helpers import (
    build_covered_indexes,
)

PublicPathwaysBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="PublicPathwaysBase"
)

# Key to pass to __table_args__["info"] to keep the table up-to-date with alembic migrations.
RUN_MIGRATIONS = "run_migrations"


class MetricMetadata(PublicPathwaysBase):
    """Table storing metadata about our metrics."""

    __tablename__ = "metric_metadata"
    # Other tables are deleted/recreated at import time, but this table needs to be kept up to date
    # via alembic migrations.
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    metric = Column(String, primary_key=True, nullable=False)
    last_updated = Column(Date, nullable=False)
    facility_id_name_map = Column(JSONB, nullable=True)
    gender_id_name_map = Column(JSONB, nullable=True)
    race_id_name_map = Column(JSONB, nullable=True)
    dynamic_filter_options = Column(JSONB, nullable=True)

    def to_json(self) -> Dict[str, Any]:
        json_dict = {}
        if self.metric:
            json_dict["metric"] = self.metric
        if self.last_updated:
            json_dict["lastUpdated"] = self.last_updated.isoformat()
        if self.facility_id_name_map:
            json_dict["facilityIdNameMap"] = self.facility_id_name_map
        if self.gender_id_name_map:
            json_dict["genderIdNameMap"] = self.gender_id_name_map
        if self.race_id_name_map:
            json_dict["raceIdNameMap"] = self.race_id_name_map
        if self.dynamic_filter_options:
            json_dict["dynamicFilterOptions"] = self.dynamic_filter_options
        return json_dict


class PublicPrisonPopulationOverTime(PublicPathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.public_pathways.event_level.prison_population_over_time`
    """

    __tablename__ = "prison_population_over_time"
    # Adds covered index for time series view
    __table_args__ = (
        Index(
            "prison_population_over_time_pk",
            "person_id",
            "date_in_population",
            "age_group",
            "facility",
            "gender",
            "sex",
            "race",
            unique=True,
        ),
        Index(
            "prison_population_over_time_time_series",
            "date_in_population",
            postgresql_include=[
                "person_id",
                "age_group",
                "gender",
                "sex",
                "facility",
                "race",
            ],
        ),
        # Allows fast execution of select min(date_in_population)
        Index(
            "prison_population_over_time_watermark",
            "time_period",
            "date_in_population",
        ),
    )

    state_code = Column(String, primary_key=True, nullable=False)
    # Date in population
    date_in_population = Column(Date, primary_key=True, nullable=False)
    # Bin of when the person was in population (see recidiviz.calculator.query.bq_utils.get_binned_time_period_months)
    time_period = Column(String, primary_key=True, nullable=False)
    # Person ID for the session. BigInt has faster sorting/grouping than String
    person_id = Column(BigInteger, primary_key=True, nullable=False)
    # Current age group of the person (see recidiviz.calculator.query.bq_utils.add_age_groups)
    age_group = Column(String, primary_key=True, nullable=True)
    # Facility the person resides
    facility = Column(String, primary_key=True, nullable=True)
    # Gender of the person
    gender = Column(String, primary_key=True, nullable=True)
    # Sex of the person
    sex = Column(String, primary_key=True, nullable=True)
    # Race of the person
    race = Column(String, primary_key=True, nullable=True)


class PublicPrisonPopulationByDimension(PublicPathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.public_pathways.event_level.prison_population_by_dimension`
    """

    __tablename__ = "prison_population_by_dimension"

    # Adds covered indexes for groupable columns and includes other columns that may be used in the same query
    __table_args__ = (
        Index(
            "prison_population_by_dimension_pk",
            "person_id",
            "age_group",
            "facility",
            "gender",
            "sex",
            "race",
            unique=True,
        ),
        *build_covered_indexes(
            index_base_name="prison_population_by_dimension",
            dimensions=[
                "age_group",
                "facility",
                "gender",
                "sex",
                "race",
            ],
            includes=["person_id"],
        ),
    )

    state_code = Column(String, primary_key=True, nullable=False)

    # Person ID for the session. BigInt has faster sorting/grouping than String
    person_id = Column(BigInteger, primary_key=True, nullable=False)
    # Current age group of the person (see recidiviz.calculator.query.bq_utils.add_age_groups)
    age_group = Column(String, primary_key=True, nullable=False)
    # Facility the person is in
    facility = Column(String, primary_key=True, nullable=False)
    # Gender of the person
    gender = Column(String, primary_key=True, nullable=False)
    # Sex of the person
    sex = Column(String, primary_key=True, nullable=False)
    # Race of the person
    race = Column(String, primary_key=True, nullable=False)
