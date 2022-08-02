# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Define the ORM schema objects that map directly to the database, for Pathways related entities.
"""
from typing import List, Optional

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Column,
    Date,
    Float,
    Index,
    Integer,
    SmallInteger,
    String,
    func,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeMeta, declarative_base

# Defines the base class for all table classes in the pathways schema.
# For actual schema definitions, see /pathways/schema.py.
from sqlalchemy.sql.ddl import DDLElement

from recidiviz.persistence.database.database_entity import DatabaseEntity

PathwaysBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="PathwaysBase"
)


def build_covered_indexes(
    *,
    index_base_name: str,
    dimensions: List[str],
    includes: Optional[List[str]] = None,
) -> List[Index]:
    dimensions = sorted(dimensions)
    dimension_set = set(dimensions)
    includes = [] if includes is None else includes
    return [
        Index(
            f"{index_base_name}_{dimension}",
            dimension,
            postgresql_include=[
                *sorted(list(dimension_set - set([dimension]))),
                *includes,
            ],
        )
        for dimension in dimensions
    ]


class TransitionsOverTimeMixin:
    transition_date: Column

    @hybrid_property
    def month_timestamp(self) -> DDLElement:
        return func.date_trunc("month", func.cast(self.transition_date, TIMESTAMP))


class LibertyToPrisonTransitions(PathwaysBase, TransitionsOverTimeMixin):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.liberty_to_prison_transitions`
    """

    __tablename__ = "liberty_to_prison_transitions"

    # Date that the transition occurred
    transition_date = Column(Date, primary_key=True, nullable=False)
    # Denormalized transition year
    year = Column(SmallInteger, nullable=False)
    # Denormalized transition month
    month = Column(SmallInteger, nullable=False)
    # Bin of when the transition occurred (see recidiviz.calculator.query.bq_utils.get_binned_time_period_months)
    time_period = Column(String, nullable=True)
    # Person ID for the transition
    person_id = Column(BigInteger, primary_key=True, nullable=False)
    # Age group of the person when the transition occurred (see recidiviz.calculator.query.bq_utils.add_age_groups)
    age_group = Column(String, nullable=True)
    # Gender of the person
    gender = Column(String, nullable=True)
    # `prioritized_race_or_ethnicity` of the person
    race = Column(String, nullable=True)
    # District the transition occurred in
    judicial_district = Column(String, nullable=False)
    # Total number of months the person was previously incarcerated
    prior_length_of_incarceration = Column(String, nullable=False)
    # State code for the transition
    state_code = Column(String, nullable=False)


class PrisonPopulationOverTime(PathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_over_time`"""

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
            "admission_reason",
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
                "facility",
                "admission_reason",
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
    # Admission reason
    admission_reason = Column(String, primary_key=True, nullable=True)
    # Race of the person
    race = Column(String, primary_key=True, nullable=True)


class PrisonPopulationByDimension(PathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_by_dimension`"""

    __tablename__ = "prison_population_by_dimension"

    # Adds covered indexes for groupable columns and includes other columns that may be used in the same query
    __table_args__ = (
        Index(
            "prison_population_by_dimension_pk",
            "person_id",
            "gender",
            "admission_reason",
            "facility",
            "age_group",
            "race",
            unique=True,
        ),
        *build_covered_indexes(
            index_base_name="prison_population_by_dimension",
            dimensions=["age_group", "facility", "gender", "admission_reason", "race"],
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
    # Admission reason
    admission_reason = Column(String, primary_key=True, nullable=False)
    # Race of the person
    race = Column(String, primary_key=True, nullable=False)
    # Binned length of incarceration in months
    length_of_stay = Column(String, nullable=True)


class PrisonPopulationProjection(PathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_projection`"""

    __tablename__ = "prison_population_projection"

    # State
    state_code = Column(String, primary_key=True, nullable=False)
    # Year of the historical / projected count
    year = Column(SmallInteger, primary_key=True, nullable=False)
    # Month of the historical / projected count
    month = Column(SmallInteger, primary_key=True, nullable=False)
    # HISTORICAL or BASELINE (projection)
    simulation_tag = Column(String, primary_key=True, nullable=False)
    # Gender of the population
    gender = Column(String, primary_key=True, nullable=True)
    # Legal status of the population
    admission_reason = Column(String, primary_key=True, nullable=True)
    # Projected population
    total_population = Column(Float, nullable=False)
    # Min error
    total_population_min = Column(Float, nullable=False)
    # Max error
    total_population_max = Column(Float, nullable=False)


class PrisonPopulationPersonLevel(PathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_person_level"""

    __tablename__ = "prison_population_person_level"

    state_code = Column(String, primary_key=True, nullable=False)
    # Person's external ID
    state_id = Column(String, primary_key=True, nullable=False)
    # Full name of the person
    full_name = Column(String)
    # Current age of the person
    age = Column(String)
    gender = Column(String)
    # Facility the person resides in
    facility = Column(String)
    # Admission reason
    admission_reason = Column(String)
    # Age group of the person(see recidiviz.calculator.query.bq_utils.add_age_groups)
    age_group = Column(String)
    # `prioritized_race_or_ethnicity` of the person
    race = Column(String)


class PrisonToSupervisionTransitions(PathwaysBase, TransitionsOverTimeMixin):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_to_supervision_transitions`
    """

    __tablename__ = "prison_to_supervision_transitions"

    # Date that the transition occurred
    transition_date = Column(Date, primary_key=True, nullable=False)
    # Denormalized transition year
    year = Column(SmallInteger, nullable=False)
    # Denormalized transition month
    month = Column(SmallInteger, nullable=False)
    # Person ID for the transition
    person_id = Column(BigInteger, primary_key=True, nullable=False)
    # Age group of the person when the transition occurred (see recidiviz.calculator.query.bq_utils.add_age_groups)
    age_group = Column(String, nullable=True)
    # Age of the person
    age = Column(Integer, nullable=True)
    # Gender of the person
    gender = Column(String, nullable=True)
    # `prioritized_race_or_ethnicity` of the person
    race = Column(String, nullable=True)
    # Facility the transition occurred from
    facility = Column(String, nullable=True)
    # Full name of the person
    full_name = Column(String, nullable=True)
    # Bin of when the transition occurred (see recidiviz.calculator.query.bq_utils.get_binned_time_period_months)
    time_period = Column(String, nullable=True)
    # External ID of the person
    state_id = Column(String, nullable=True)
    # State code for the transition
    state_code = Column(String, nullable=False)


class SupervisionPopulationOverTime(PathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population_over_time`"""

    __tablename__ = "supervision_population_over_time"
    # Adds covered index for time series view
    __table_args__ = (
        Index(
            "supervision_population_over_time_pk",
            "date_in_population",
            "supervision_district",
            "supervision_level",
            "race",
            "person_id",
            unique=True,
        ),
        Index(
            "supervision_population_over_time_time_series",
            "date_in_population",
            postgresql_include=[
                "person_id",
                "supervision_district",
                "supervision_level",
                "race",
            ],
        ),
        # Allows fast execution of select min(date_in_population)
        Index(
            "supervision_population_over_time_watermark",
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
    # District that the person is in
    supervision_district = Column(String, primary_key=True, nullable=True)
    # Supervision level of the person
    supervision_level = Column(String, primary_key=True, nullable=True)
    # Race of the person
    race = Column(String, primary_key=True, nullable=True)


class SupervisionPopulationByDimension(PathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population_by_dimension`"""

    __tablename__ = "supervision_population_by_dimension"

    # Adds covered indexes for groupable columns and includes other columns that may be used in the same query
    __table_args__ = (
        Index(
            "supervision_population_by_dimension_pk",
            "person_id",
            "supervision_district",
            "supervision_level",
            "race",
            unique=True,
        ),
        *build_covered_indexes(
            index_base_name="supervision_population_by_dimension",
            dimensions=["supervision_district", "supervision_level", "race"],
            includes=["person_id"],
        ),
    )

    state_code = Column(String, primary_key=True, nullable=False)

    # Person ID for the session. BigInt has faster sorting/grouping than String
    person_id = Column(BigInteger, primary_key=True, nullable=False)
    # District that the person is in
    supervision_district = Column(String, primary_key=True, nullable=True)
    # Supervision level of the person
    supervision_level = Column(String, primary_key=True, nullable=True)
    # Race of the person
    race = Column(String, primary_key=True, nullable=True)


class SupervisionPopulationProjection(PathwaysBase):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population_projection`"""

    __tablename__ = "supervision_population_projection"

    # State
    state_code = Column(String, primary_key=True, nullable=False)
    # Year of the historical / projected count
    year = Column(SmallInteger, primary_key=True, nullable=False)
    # Month of the historical / projected count
    month = Column(SmallInteger, primary_key=True, nullable=False)
    # HISTORICAL or BASELINE (projection)
    simulation_tag = Column(String, primary_key=True, nullable=False)
    # Gender of the population
    gender = Column(String, primary_key=True, nullable=True)
    # Legal status of the population
    admission_reason = Column(String, primary_key=True, nullable=True)
    # Projected population
    total_population = Column(Float, nullable=False)
    # Min error
    total_population_min = Column(Float, nullable=False)
    # Max error
    total_population_max = Column(Float, nullable=False)


class SupervisionToLibertyTransitions(PathwaysBase, TransitionsOverTimeMixin):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_to_liberty_transitions`
    """

    __tablename__ = "supervision_to_liberty_transitions"

    # Date that the transition occurred
    transition_date = Column(Date, primary_key=True, nullable=False)
    # Denormalized transition year
    year = Column(SmallInteger, nullable=False)
    # Denormalized transition month
    month = Column(SmallInteger, nullable=False)
    # Person ID for the transition
    person_id = Column(BigInteger, primary_key=True, nullable=False)
    # Age group of the person when the transition occurred (see recidiviz.calculator.query.bq_utils.add_age_groups)
    age_group = Column(String, nullable=True)
    # Age of the person
    age = Column(Integer, nullable=True)
    # Gender of the person
    gender = Column(String, nullable=True)
    # `prioritized_race_or_ethnicity` of the person
    race = Column(String, nullable=True)
    # Type of supervision the person was under
    supervision_type = Column(String, nullable=True)
    # Level of supervision the person was under
    supervision_level = Column(String, nullable=True)
    # District the person was supervised in
    supervision_district = Column(String, nullable=True)
    # ID of the person's supervising officer at time of release
    supervising_officer = Column(String, nullable=True)
    # When the ending supervision period started
    supervision_start_date = Column(Date, nullable=True)
    # Bin of when the transition occurred (see recidiviz.calculator.query.bq_utils.get_binned_time_period_months)
    time_period = Column(String, nullable=True)
    # Binned length of supervision in months
    length_of_stay = Column(String, nullable=True)
    # State code for the transition
    state_code = Column(String, nullable=False)


class SupervisionToPrisonTransitions(PathwaysBase, TransitionsOverTimeMixin):
    """ETL data imported from
    `recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_to_prison_transitions`
    """

    __tablename__ = "supervision_to_prison_transitions"

    # Date that the transition occurred
    transition_date = Column(Date, primary_key=True, nullable=False)
    # Denormalized transition year
    year = Column(SmallInteger, nullable=False)
    # Denormalized transition month
    month = Column(SmallInteger, nullable=False)
    # Person ID for the transition
    person_id = Column(BigInteger, primary_key=True, nullable=False)
    # Type of supervision the person was under
    supervision_type = Column(String, nullable=True)
    # Level of supervision the person was under
    supervision_level = Column(String, nullable=True)
    # Age group of the person when the transition occurred (see recidiviz.calculator.query.bq_utils.add_age_groups)
    age_group = Column(String, nullable=True)
    # Age of the person
    age = Column(Integer, nullable=True)
    # Gender of the person
    gender = Column(String, nullable=True)
    # `prioritized_race_or_ethnicity` of the person
    race = Column(String, nullable=True)
    # ID of the person's supervising officer at time of release
    supervising_officer = Column(String, nullable=True)
    # When the ending supervision period started
    supervision_start_date = Column(Date, nullable=True)
    # District that the person is in
    supervision_district = Column(String, nullable=True)
    # Bin of when the transition occurred (see recidiviz.calculator.query.bq_utils.get_binned_time_period_months)
    time_period = Column(String, nullable=True)
    # Binned length of supervision in months
    length_of_stay = Column(String, nullable=True)
    # State code for the transition
    state_code = Column(String, nullable=False)
