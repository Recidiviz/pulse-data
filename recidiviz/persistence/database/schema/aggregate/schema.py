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
# ============================================================================

"""Define the ORM schema objects that map directly to the database,
for aggregate-level entities.

The below schema uses only generic SQLAlchemy types, and therefore should be
portable between database implementations.
"""

from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    Enum,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import validates

from recidiviz.common.constants.aggregate import enum_canonical_strings as enum_strings
from recidiviz.persistence.database.base_schema import JailsBase

from recidiviz.persistence.database.schema.shared_enums import (
    gender,
    race,
    ethnicity,
)

# SQLAlchemy enums. Created separately from the tables so they can be shared
# between the master and historical tables for each entity.

time_granularity = Enum(
    enum_strings.daily_granularity,
    enum_strings.weekly_granularity,
    enum_strings.monthly_granularity,
    enum_strings.quarterly_granularity,
    enum_strings.yearly_granularity,
    name="time_granularity",
)

# Note that we generally don't aggregate any fields in the schemas below.  The
# fields are a one to one mapping from the column names in the PDF tables from
# the aggregate reports.  Any aggregation will happen later in the post
# processing.  The exception is total_population which some states did not
# include a column for, but could be calculated from multiple columns.  It is
# an important enough field that we add it as a field.


class _AggregateTableMixin:
    """A mixin which defines common fields between all Aggregate Tables."""

    # Consider this class a mixin and only allow instantiating subclasses
    def __new__(cls, *_, **__):
        if cls is _AggregateTableMixin:
            raise Exception("_AggregateTableMixin cannot be instantiated")
        return super().__new__(cls)

    # Use a synthetic primary key and enforce uniqueness over a set of columns
    # (instead of setting these columns as a MultiColumn Primary Key) to allow
    # each row to be referenced by an int directly.
    record_id = Column(Integer, primary_key=True)

    # TODO(#1396): Add JID to aggregate reports
    fips = Column(String(5), nullable=False)

    report_date = Column(Date, nullable=False)

    # The time range that the reported statistics are aggregated over
    aggregation_window = Column(time_granularity, nullable=False)

    # The expected time between snapshots of data
    report_frequency = Column(time_granularity, nullable=False)

    @validates("fips")
    def validate_fips(self, _, fips: str) -> str:
        if len(fips) != 5:
            raise ValueError(
                "FIPS code invalid length: {} characters, should be 5".format(len(fips))
            )
        return fips


class CaFacilityAggregate(JailsBase, _AggregateTableMixin):
    """CA state-provided aggregate statistics."""

    __tablename__ = "ca_facility_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="ca_facility_aggregate_fips_length_check"
        ),
    )

    jurisdiction_name = Column(String(255))
    facility_name = Column(String(255), nullable=False)
    average_daily_population = Column(Integer)
    unsentenced_male_adp = Column(Integer)
    unsentenced_female_adp = Column(Integer)
    sentenced_male_adp = Column(Integer)
    sentenced_female_adp = Column(Integer)


class FlCountyAggregate(JailsBase, _AggregateTableMixin):
    """FL state-provided aggregate statistics."""

    __tablename__ = "fl_county_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="fl_county_aggregate_fips_length_check"
        ),
    )

    county_name = Column(String(255), nullable=False)
    county_population = Column(Integer)
    average_daily_population = Column(Integer)

    # If a county fails to send updated statistics to FL State, date_reported
    # will be set with the last time valid data was add to this report.
    date_reported = Column(Date)


class FlFacilityAggregate(JailsBase, _AggregateTableMixin):
    """FL state-provided pretrial aggregate statistics.

    Note: This 2nd FL database table is special because FL reports contain a 2nd
    table for Pretrial information by Facility.
    """

    __tablename__ = "fl_facility_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="fl_facility_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)
    average_daily_population = Column(Integer)
    number_felony_pretrial = Column(Integer)
    number_misdemeanor_pretrial = Column(Integer)


class GaCountyAggregate(JailsBase, _AggregateTableMixin):
    """GA state-provided aggregate statistics."""

    __tablename__ = "ga_county_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="ga_county_aggregate_fips_length_check"
        ),
    )

    county_name = Column(String(255), nullable=False)
    total_number_of_inmates_in_jail = Column(Integer)
    jail_capacity = Column(Integer)

    number_of_inmates_sentenced_to_state = Column(Integer)
    number_of_inmates_awaiting_trial = Column(Integer)  # Pretrial
    number_of_inmates_serving_county_sentence = Column(Integer)  # Sentenced
    number_of_other_inmates = Column(Integer)


class HiFacilityAggregate(JailsBase, _AggregateTableMixin):
    """HI state-provided aggregate statistics."""

    __tablename__ = "hi_facility_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="hi_facility_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)

    design_bed_capacity = Column(Integer)
    operation_bed_capacity = Column(Integer)

    total_population = Column(Integer)
    male_population = Column(Integer)
    female_population = Column(Integer)

    sentenced_felony_male_population = Column(Integer)
    sentenced_felony_female_population = Column(Integer)

    sentenced_felony_probation_male_population = Column(Integer)
    sentenced_felony_probation_female_population = Column(Integer)

    sentenced_misdemeanor_male_population = Column(Integer)
    sentenced_misdemeanor_female_population = Column(Integer)

    pretrial_felony_male_population = Column(Integer)
    pretrial_felony_female_population = Column(Integer)

    pretrial_misdemeanor_male_population = Column(Integer)
    pretrial_misdemeanor_female_population = Column(Integer)

    held_for_other_jurisdiction_male_population = Column(Integer)
    held_for_other_jurisdiction_female_population = Column(Integer)

    parole_violation_male_population = Column(Integer)
    parole_violation_female_population = Column(Integer)

    probation_violation_male_population = Column(Integer)
    probation_violation_female_population = Column(Integer)


class KyFacilityAggregate(JailsBase, _AggregateTableMixin):
    """KY state-provided aggregate statistics."""

    __tablename__ = "ky_facility_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="ky_facility_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)

    total_jail_beds = Column(Integer)
    reported_population = Column(Integer)

    male_population = Column(Integer)
    female_population = Column(Integer)

    class_d_male_population = Column(Integer)
    class_d_female_population = Column(Integer)

    community_custody_male_population = Column(Integer)
    community_custody_female_population = Column(Integer)

    alternative_sentence_male_population = Column(Integer)
    alternative_sentence_female_population = Column(Integer)

    controlled_intake_male_population = Column(Integer)
    controlled_intake_female_population = Column(Integer)

    parole_violators_male_population = Column(Integer)
    parole_violators_female_population = Column(Integer)

    federal_male_population = Column(Integer)
    federal_female_population = Column(Integer)


class NyFacilityAggregate(JailsBase, _AggregateTableMixin):
    """NY state-provided aggregate statistics."""

    __tablename__ = "ny_facility_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="ny_facility_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)

    census = Column(Integer)  # `In House` - `Boarded In` + `Boarded Out`
    in_house = Column(Integer)  # ADP of people assigned to facility
    boarded_in = Column(Integer)  # This is held_for_other_jurisdiction_adp
    boarded_out = Column(Integer)  # sent_to_other_jurisdiction_adp

    sentenced = Column(Integer)
    civil = Column(Integer)
    federal = Column(Integer)
    technical_parole_violators = Column(Integer)
    state_readies = Column(Integer)
    other_unsentenced = Column(Integer)


class TxCountyAggregate(JailsBase, _AggregateTableMixin):
    """TX state-provided aggregate statistics."""

    __tablename__ = "tx_county_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="tx_county_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)

    pretrial_felons = Column(Integer)

    # These 2 added are 'sentenced' as defined by Vera
    convicted_felons = Column(Integer)
    convicted_felons_sentenced_to_county_jail = Column(Integer)

    parole_violators = Column(Integer)
    parole_violators_with_new_charge = Column(Integer)

    pretrial_misdemeanor = Column(Integer)
    convicted_misdemeanor = Column(Integer)

    bench_warrants = Column(Integer)

    federal = Column(Integer)
    pretrial_sjf = Column(Integer)
    convicted_sjf_sentenced_to_county_jail = Column(Integer)
    convicted_sjf_sentenced_to_state_jail = Column(Integer)

    # We ignore Total Local, since that's the sum of above
    total_contract = Column(Integer)  # This is held_for_other_population
    total_population = Column(Integer)  # This is Total Population

    total_other = Column(Integer)

    total_capacity = Column(Integer)
    available_beds = Column(Integer)


class DcFacilityAggregate(JailsBase, _AggregateTableMixin):
    """DC state-provided aggregate statistics."""

    __tablename__ = "dc_facility_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="dc_facility_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)

    total_population = Column(Integer)
    male_population = Column(Integer)
    female_population = Column(Integer)

    stsf_male_population = Column(Integer)
    stsf_female_population = Column(Integer)

    usms_gb_male_population = Column(Integer)
    usms_gb_female_population = Column(Integer)

    juvenile_male_population = Column(Integer)
    juvenile_female_population = Column(Integer)


class PaFacilityPopAggregate(JailsBase, _AggregateTableMixin):
    """PA state-provided aggregate population statistics."""

    __tablename__ = "pa_facility_pop_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="pa_facility_pop_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)
    bed_capacity = Column(Integer)
    work_release_community_corrections_beds = Column(Integer)
    in_house_adp = Column(Integer)
    housed_elsewhere_adp = Column(Integer)
    work_release_adp = Column(Integer)
    admissions = Column(Integer)
    discharge = Column(Integer)


class PaCountyPreSentencedAggregate(JailsBase, _AggregateTableMixin):
    """PA state-provided pre-sentenced statistics."""

    __tablename__ = "pa_county_pre_sentenced_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5",
            name="pa_county_pre_sentenced_aggregate_fips_length_check",
        ),
    )

    county_name = Column(String(255), nullable=False)
    pre_sentenced_population = Column(Integer)


class TnFacilityAggregate(JailsBase, _AggregateTableMixin):
    """TN state-provided aggregate population statistics."""

    __tablename__ = "tn_facility_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="tn_facility_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)
    tdoc_backup_population = Column(Integer)
    local_felons_population = Column(Integer)
    other_convicted_felons_population = Column(Integer)
    federal_and_other_population = Column(Integer)
    convicted_misdemeanor_population = Column(Integer)
    pretrial_felony_population = Column(Integer)
    pretrial_misdemeanor_population = Column(Integer)
    total_jail_population = Column(Integer)
    total_beds = Column(Integer)


class TnFacilityFemaleAggregate(JailsBase, _AggregateTableMixin):
    """TN state-provided aggregate population statistics."""

    __tablename__ = "tn_facility_female_aggregate"
    __table_args__ = (
        UniqueConstraint("fips", "facility_name", "report_date", "aggregation_window"),
        CheckConstraint(
            "LENGTH(fips) = 5", name="tn_facility_aggregate_fips_length_check"
        ),
    )

    facility_name = Column(String(255), nullable=False)
    tdoc_backup_population = Column(Integer)
    local_felons_population = Column(Integer)
    other_convicted_felons_population = Column(Integer)
    federal_and_other_population = Column(Integer)
    convicted_misdemeanor_population = Column(Integer)
    pretrial_felony_population = Column(Integer)
    pretrial_misdemeanor_population = Column(Integer)
    female_jail_population = Column(Integer)
    female_beds = Column(Integer)


class SingleCountAggregate(JailsBase):
    __tablename__ = "single_count_aggregate"

    __table_args__ = (
        UniqueConstraint("jid", "date", "ethnicity", "gender", "race"),
        CheckConstraint("LENGTH(jid) = 8", name="single_count_jid_length_check"),
    )

    record_id = Column(Integer, primary_key=True)
    jid = Column(String(8), nullable=False)
    date = Column(Date, nullable=False)
    ethnicity = Column(ethnicity, nullable=True)
    gender = Column(gender, nullable=True)
    race = Column(race, nullable=True)
    count = Column(Integer, nullable=False)
