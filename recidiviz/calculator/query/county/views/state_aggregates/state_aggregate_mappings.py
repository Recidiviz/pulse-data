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
"""Defines mappings from the database to BQ combined aggregate views."""


from recidiviz.calculator.query.county.views.state_aggregates.mappings import Mappings
from recidiviz.persistence.database.schema.aggregate.schema import (
    CaFacilityAggregate,
    FlFacilityAggregate,
    GaCountyAggregate,
    HiFacilityAggregate,
    KyFacilityAggregate,
    NyFacilityAggregate,
    TxCountyAggregate,
    PaFacilityPopAggregate,
    PaCountyPreSentencedAggregate,
    TnFacilityAggregate,
    TnFacilityFemaleAggregate,
)

MAPPINGS = set()

MAPPINGS.add(
    Mappings(
        fips=CaFacilityAggregate.fips,
        facility_name=CaFacilityAggregate.facility_name,
        report_date=CaFacilityAggregate.report_date,
        aggregation_window=CaFacilityAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=None,
        custodial=CaFacilityAggregate.average_daily_population,
        admissions=None,
        jurisdictional=None,
        male=CaFacilityAggregate.sentenced_male_adp
        + CaFacilityAggregate.unsentenced_male_adp,
        female=CaFacilityAggregate.sentenced_female_adp
        + CaFacilityAggregate.unsentenced_female_adp,
        sentenced=CaFacilityAggregate.sentenced_male_adp
        + CaFacilityAggregate.sentenced_female_adp,
        pretrial=CaFacilityAggregate.unsentenced_male_adp
        + CaFacilityAggregate.unsentenced_female_adp,
        felony=None,
        misdemeanor=None,
        parole_violators=None,
        probation_violators=None,
        technical_parole_violators=None,
        civil=None,
        held_for_doc=None,
        held_for_federal=None,
        total_held_for_other=None,
        held_elsewhere=None,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=CaFacilityAggregate.sentenced_male_adp,
        sentenced_female=CaFacilityAggregate.sentenced_female_adp,
        pretrial_male=CaFacilityAggregate.unsentenced_male_adp,
        pretrial_female=CaFacilityAggregate.unsentenced_female_adp,
        felony_male=None,
        felony_female=None,
        felony_pretrial=None,
        felony_sentenced=None,
        felony_pretrial_male=None,
        felony_pretrial_female=None,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=None,
        misdemeanor_sentenced=None,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=None,
        misdemeanor_pretrial_female=None,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=None,
        held_for_doc_female=None,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=None,
        total_held_for_other_female=None,
    )
)

MAPPINGS.add(
    Mappings(
        fips=FlFacilityAggregate.fips,
        facility_name=FlFacilityAggregate.facility_name,
        report_date=FlFacilityAggregate.report_date,
        aggregation_window=FlFacilityAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=None,
        custodial=FlFacilityAggregate.average_daily_population,
        admissions=None,
        jurisdictional=None,
        male=None,
        female=None,
        sentenced=None,
        pretrial=FlFacilityAggregate.number_felony_pretrial
        + FlFacilityAggregate.number_misdemeanor_pretrial,
        felony=None,
        misdemeanor=None,
        parole_violators=None,
        probation_violators=None,
        technical_parole_violators=None,
        civil=None,
        held_for_doc=None,
        held_for_federal=None,
        total_held_for_other=None,
        held_elsewhere=None,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=None,
        sentenced_female=None,
        pretrial_male=None,
        pretrial_female=None,
        felony_male=None,
        felony_female=None,
        felony_pretrial=FlFacilityAggregate.number_felony_pretrial,
        felony_sentenced=None,
        felony_pretrial_male=None,
        felony_pretrial_female=None,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=FlFacilityAggregate.number_misdemeanor_pretrial,
        misdemeanor_sentenced=None,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=None,
        misdemeanor_pretrial_female=None,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=None,
        held_for_doc_female=None,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=None,
        total_held_for_other_female=None,
    )
)

MAPPINGS.add(
    Mappings(
        fips=GaCountyAggregate.fips,
        facility_name=GaCountyAggregate.county_name,
        report_date=GaCountyAggregate.report_date,
        aggregation_window=GaCountyAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=GaCountyAggregate.jail_capacity,
        custodial=GaCountyAggregate.total_number_of_inmates_in_jail,
        admissions=None,
        jurisdictional=None,
        male=None,
        female=None,
        # We can't compute `sentenced` because summing `sentenced_to_state` and
        # `serving_county_sentence` discards people in `other` who are sentenced
        sentenced=None,
        pretrial=GaCountyAggregate.number_of_inmates_awaiting_trial,
        felony=None,
        misdemeanor=None,
        parole_violators=None,
        probation_violators=None,
        technical_parole_violators=None,
        civil=None,
        held_for_doc=GaCountyAggregate.number_of_inmates_sentenced_to_state,
        held_for_federal=None,
        total_held_for_other=None,
        held_elsewhere=None,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=None,
        sentenced_female=None,
        pretrial_male=None,
        pretrial_female=None,
        felony_male=None,
        felony_female=None,
        felony_pretrial=None,
        felony_sentenced=None,
        felony_pretrial_male=None,
        felony_pretrial_female=None,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=None,
        misdemeanor_sentenced=None,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=None,
        misdemeanor_pretrial_female=None,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=None,
        held_for_doc_female=None,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=None,
        total_held_for_other_female=None,
    )
)

MAPPINGS.add(
    Mappings(
        fips=HiFacilityAggregate.fips,
        facility_name=HiFacilityAggregate.facility_name,
        report_date=HiFacilityAggregate.report_date,
        aggregation_window=HiFacilityAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=HiFacilityAggregate.operation_bed_capacity,
        custodial=HiFacilityAggregate.total_population,
        admissions=None,
        jurisdictional=None,
        male=HiFacilityAggregate.male_population,
        female=HiFacilityAggregate.female_population,
        sentenced=HiFacilityAggregate.sentenced_felony_male_population
        + HiFacilityAggregate.sentenced_felony_female_population
        + HiFacilityAggregate.sentenced_felony_probation_male_population
        + HiFacilityAggregate.sentenced_felony_probation_female_population
        + HiFacilityAggregate.sentenced_misdemeanor_male_population
        + HiFacilityAggregate.sentenced_misdemeanor_female_population
        + HiFacilityAggregate.parole_violation_male_population
        + HiFacilityAggregate.parole_violation_female_population
        + HiFacilityAggregate.probation_violation_male_population
        + HiFacilityAggregate.probation_violation_female_population,
        pretrial=HiFacilityAggregate.pretrial_felony_male_population
        + HiFacilityAggregate.pretrial_felony_female_population
        + HiFacilityAggregate.pretrial_misdemeanor_male_population
        + HiFacilityAggregate.pretrial_misdemeanor_female_population,
        felony=HiFacilityAggregate.sentenced_felony_male_population
        + HiFacilityAggregate.sentenced_felony_female_population
        + HiFacilityAggregate.sentenced_felony_probation_male_population
        + HiFacilityAggregate.sentenced_felony_probation_female_population
        + HiFacilityAggregate.pretrial_felony_male_population
        + HiFacilityAggregate.pretrial_felony_female_population,
        misdemeanor=HiFacilityAggregate.sentenced_misdemeanor_male_population
        + HiFacilityAggregate.sentenced_misdemeanor_female_population
        + HiFacilityAggregate.pretrial_misdemeanor_male_population
        + HiFacilityAggregate.pretrial_misdemeanor_female_population,
        parole_violators=HiFacilityAggregate.parole_violation_male_population
        + HiFacilityAggregate.parole_violation_female_population,
        probation_violators=HiFacilityAggregate.probation_violation_male_population
        + HiFacilityAggregate.probation_violation_female_population,
        technical_parole_violators=None,
        civil=None,
        held_for_doc=None,
        held_for_federal=None,
        total_held_for_other=HiFacilityAggregate.held_for_other_jurisdiction_male_population
        + HiFacilityAggregate.held_for_other_jurisdiction_female_population,
        held_elsewhere=None,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=HiFacilityAggregate.sentenced_felony_male_population
        + HiFacilityAggregate.sentenced_felony_probation_male_population
        + HiFacilityAggregate.sentenced_misdemeanor_male_population
        + HiFacilityAggregate.parole_violation_male_population
        + HiFacilityAggregate.probation_violation_male_population,
        sentenced_female=HiFacilityAggregate.sentenced_felony_female_population
        + HiFacilityAggregate.sentenced_felony_probation_female_population
        + HiFacilityAggregate.sentenced_misdemeanor_female_population
        + HiFacilityAggregate.parole_violation_female_population
        + HiFacilityAggregate.probation_violation_female_population,
        pretrial_male=HiFacilityAggregate.pretrial_felony_male_population
        + HiFacilityAggregate.pretrial_misdemeanor_male_population,
        pretrial_female=HiFacilityAggregate.pretrial_felony_female_population
        + HiFacilityAggregate.pretrial_misdemeanor_female_population,
        # We can't compute any information on felony vs sentenced misdemeanor since
        # we don't know if probation_violation is a felony or a misdemeanor
        felony_male=None,
        felony_female=None,
        felony_pretrial=HiFacilityAggregate.pretrial_felony_male_population
        + HiFacilityAggregate.pretrial_felony_female_population,
        felony_sentenced=None,
        felony_pretrial_male=HiFacilityAggregate.pretrial_felony_male_population,
        felony_pretrial_female=HiFacilityAggregate.pretrial_felony_female_population,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=HiFacilityAggregate.pretrial_misdemeanor_male_population
        + HiFacilityAggregate.pretrial_misdemeanor_female_population,
        misdemeanor_sentenced=None,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=HiFacilityAggregate.pretrial_misdemeanor_male_population,
        misdemeanor_pretrial_female=HiFacilityAggregate.pretrial_misdemeanor_female_population,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=None,
        held_for_doc_female=None,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=HiFacilityAggregate.held_for_other_jurisdiction_male_population,
        total_held_for_other_female=HiFacilityAggregate.held_for_other_jurisdiction_female_population,
    )
)

MAPPINGS.add(
    Mappings(
        fips=KyFacilityAggregate.fips,
        facility_name=KyFacilityAggregate.facility_name,
        report_date=KyFacilityAggregate.report_date,
        aggregation_window=KyFacilityAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=KyFacilityAggregate.total_jail_beds,
        custodial=KyFacilityAggregate.reported_population,
        admissions=None,
        jurisdictional=None,
        male=KyFacilityAggregate.male_population,
        female=KyFacilityAggregate.female_population,
        sentenced=None,
        pretrial=None,
        felony=None,
        misdemeanor=None,
        parole_violators=KyFacilityAggregate.parole_violators_male_population
        + KyFacilityAggregate.parole_violators_female_population,
        probation_violators=None,
        technical_parole_violators=None,
        civil=None,
        held_for_doc=(
            KyFacilityAggregate.alternative_sentence_female_population
            + KyFacilityAggregate.alternative_sentence_male_population
            + KyFacilityAggregate.class_d_female_population
            + KyFacilityAggregate.class_d_male_population
            + KyFacilityAggregate.community_custody_female_population
            + KyFacilityAggregate.community_custody_male_population
            + KyFacilityAggregate.controlled_intake_female_population
            + KyFacilityAggregate.controlled_intake_male_population
            + KyFacilityAggregate.parole_violators_female_population
            + KyFacilityAggregate.parole_violators_male_population
        ),
        held_for_federal=KyFacilityAggregate.federal_male_population
        + KyFacilityAggregate.federal_female_population,
        total_held_for_other=None,
        held_elsewhere=None,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=None,
        sentenced_female=None,
        pretrial_male=None,
        pretrial_female=None,
        felony_male=None,
        felony_female=None,
        felony_pretrial=None,
        felony_sentenced=None,
        felony_pretrial_male=None,
        felony_pretrial_female=None,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=None,
        misdemeanor_sentenced=None,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=None,
        misdemeanor_pretrial_female=None,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=KyFacilityAggregate.parole_violators_male_population,
        parole_violators_female=KyFacilityAggregate.parole_violators_female_population,
        held_for_doc_male=(
            KyFacilityAggregate.alternative_sentence_male_population
            + KyFacilityAggregate.class_d_male_population
            + KyFacilityAggregate.community_custody_male_population
            + KyFacilityAggregate.controlled_intake_male_population
            + KyFacilityAggregate.parole_violators_male_population
        ),
        held_for_doc_female=(
            KyFacilityAggregate.alternative_sentence_female_population
            + KyFacilityAggregate.class_d_female_population
            + KyFacilityAggregate.community_custody_female_population
            + KyFacilityAggregate.controlled_intake_female_population
            + KyFacilityAggregate.parole_violators_female_population
        ),
        held_for_federal_male=KyFacilityAggregate.federal_male_population,
        held_for_federal_female=KyFacilityAggregate.federal_female_population,
        total_held_for_other_male=None,
        total_held_for_other_female=None,
    )
)

MAPPINGS.add(
    Mappings(
        fips=NyFacilityAggregate.fips,
        facility_name=NyFacilityAggregate.facility_name,
        report_date=NyFacilityAggregate.report_date,
        aggregation_window=NyFacilityAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=None,
        custodial=NyFacilityAggregate.in_house,
        admissions=None,
        jurisdictional=NyFacilityAggregate.census,
        male=None,
        female=None,
        sentenced=NyFacilityAggregate.sentenced,
        pretrial=NyFacilityAggregate.other_unsentenced,
        felony=None,
        misdemeanor=None,
        parole_violators=None,
        probation_violators=None,
        technical_parole_violators=NyFacilityAggregate.technical_parole_violators,
        civil=NyFacilityAggregate.civil,
        held_for_doc=NyFacilityAggregate.state_readies,
        held_for_federal=NyFacilityAggregate.federal,
        total_held_for_other=NyFacilityAggregate.boarded_in
        + NyFacilityAggregate.state_readies
        + NyFacilityAggregate.federal,
        held_elsewhere=NyFacilityAggregate.boarded_out,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=None,
        sentenced_female=None,
        pretrial_male=None,
        pretrial_female=None,
        felony_male=None,
        felony_female=None,
        felony_pretrial=None,
        felony_sentenced=None,
        felony_pretrial_male=None,
        felony_pretrial_female=None,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=None,
        misdemeanor_sentenced=None,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=None,
        misdemeanor_pretrial_female=None,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=None,
        held_for_doc_female=None,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=None,
        total_held_for_other_female=None,
    )
)

MAPPINGS.add(
    Mappings(
        fips=TxCountyAggregate.fips,
        facility_name=TxCountyAggregate.facility_name,
        report_date=TxCountyAggregate.report_date,
        aggregation_window=TxCountyAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=TxCountyAggregate.total_capacity,
        custodial=TxCountyAggregate.total_population,
        admissions=None,
        jurisdictional=None,
        male=None,
        female=None,
        sentenced=TxCountyAggregate.convicted_felons
        + TxCountyAggregate.convicted_felons_sentenced_to_county_jail
        + TxCountyAggregate.parole_violators
        + TxCountyAggregate.parole_violators_with_new_charge
        + TxCountyAggregate.convicted_misdemeanor
        + TxCountyAggregate.convicted_sjf_sentenced_to_county_jail
        + TxCountyAggregate.convicted_sjf_sentenced_to_state_jail,
        pretrial=TxCountyAggregate.pretrial_felons
        + TxCountyAggregate.pretrial_misdemeanor
        + TxCountyAggregate.pretrial_sjf,
        felony=TxCountyAggregate.pretrial_felons
        + TxCountyAggregate.convicted_felons
        + TxCountyAggregate.convicted_felons_sentenced_to_county_jail
        + TxCountyAggregate.pretrial_sjf
        + TxCountyAggregate.convicted_sjf_sentenced_to_county_jail
        + TxCountyAggregate.convicted_sjf_sentenced_to_state_jail,
        misdemeanor=TxCountyAggregate.pretrial_misdemeanor
        + TxCountyAggregate.convicted_misdemeanor,
        parole_violators=TxCountyAggregate.parole_violators
        + TxCountyAggregate.parole_violators_with_new_charge,
        probation_violators=None,
        technical_parole_violators=TxCountyAggregate.parole_violators
        - TxCountyAggregate.parole_violators_with_new_charge,
        civil=None,
        held_for_doc=None,
        held_for_federal=TxCountyAggregate.federal,
        total_held_for_other=TxCountyAggregate.total_other + TxCountyAggregate.federal,
        held_elsewhere=None,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=None,
        sentenced_female=None,
        pretrial_male=None,
        pretrial_female=None,
        felony_male=None,
        felony_female=None,
        felony_pretrial=TxCountyAggregate.pretrial_felons
        + TxCountyAggregate.pretrial_sjf,
        felony_sentenced=TxCountyAggregate.convicted_felons
        + TxCountyAggregate.convicted_felons_sentenced_to_county_jail
        + TxCountyAggregate.convicted_sjf_sentenced_to_county_jail
        + TxCountyAggregate.convicted_sjf_sentenced_to_state_jail,
        felony_pretrial_male=None,
        felony_pretrial_female=None,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=TxCountyAggregate.pretrial_misdemeanor,
        misdemeanor_sentenced=TxCountyAggregate.convicted_misdemeanor,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=None,
        misdemeanor_pretrial_female=None,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=None,
        held_for_doc_female=None,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=None,
        total_held_for_other_female=None,
    )
)

MAPPINGS.add(
    Mappings(
        fips=PaFacilityPopAggregate.fips,
        facility_name=PaFacilityPopAggregate.facility_name,
        report_date=PaFacilityPopAggregate.report_date,
        aggregation_window=PaFacilityPopAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=PaFacilityPopAggregate.bed_capacity,
        custodial=PaFacilityPopAggregate.in_house_adp,
        admissions=PaFacilityPopAggregate.admissions,
        jurisdictional=None,
        male=None,
        female=None,
        sentenced=None,
        pretrial=None,
        felony=None,
        misdemeanor=None,
        parole_violators=None,
        probation_violators=None,
        technical_parole_violators=None,
        civil=None,
        held_for_doc=None,
        held_for_federal=None,
        total_held_for_other=None,
        held_elsewhere=PaFacilityPopAggregate.housed_elsewhere_adp,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=None,
        sentenced_female=None,
        pretrial_male=None,
        pretrial_female=None,
        felony_male=None,
        felony_female=None,
        felony_pretrial=None,
        felony_sentenced=None,
        felony_pretrial_male=None,
        felony_pretrial_female=None,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=None,
        misdemeanor_sentenced=None,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=None,
        misdemeanor_pretrial_female=None,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=None,
        held_for_doc_female=None,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=None,
        total_held_for_other_female=None,
    )
)

MAPPINGS.add(
    Mappings(
        fips=PaCountyPreSentencedAggregate.fips,
        facility_name=PaCountyPreSentencedAggregate.county_name,
        report_date=PaCountyPreSentencedAggregate.report_date,
        aggregation_window=PaCountyPreSentencedAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=None,
        custodial=None,
        admissions=None,
        jurisdictional=None,
        male=None,
        female=None,
        sentenced=None,
        pretrial=PaCountyPreSentencedAggregate.pre_sentenced_population,
        felony=None,
        misdemeanor=None,
        parole_violators=None,
        probation_violators=None,
        technical_parole_violators=None,
        civil=None,
        held_for_doc=None,
        held_for_federal=None,
        total_held_for_other=None,
        held_elsewhere=None,
        jail_capacity_male=None,
        jail_capacity_female=None,
        sentenced_male=None,
        sentenced_female=None,
        pretrial_male=None,
        pretrial_female=None,
        felony_male=None,
        felony_female=None,
        felony_pretrial=None,
        felony_sentenced=None,
        felony_pretrial_male=None,
        felony_pretrial_female=None,
        felony_sentenced_male=None,
        felony_sentenced_female=None,
        misdemeanor_pretrial=None,
        misdemeanor_sentenced=None,
        misdemeanor_male=None,
        misdemeanor_female=None,
        misdemeanor_pretrial_male=None,
        misdemeanor_pretrial_female=None,
        misdemeanor_sentenced_male=None,
        misdemeanor_sentenced_female=None,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=None,
        held_for_doc_female=None,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=None,
        total_held_for_other_female=None,
    )
)

MAPPINGS.add(
    Mappings(
        fips=TnFacilityAggregate.fips,
        facility_name=TnFacilityAggregate.facility_name,
        report_date=TnFacilityAggregate.report_date,
        aggregation_window=TnFacilityAggregate.aggregation_window,
        resident_population=None,
        jail_capacity=TnFacilityAggregate.total_beds,
        custodial=TnFacilityAggregate.total_jail_population,
        admissions=None,
        jurisdictional=None,
        male=TnFacilityAggregate.total_jail_population
        - TnFacilityFemaleAggregate.female_jail_population,
        female=TnFacilityFemaleAggregate.female_jail_population,
        sentenced=TnFacilityAggregate.tdoc_backup_population
        + TnFacilityAggregate.local_felons_population
        + TnFacilityAggregate.other_convicted_felons_population
        + TnFacilityAggregate.convicted_misdemeanor_population,
        pretrial=TnFacilityAggregate.pretrial_felony_population
        + TnFacilityAggregate.pretrial_misdemeanor_population,
        felony=TnFacilityAggregate.tdoc_backup_population
        + TnFacilityAggregate.local_felons_population
        + TnFacilityAggregate.other_convicted_felons_population
        + TnFacilityAggregate.pretrial_felony_population,
        misdemeanor=TnFacilityAggregate.pretrial_misdemeanor_population
        + TnFacilityAggregate.convicted_misdemeanor_population,
        parole_violators=None,
        probation_violators=None,
        technical_parole_violators=None,
        civil=None,
        held_for_doc=TnFacilityAggregate.tdoc_backup_population,
        held_for_federal=None,
        total_held_for_other=TnFacilityAggregate.federal_and_other_population
        + TnFacilityAggregate.tdoc_backup_population,
        held_elsewhere=None,
        jail_capacity_male=TnFacilityAggregate.total_beds
        - TnFacilityFemaleAggregate.female_beds,
        jail_capacity_female=TnFacilityFemaleAggregate.female_beds,
        sentenced_male=TnFacilityAggregate.tdoc_backup_population
        + TnFacilityAggregate.local_felons_population
        + TnFacilityAggregate.other_convicted_felons_population
        + TnFacilityAggregate.convicted_misdemeanor_population
        - (
            TnFacilityFemaleAggregate.tdoc_backup_population
            + TnFacilityFemaleAggregate.local_felons_population
            + TnFacilityFemaleAggregate.other_convicted_felons_population
            + TnFacilityFemaleAggregate.convicted_misdemeanor_population
        ),
        sentenced_female=TnFacilityFemaleAggregate.tdoc_backup_population
        + TnFacilityFemaleAggregate.local_felons_population
        + TnFacilityFemaleAggregate.other_convicted_felons_population
        + TnFacilityFemaleAggregate.convicted_misdemeanor_population,
        pretrial_male=TnFacilityAggregate.pretrial_felony_population
        + TnFacilityAggregate.pretrial_misdemeanor_population
        - (
            TnFacilityFemaleAggregate.pretrial_felony_population
            + TnFacilityFemaleAggregate.pretrial_misdemeanor_population
        ),
        pretrial_female=TnFacilityFemaleAggregate.pretrial_felony_population
        + TnFacilityFemaleAggregate.pretrial_misdemeanor_population,
        felony_male=TnFacilityAggregate.tdoc_backup_population
        + TnFacilityAggregate.local_felons_population
        + TnFacilityAggregate.other_convicted_felons_population
        + TnFacilityAggregate.pretrial_felony_population
        - (
            TnFacilityFemaleAggregate.tdoc_backup_population
            + TnFacilityFemaleAggregate.local_felons_population
            + TnFacilityFemaleAggregate.other_convicted_felons_population
            + TnFacilityFemaleAggregate.pretrial_felony_population
        ),
        felony_female=TnFacilityFemaleAggregate.tdoc_backup_population
        + TnFacilityFemaleAggregate.local_felons_population
        + TnFacilityFemaleAggregate.other_convicted_felons_population
        + TnFacilityFemaleAggregate.pretrial_felony_population,
        felony_pretrial=TnFacilityAggregate.pretrial_felony_population,
        felony_pretrial_male=TnFacilityAggregate.pretrial_felony_population
        - TnFacilityFemaleAggregate.pretrial_felony_population,
        felony_pretrial_female=TnFacilityFemaleAggregate.pretrial_felony_population,
        felony_sentenced=TnFacilityAggregate.tdoc_backup_population
        + TnFacilityAggregate.local_felons_population
        + TnFacilityAggregate.other_convicted_felons_population,
        felony_sentenced_male=TnFacilityAggregate.tdoc_backup_population
        + TnFacilityAggregate.local_felons_population
        + TnFacilityAggregate.other_convicted_felons_population
        - (
            TnFacilityFemaleAggregate.tdoc_backup_population
            + TnFacilityFemaleAggregate.local_felons_population
            + TnFacilityFemaleAggregate.other_convicted_felons_population
        ),
        felony_sentenced_female=TnFacilityFemaleAggregate.tdoc_backup_population
        + TnFacilityFemaleAggregate.local_felons_population
        + TnFacilityFemaleAggregate.other_convicted_felons_population,
        misdemeanor_pretrial=TnFacilityAggregate.pretrial_misdemeanor_population,
        misdemeanor_pretrial_male=TnFacilityAggregate.pretrial_misdemeanor_population
        - TnFacilityFemaleAggregate.pretrial_misdemeanor_population,
        misdemeanor_pretrial_female=TnFacilityFemaleAggregate.pretrial_misdemeanor_population,
        misdemeanor_sentenced=TnFacilityAggregate.convicted_misdemeanor_population,
        misdemeanor_sentenced_male=TnFacilityAggregate.convicted_misdemeanor_population
        - TnFacilityFemaleAggregate.convicted_misdemeanor_population,
        misdemeanor_sentenced_female=TnFacilityFemaleAggregate.convicted_misdemeanor_population,
        misdemeanor_male=TnFacilityAggregate.pretrial_misdemeanor_population
        + TnFacilityAggregate.convicted_misdemeanor_population
        - (
            TnFacilityFemaleAggregate.pretrial_misdemeanor_population
            + TnFacilityFemaleAggregate.convicted_misdemeanor_population
        ),
        misdemeanor_female=TnFacilityFemaleAggregate.pretrial_misdemeanor_population
        + TnFacilityFemaleAggregate.convicted_misdemeanor_population,
        parole_violators_male=None,
        parole_violators_female=None,
        held_for_doc_male=TnFacilityAggregate.tdoc_backup_population
        - TnFacilityFemaleAggregate.tdoc_backup_population,
        held_for_doc_female=TnFacilityFemaleAggregate.tdoc_backup_population,
        held_for_federal_male=None,
        held_for_federal_female=None,
        total_held_for_other_male=TnFacilityAggregate.federal_and_other_population
        + TnFacilityAggregate.tdoc_backup_population
        - (
            TnFacilityFemaleAggregate.federal_and_other_population
            + TnFacilityFemaleAggregate.tdoc_backup_population
        ),
        total_held_for_other_female=TnFacilityFemaleAggregate.federal_and_other_population
        + TnFacilityFemaleAggregate.tdoc_backup_population,
    )
)
