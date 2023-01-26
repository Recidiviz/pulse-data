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
# =============================================================================
"""Includes/Excludes definition for offense type dimensions """

from enum import Enum


# Shared among most metrics
class PersonOffenseIncludesExcludes(Enum):
    """Global Includes/Excludes for Person Offenses"""

    AGGRAVATED_ASSAULT = "Aggravated assault"
    SIMPLE_ASSAULT = "Simple assault"
    INTIMIDATION = "Intimidation"
    MURDER = "Murder and nonnegligent manslaughter"
    MANSLAUGHTER = "Negligent manslaughter"
    HUMAN_TRAFFICKING_COMMERCIAL = "Human trafficking, commercial sex acts"
    HUMAN_TRAFFICKING_INVOLUNTARY = "Human trafficking, involuntary servitude"
    KIDNAPPING = "Kidnapping/abduction"
    RAPE = "Rape"
    SODOMY = "Sodomy"
    SEXUAL_ASSAULT = "Sexual assault with an object"
    FONDLING = "Fondling"
    INCEST = "Incest"
    STATUTORY_RAPE = "Statutory rape"
    ROBBERY = "Robbery"
    JUSTIFIABLE_HOMICIDE = "Justifiable homicide"


class PropertyOffenseIncludesExcludes(Enum):
    """Global Includes/Excludes for Property Offenses"""

    ARSON = "Arson"
    BRIBERY = "Bribery"
    BURGLARY = "Burglary/breaking and entering"
    COUNTERFEITING = "Counterfeiting/forgery"
    VANDALISM = "Destruction/damage/vandalism of property"
    EMBEZZLEMENT = "Embezzlement"
    EXTORTION = "Extortion/blackmail"
    FALSE_PRETENSES = "False pretenses/swindle/confidence game"
    CREDIT_CARD = "Credit card/automated teller machine fraud"
    IMPERSONATION = "Impersonation"
    WELFARE_FRAUD = "Welfare fraud"
    WIRE_FRAUD = "Wire fraud"
    IDENTITY_THEFT = "Identity theft"
    HACKING = "Hacking/computer invasion"
    POCKET_PICKING = "Pocket-picking"
    PURSE_SNATCHING = "Purse-snatching"
    SHOPLIFTING = "Shoplifting"
    THEFT_FROM_BULIDING = "Theft from building"
    THEFT_FROM_MACHINE = "Theft from coin-operated machine or device"
    THEFT_FROM_VEHICLE = "Theft from motor vehicle"
    THEFT_OF_VEHICLE_PARTS = "Theft of motor vehicle parts or accessories"
    LARCENY = "All other larceny"
    THEFT_OF_VEHICLE = "Motor vehicle theft"
    STOLEN_PROPERTY = "Stolen property offenses"
    ROBBERY = "Robbery"


class PublicOrderOffenseIncludesExcludes(Enum):
    """Global Includes/Excludes for Public Order Offenses"""

    ANIMAL_CRUELTY = "Animal cruelty"
    IMPORT_VIOLATIONS = "Import violations"
    EXPORT_VIOLATIONS = "Export violations"
    LIQUOR = "Federal liquor offenses"
    TOBACCO = "Federal tobacco offenses"
    WILDLIFE = "Wildlife trafficking"
    ESPIONAGE = "Espionage"
    MONEY_LAUNDERING = "Money laundering"
    HARBORING = "Harboring escapee/concealing from arrest"
    FLIGHT_PROSECUTION = "Flight to avoid prosecution"
    FLIGHT_DEPORTATION = "Flight to avoid deportation"
    BETTING = "Betting/wagering"
    GAMBLING = "Operating/promoting/assisting gambling"
    GAMBLING_EQUIPMENT = "Gambling equipment violations"
    SPORTS_TAMPERING = "Sports tampering"
    ILLEGAL_ENTRY = "Illegal entry into the United States"
    FALSE_CITIZENSHIP = "False citizenship"
    SMUGGLING = "Smuggling aliens"
    RENTRY = "Re-entry after deportation"
    PORNOGRAPHY = "Pornography/obscene material"
    PROSTITUTION = "Prostitution"
    ASSISTING_PROSTITUTION = "Assisting or promoting prostitution"
    PURCHASING_PROSTITUTION = "Purchasing prostitution"
    TREASON = "Treason"
    WEAPON_LAW_VIOLATIONS = "Weapon law violations"
    FIREARM_VIOLATIONS = "Violation of National Firearm Act of 1934"
    WEAPONS_OF_MASS_DESTRUCTION = "Weapons of mass destruction"
    EXPLOSIVES = "Explosives"
    FAILURE_TO_APPEAR = "Failure to appear"
    CURFEW = "Curfew/loitering/vagrancy violations"
    DISORDERLY_CONDUCT = "Disorderly conduct"
    DUI = "Driving under the influence"
    FAMILY_OFFENSES = "Family offenses, nonviolent"
    FEDERAL_RESOURCE_VIOLATIONS = "Federal resource violations"
    LIQUOR_LAW_VIOLATIONS = "Liquor law violations"
    PERJURY = "Perjury"
    TRESPASS = "Trespass of real property"
    DRUG_VIOLATIONS = "Drug/narcotic violations"
    DRUG_EQUIPMENT_VIOLATIONS = "Drug equipment violations"
    DRUG_SALES = "Drug sales"
    DRUG_DISTRIBUTION = "Drug distribution"
    DRUG_MANUFACTURING = "Drug manufacturing"
    DRUG_SMUGGLING = "Drug smuggling"
    DRUG_PRODUCTION = "Drug production"
    DRUG_POSSESSION = "Drug possession"


class DrugOffenseIncludesExcludes(Enum):
    """Global Includes/Excludes for Drug Offenses"""

    DRUG_VIOLATIONS = "Drug/narcotic violations"
    DRUG_EQUIPMENT_VIOLATIONS = "Drug equipment violations"
    DRUG_SALES = "Drug sales"
    DRUG_DISTRIBUTION = "Drug distribution"
    DRUG_MANUFACTURING = "Drug manufacturing"
    DRUG_SMUGGLING = "Drug smuggling"
    DRUG_PRODUCTION = "Drug production"
    DRUG_POSSESSION = "Drug possession"


# Shared among Supervision and Prisons
class ProbationDefinitionIncludesExcludes(Enum):
    IN_LIEU_INCARCERATION = "People sentenced to a period of probation in lieu of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    AFTER_INCARCERATION = "People sentenced to a period of probation after a period of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    POST_ADJUCATION_PROGRAM = "People on probation as part of a post-adjudication specialty or problem-solving court program (e.g., drug court)"
    TEMPORARILY_CONFINED = "People sentenced to probation who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    CONFINED_ANY_LENGTH = "People sentenced to probation confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    HOLD_PENDING = "People sentenced to probation who are in jail or prison on a hold pending resolution of a violation or revocation"
    LONGER_SANCTION = "People sentenced to probation who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    COMPACT_AGREEMENT = "People sentenced to probation in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    ANOTHER_JURISTICTION = (
        "People sentenced to probation who are being supervised by another jurisdiction"
    )
    IN_COMMUNITY = "People who have not been sentenced but are supervised on probation in the community prior to the resolution of their case"
    ANOTHER_FORM_SUPERVISION = (
        "People sentenced to probation who are also on another form of supervision"
    )
    PRE_ADJUCTATION_PROGRAM = "People on probation as part of a pre-adjudication specialty or problem-solving court program (e.g., drug court)"


class ParoleDefinitionIncludesExcludes(Enum):
    EARLY_RELEASE = "People approved by a parole board or similar entity for early conditional release from incarceration to parole supervision (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    STATUTORY_REQUIREMENT = "People conditionally released from incarceration to parole supervision by statutory requirement (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    TEMPORARILY_CONFINED = "People on parole who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    CONFINED_ANY_LENGTH = "People on parole confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    HOLD_PENDING = "People on parole who are in jail or prison on a hold pending resolution of a violation or revocation"
    LONGER_SANCTION = "People on parole who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    COMPACT_AGREEMENT = "People released to parole in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    ANOTHER_FORM_SUPERVISION = (
        "People on parole who are also on another form of supervision"
    )
    ANOTHER_JURISTICTION = (
        "People on parole who are being supervised by another jurisdiction"
    )


class PretrialDefinitionIncludesExcludes(Enum):
    CITATION_RELEASE = "People on citation release (i.e., were never booked)"
    CONDITION_SUPERVISION = "People released from jail or otherwise not held pretrial on the condition of supervision (including electronic monitoring, home confinement, traditional supervision, etc.)"
    STATUTORY_REQUIREMENT = "People released from jail or otherwise not held pretrial due to statutory requirement"
    COURT_PROGRAM = "People supervised as part of a pre-adjudication specialty or problem-solving court program (e.g., drug court)"
    HOLD_PENDING = "People on pretrial supervision who are incarcerated on a hold pending resolution of a violation or revocation"
    ANOTHER_FORM_SUPERVISION = (
        "People on pretrial supervision who are also on another form of supervision"
    )


class OtherCommunityDefinitionIncludesExcludes(Enum):
    IN_LIEU_INCARCERATION = "People sentenced to a period of other community supervision in lieu of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    DETERMINATE_PERIOD = "People sentenced to a determinate period of other community supervision after a period of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    POST_ADJUCATION_PROGRAM = "People on other community supervision as part of a post-adjudication specialty or problem-solving court program (e.g., drug court)"
    EARLY_RELEASE = "People approved by a parole board or similar entity for early conditional release from incarceration to other community supervision (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    STATUTORY_REQUIREMENT = "People conditionally released from incarceration to other community supervision by statutory requirement (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    TEMPORARILY_CONFINED = "People on other community supervision who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    CONFINED_ANY_LENGTH = "People on other community supervision confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    JAIL_OR_PRISON_HOLD_PENDING = "People on other community supervision who are in jail or prison on a hold pending resolution of a violation or revocation"
    LONGER_SANTION = "People on other community supervision who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    INCARCERATED_HOLD_PENDING = "People on other community supervision who are incarcerated on a hold pending resolution of a violation or revocation"
    COMPACT_AGREEMENT = "People on supervision in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    ANOTHER_FORM_SUPERVISION = "People on other community supervision who are also on another form of supervision"
    PRIOR_TO_RESOLUTION = "People on other community supervision who have not been sentenced but are supervised in the community prior to the resolution of their case"
    COURT_PROGRAM = "People on other community supervision in a pre-adjudication specialty or problem-solving court program (e.g., drug court, etc.)"
