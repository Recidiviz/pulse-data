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


class PersonOffenseIncludesExcludes(Enum):
    AGGRAVATED_ASSAULT = "Aggravated assault"
    SIMPLE_ASSAULT = "Simple assault"
    INTIMIDATION = "Intimidation"
    MURDER = "Murder and nonnegligent manslaughter"
    MANSLAUGHTER = "Negligent manslaughter"
    HUMAN_TRAFFICKING_COMMERCIAL = "Human trafficking, commercial sex acts"
    HUMAN_TRAFFICKING_INVOLUNTARY = "Human trafficking, involuntary servitude"
    KIDNAPPING = "Kidnapping or abduction"
    RAPE = "Rape"
    SODOMY = "Sodomy"
    SEXUAL_ASSAULT = "Sexual assault with an object"
    FONDLING = "Fondling"
    INCEST = "Incest"
    STATUTORY_RAPE = "Statutory rape"
    ROBBERY = "Robbery"
    JUSTIFIABLE_HOMICIDE = "Justifiable homicide"


class PropertyOffenseIncludesExcludes(Enum):
    ARSON = "Arson"
    BRIBERY = "Bribery"
    BURGLARY = "Burglary/ breaking and entering"
    COUNTERFEITING = "Counterfeiting/forgery"
    VANDALISM = "Destruction/damage/vandalism of property"
    EMBEZZLEMENT = "Embezzlement"
    EXTORTION = "Extortion/blackmail"
    FALSE_PRETENSES = "False pretenses/swindle/confidence game"
    CREDIT_CARD = "Credit card/automated teller machine fraud"
    IMPERSONATION = "Impersonation"
    WELFARE_FRAUD = "Welfare Fraud"
    WIRE_FRAUD = "Wire Fraud"
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
    """Includes/Excludes for Public Order Offenses"""

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
    DRUG_VIOLATIONS = "Drug/narcotic violations"
    DRUG_EQUIPMENT_VIOLATIONS = "Drug equipment violations"
    DRUG_SALES = "Drug sales"
    DRUG_DISTRIBUTION = "Drug distribution"
    DRUG_MANUFACTURING = "Drug manufacturing"
    DRUG_SMUGGLING = "Drug smuggling"
    DRUG_PRODUCTION = "Drug production"
    DRUG_POSSESSION = "Drug possession"
