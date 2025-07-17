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
"""Item models and data processing for the scraper engine."""
import base64
from enum import Enum
from typing import Dict, Optional

import usaddress  # type: ignore
from pydantic import BaseModel, Field
from typing_extensions import NotRequired, TypedDict

from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceOrigin,
    ResourceSubcategory,
)
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidate


class CategoryMapping(TypedDict):
    category: ResourceCategory
    subcategory: NotRequired[Optional[ResourceSubcategory]]


CategoryMapperType = Dict[str, CategoryMapping]


class FieldTypes(Enum):
    PHONE_NUMBER = "phone_number"
    ADDRESS = "street_address"
    CATEGORY = "category_mapping"
    DESCRIPTION = "description"
    NAME = "name"
    HOURS_OF_OPERATION = "hours_of_operation"
    WEBSITE = "website"
    EMAIL = "email"
    ADDITIONAL_DATA = "additional_data"


class AddressParser(BaseModel):
    """Parser for extracting structured address components from address strings."""

    address: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None

    def parse_address(self) -> None:
        """Parse the address string and extract structured components.

        Uses the usaddress library to parse the address string and populate
        the street, city, state, and zip fields. Handles various address
        components including street numbers, directional prefixes/suffixes,
        street names, and place names.
        """
        if not self.address:
            return

        street_components: dict[str, str] = {
            "number": "",
            "predirectional": "",
            "name": "",
            "posttype": "",
            "postdirectional": "",
        }
        city_parts: list[str] = []

        details = usaddress.parse(self.address)

        for detail in details:
            value, label = detail
            match label:
                case "AddressNumber":
                    street_components["number"] = value
                case "StreetNamePreDirectional":
                    street_components["predirectional"] = value
                case "StreetName":
                    street_components["name"] = value
                case "StreetNamePostType":
                    street_components["posttype"] = value
                case "StreetNamePostDirectional":
                    street_components["postdirectional"] = value
                case "PlaceName":
                    city_parts.append(value)
                case "StateName":
                    self.state = value.rstrip(",")
                case "ZipCode":
                    self.zip = value.rstrip(",")

        street_parts = [
            street_components["number"],
            street_components["predirectional"],
            street_components["name"],
            street_components["posttype"],
            street_components["postdirectional"],
        ]
        self.street = " ".join(filter(None, street_parts)).strip()

        if city_parts:
            self.city = " ".join(city_parts)

    @staticmethod
    def from_string(address_string: Optional[str]) -> "AddressParser":
        """Create an Address instance from a string"""
        address = AddressParser(
            address=address_string, street="", city="", state="", zip=""
        )
        address.parse_address()
        return address


class OutputItem(BaseModel):
    """Represents a scraped resource item with all its extracted data."""

    phone_number: Optional[str] = None
    address: Optional[str] = None
    category: Optional[ResourceCategory] = ResourceCategory.UNKNOWN
    subcategory: Optional[ResourceSubcategory] = None
    description: Optional[str] = None
    name: Optional[str] = None
    hours_of_operation: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    additional_data: dict[str, str] = {}

    unique_id: Optional[str] = Field(
        None,
        description="""
Once item is processed, we need to create a unique identifier that will identify
this resource consistently across scraping sessions.

Possible identifiers:
- Website URL
- Slug derived from physical address
- Combination of fields (e.g., email + phone number)

We need a fallback mechanism that will prevent items from passing further if both conditions are true:
Slug construction fails
Website URL is missing
                                     """,
    )

    def set_by_type(self, field_type: FieldTypes, val: str) -> None:
        match field_type:
            case FieldTypes.PHONE_NUMBER:
                self.phone_number = val
            case FieldTypes.ADDRESS:
                if self.address:
                    self.address += " " + val
                else:
                    self.address = val
            case FieldTypes.DESCRIPTION:
                if self.description:
                    self.description += "\n" + val
                else:
                    self.description = val
            case FieldTypes.NAME:
                self.name = val
            case FieldTypes.HOURS_OF_OPERATION:
                if self.hours_of_operation:
                    self.hours_of_operation += "\n" + val
                else:
                    self.hours_of_operation = val
            case FieldTypes.WEBSITE:
                self.website = val
            case FieldTypes.EMAIL:
                self.email = val

    def map_category(self, raw_category: str, mapper: CategoryMapperType) -> None:

        raw_category = raw_category.lower().strip()

        if category := mapper.get(raw_category):
            self.category = category.get("category")
            self.subcategory = category.get("subcategory")

    def set_additional_data(self, data: dict[str, str]) -> None:
        self.additional_data = self.additional_data | data

    def create_id(self) -> None:
        if self.website:
            self.unique_id = self.website
        elif self.address and self.name:
            encoded_address = base64.urlsafe_b64encode(
                f"{self.address + self.name}".encode()
            ).decode()
            self.unique_id = f"resource://candidate/{encoded_address}"
        elif self.email and self.phone_number and self.name:
            encoded_value = base64.urlsafe_b64encode(
                f"{self.email + self.phone_number + self.name}".encode()
            ).decode()
            self.unique_id = f"resource://candidate/{encoded_value}"

    def produce_resource_candidate(self) -> Optional[ResourceCandidate]:
        if not self.unique_id or not self.address:
            return None

        address = AddressParser.from_string(self.address)

        return ResourceCandidate(
            origin=ResourceOrigin.CRAWLER,
            name=self.name,
            website=self.website,
            phone=self.phone_number,
            email=self.email,
            description=self.description,
            operationalStatus=self.hours_of_operation,
            rawData=self.additional_data,
            category=self.category,
            subcategory=self.subcategory,
            address=address.address,
            street=address.street,
            city=address.city,
            state=address.state,
            zip=address.zip,
        )
