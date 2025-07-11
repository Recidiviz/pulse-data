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
# =============================================================================

"""Helpers for CRUD operations"""

import logging
from typing import Any, Optional
from urllib.parse import quote

import usaddress  # type: ignore

from recidiviz.resource_search.src.geo import coordinates_to_location_wkb
from recidiviz.resource_search.src.models.resource import ResourceCreate
from recidiviz.resource_search.src.models.resource_enums import ResourceCategory
from recidiviz.resource_search.src.typez.crud.resources import (
    ResourceCandidate,
    ResourceCandidateWithURI,
    ResourceCandidateWithURICoord,
)
from recidiviz.resource_search.src.typez.handlers.base import ApiSearchResult


def make_resource_create(
    body: ResourceCandidateWithURICoord, embedding: Optional[list[float]] = None
) -> ResourceCreate:
    """Make resource to be created"""
    wkb_location = coordinates_to_location_wkb(body.lat, body.lon)

    extra_data: dict[str, Any] = {}
    if body.price_level:
        extra_data["priceLevel"] = body.price_level
    if body.rating:
        extra_data["rating"] = float(body.rating)
    if body.ratingCount:
        extra_data["ratingCount"] = int(body.ratingCount)
    if body.operationalStatus:
        extra_data["operationalStatus"] = body.operationalStatus

    resource = ResourceCreate(
        **body.model_dump(),
        location=wkb_location,
        embedding=embedding,
        extra_data=extra_data,
    )

    return resource


def _usaddress_as_dict(address: str) -> dict[str, str]:
    """Get address parts as dict with usaddress"""
    address_dict: dict[str, str] = {}
    address_tuples = usaddress.parse(address)
    for value, type_name in address_tuples:
        address_dict[type_name] = value
    return address_dict


def backfill_resource_uri(
    resource_candidates: list[ResourceCandidate],
) -> list[ResourceCandidateWithURI]:
    """Ensure resource URL is set"""
    resources_with_uris = [
        ResourceCandidateWithURI(
            **resource.model_dump(),
            uri=make_resource_uri(resource),
        )
        for resource in resource_candidates
    ]

    doublons = []
    uris: list[
        tuple[str, Optional[ResourceCategory]]
    ] = []  # For detecting duplicate URLs
    for candidate in resources_with_uris:
        if (candidate.uri, candidate.category) in uris:
            logging.warning("Detected duplicate URI for: %s", candidate.uri)
            doublons.append(candidate)

        uris.append((candidate.uri, candidate.category))

    if doublons:  # that should not happen, but just in case
        resources_with_uris = [
            candidate
            for candidate in resources_with_uris
            if candidate.uri not in doublons
        ]

    return resources_with_uris  # no doublons


def make_resource_uri(resource: ResourceCandidate) -> str:
    """
    Create a unique URL identifier from resource candidate data
    """
    identifiers = []

    # Name
    if name := resource.name:
        identifiers.append(name)

    # Address
    address = (
        resource.address
        or f"{resource.street} {resource.city} {resource.state} {resource.zip}"
    )
    address_dict = _usaddress_as_dict(address)
    address_types = [
        "AddressNumberPrefix",
        "AddressNumber",
        "AddressNumberSuffix",
        "StreetNamePreDirectional",
        "StreetName",
        "StreetNamePostType",
        "StreetNamePostDirectional",
        "PlaceName",
        "CityName",
        "StateName",
        "ZipCode",
    ]
    address_parts = []
    for type_name in address_types:
        if value := address_dict.get(type_name):
            address_parts.append(value.replace(",", ""))
    if len(address_parts) > 0:
        identifiers.append(" ".join(address_parts))

    if len(identifiers) == 0:
        raise ValueError("No name or address identifiers exist to build resource URL")

    # Create safe URL
    safe_url = quote("/".join(identifiers).lower())
    return f"resource://{safe_url}"


def filter_banned_resources(
    resources: list[ApiSearchResult], banned: bool
) -> list[ApiSearchResult]:
    """Filter resources by banned status"""
    return [resource for resource in resources if resource.banned == banned]
