# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Parsing and formatting helpers for the Marquis eOMIS web UI.

These encode the structure Marquis renders for *every* screen (listing
tables, form inputs, paging JSON, date formats) — nothing here is specific
to one state or one screen. Callers identify their listing table by the
column headers it must contain.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Sequence

from bs4 import BeautifulSoup, Tag

from recidiviz.utils.types import assert_type

LISTING_TOTAL_RE = re.compile(r"\(\s*\d+\s*-\s*\d+\s*of\s*(\d+)\s*\)")
PROFILE_PID_RE = re.compile(r"id=['\"]profile-pid['\"]>\s*([0-9]+)\s*<")


def extract_selected_offender_id(html: str) -> str:
    """Returns the OFFENDERID from the offender header's profile-pid element,
    or "" if absent."""
    match = PROFILE_PID_RE.search(html)
    return match.group(1) if match else ""


def to_eomis_date(value: Any) -> str:
    if value in (None, ""):
        raise ValueError("date is required")
    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y")
    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value).strip(), fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    raise ValueError(f"unparseable date {value!r}")


def today_eomis() -> str:
    return datetime.now().strftime("%m/%d/%Y")


def wrap_comment(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    return value if value.startswith("<") else f"<p>{value}</p>"


def clean_optional(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip()


def clean_bool(value: Any) -> bool:
    if value in (None, ""):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "t", "true", "y", "yes"}


def clean_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return to_eomis_date(value)


def parse_listing_table(
    html: str, required_headers: Sequence[str]
) -> list[dict[str, str]]:
    """Parses the listing table identified by required_headers into rows keyed
    by each cell's data-name attribute."""
    soup = BeautifulSoup(html, "html.parser")
    target_table = None
    for table in soup.find_all("table"):
        header_text = " ".join(th.get_text(strip=True) for th in table.find_all("th"))
        if all(header in header_text for header in required_headers):
            target_table = table
            break
    if target_table is None:
        return []

    rows: list[dict[str, str]] = []
    tbody = target_table.find("tbody") or target_table
    for tr in tbody.find_all("tr"):
        if "no-rows-found" in (tr.get("class") or []):
            continue
        row: dict[str, str] = {}
        for cell in tr.find_all("td"):
            if key := cell.get("data-name"):
                row[key] = cell.get_text(" ", strip=True)
        if unique_id := tr.get("data-detailuniqueid"):
            row["_detailuniqueid"] = unique_id
        if link := tr.find("a", href=True):
            row["_detail_url"] = link["href"]
        if row:
            rows.append(row)
    return rows


def input_values(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    values: dict[str, str] = {}
    for element in soup.find_all("input"):
        name = element.get("name")
        if name and name not in values:
            values[name] = element.get("value", "")
    for element in soup.find_all("select"):
        name = element.get("name")
        if name and name not in values:
            option = element.find("option", selected=True)
            values[name] = option.get("value", "") if option else ""
    return values


def listing_total(html: str) -> int | None:
    match = LISTING_TOTAL_RE.search(html)
    return int(match.group(1)) if match else None


def parse_listing_page_rows(page_json: dict[str, Any]) -> list[dict[str, str]]:
    """Converts a listing-page JSON payload into rows shaped like
    parse_listing_table output."""
    rows: list[dict[str, str]] = []
    for result in page_json["results"]:
        row: dict[str, str] = {}
        for key, value in result.items():
            if key in ("detailuniqueid", "clazz"):
                continue
            cell = BeautifulSoup(value, "html.parser")
            row[key] = cell.get_text(" ", strip=True)
            if isinstance(link := cell.find("a", href=True), Tag):
                row["_detail_url"] = assert_type(link["href"], str)
        row["_detailuniqueid"] = result["detailuniqueid"]
        rows.append(row)
    return rows
