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
"""State-agnostic client for the Marquis eOMIS web application.

Owns the eOMIS protocol that every writeback flow shares: login and
session/uuid management, offender selection, generic listing fetch + paging,
and form POSTs. Anything screen-specific — servlet task names, IO_NAMEs,
payloads, success markers — belongs in a flow, not here. If a new state or
use case ever needs to change this file, that falsifies the assumption that
CO and AR run the same Marquis protocol and should be surfaced in review.
"""
from __future__ import annotations

import re
import time
from typing import Any, Sequence

import requests

from recidiviz.tools.eomis.parsing import (
    extract_selected_offender_id,
    listing_total,
    parse_listing_page_rows,
    parse_listing_table,
)

LOGIN_MARKER = "/login/"
SELECT_OFFENDER_MARKER = "must first select an offender"
HEADER_TASK = "HeaderServlet"
HEADER_OPTION = "nonMedicalHeader"
REQUEST_TIMEOUT = (10, 60)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": ('"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"'),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}

NAVIGATE_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Cache-Control": "max-age=0",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

UUID_RE = re.compile(r"uuid=([0-9a-f-]{36})", re.IGNORECASE)


def extract_uuid(html: str) -> str:
    match = UUID_RE.search(html)
    return match.group(1) if match else ""


class EomisClient:
    """A logged-in browser-like session against one eOMIS instance."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.headers.update(BROWSER_HEADERS)
        self.uuid = ""

    @property
    def controller_url(self) -> str:
        return (
            f"{self.base_url}/eomis/servlet/" "com.marquis.eomis.EomisControllerServlet"
        )

    @property
    def dispatch_url(self) -> str:
        return f"{self.base_url}/eomis/application/dispatch"

    def login(self) -> None:
        """Replays the login form flow and captures the per-session uuid that
        every later request must echo."""
        login_dispatch = f"{self.base_url}/eomis/login/dispatch"
        self.session.get(
            login_dispatch,
            headers={**NAVIGATE_HEADERS, "Sec-Fetch-Site": "none"},
            timeout=REQUEST_TIMEOUT,
        ).raise_for_status()

        response = self.session.post(
            f"{self.base_url}/eomis/servlet/com.marquis.eomis.LoginHandler",
            data={
                "task": "LoginHandler",
                "option": "fromLogon",
                "fromLogon": "fromLogon",
                "PCSETTINGS": (
                    "<PCSETTINGS><RESOLUTION><WIDTH>1920</WIDTH>"
                    "<HEIGHT>1080</HEIGHT></RESOLUTION></PCSETTINGS>"
                ),
                "securityMsgAccept": "N",
                "userId": self.username,
                "passwd": self.password,
            },
            headers={
                **NAVIGATE_HEADERS,
                "Sec-Fetch-Site": "same-origin",
                "Origin": self.base_url,
                "Referer": login_dispatch,
            },
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        self.uuid = extract_uuid(response.text)
        if not self.uuid or LOGIN_MARKER in response.url:
            raise RuntimeError("eOMIS login failed")

    def select_offender(self, offender_id: str) -> None:
        """Selects the offender into server-side session state that later
        screens depend on. Must be called before any per-offender screen."""
        response = self.session.get(
            self.controller_url,
            params={
                "task": "InmateSynopsisServlet",
                "option": "detailFromInmateByProf",
                "OffenderId": offender_id,
                "uuid": self.uuid,
            },
            headers={
                **NAVIGATE_HEADERS,
                "Sec-Fetch-Dest": "iframe",
                "Sec-Fetch-Site": "same-origin",
                "Referer": self.dispatch_url,
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        self.uuid = extract_uuid(response.text) or self.uuid

        selected = self.selected_offender_id()
        if selected != offender_id:
            raise RuntimeError(
                f"selected offender [{selected or 'none'}], expected [{offender_id}]"
            )

    def selected_offender_id(self) -> str:
        """Returns the OFFENDERID eOMIS currently has selected in the session."""
        response = self.session.get(
            self.controller_url,
            params={"task": HEADER_TASK, "option": HEADER_OPTION, "uuid": self.uuid},
            headers={**NAVIGATE_HEADERS, "Sec-Fetch-Site": "same-origin"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return extract_selected_offender_id(response.text)

    def browse(self, params: dict[str, str]) -> str:
        """GETs a controller listing screen. params identify the screen
        (task, option, IO_NAME, ...) and are flow-specific."""
        response = self.session.get(
            self.controller_url,
            params={**params, "uuid": self.uuid},
            headers={**NAVIGATE_HEADERS, "Sec-Fetch-Site": "same-origin"},
            allow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        if (
            LOGIN_MARKER in response.url
            or SELECT_OFFENDER_MARKER in response.text.lower()
        ):
            raise RuntimeError("eOMIS session lost or offender was not selected")
        return response.text

    def next_listing_page(self, listing_referer: str) -> dict[str, Any]:
        """Returns the next listing page as JSON. The server-side paging cursor
        is set by the browse call whose URL is passed as listing_referer."""
        response = self.session.get(
            f"{self.base_url}/eomis/servlet/"
            "com.marquis.eomis.gui.v5.BrowseSupportServlet",
            params={
                "uuid": self.uuid,
                "option": "getPage",
                "browse": "list",
                "page": "next",
                "_": str(int(time.time() * 1000)),
            },
            headers={
                "Accept": "*/*",
                "X-Requested-With": "XMLHttpRequest",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Referer": listing_referer,
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        if LOGIN_MARKER in response.url or "json" not in response.headers.get(
            "Content-Type", ""
        ):
            raise RuntimeError("eOMIS session lost while paging listing")
        return response.json()

    def fetch_all_listing_rows(
        self,
        *,
        browse_params: dict[str, str],
        listing_referer: str,
        required_headers: Sequence[str],
    ) -> list[dict[str, str]]:
        """Fetches every page of a listing screen.

        Raises if paging stalls before reaching the advertised row count —
        acting on a partial list risks creating a duplicate record.
        """
        html = self.browse(browse_params)
        rows = parse_listing_table(html, required_headers)
        total = listing_total(html)
        if total is None:
            return rows

        seen_ids = {row.get("_detailuniqueid") for row in rows}
        while len(rows) < total:
            page_rows = parse_listing_page_rows(self.next_listing_page(listing_referer))
            new_rows = [
                row for row in page_rows if row.get("_detailuniqueid") not in seen_ids
            ]
            if not new_rows:
                raise RuntimeError(
                    f"listing paging stalled at {len(rows)} of {total} rows; "
                    "cannot confirm record state - needs manual review"
                )
            rows.extend(new_rows)
            seen_ids.update(row.get("_detailuniqueid") for row in new_rows)
        return rows

    def get(self, url_or_path: str) -> str:
        url = (
            url_or_path
            if url_or_path.startswith("http")
            else f"{self.base_url}{url_or_path}"
        )
        response = self.session.get(
            url,
            headers={
                **NAVIGATE_HEADERS,
                "Sec-Fetch-Site": "same-origin",
                "Referer": self.dispatch_url,
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.text

    def post_write(
        self, payload: list[tuple[str, str]], referer: str
    ) -> requests.Response:
        cachebuster = f"{int(time.time() * 1000)}-0"
        response = self.session.post(
            f"{self.controller_url}?cachebuster={cachebuster}",
            data=payload,
            headers={
                **NAVIGATE_HEADERS,
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": self.base_url,
                "Referer": referer,
                "Sec-Fetch-Dest": "iframe",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response
