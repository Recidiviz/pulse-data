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
"""Script for creating release notes of production deploys.

Run the following command within a pipenv shell to execute:

python -m recidiviz.tools.deploy.generate_release_notes --previous_tag [PREVIOUS_TAG] --new_tag [NEW_TAG] --github_token [GITHUB_TOKEN]
"""
import argparse
import logging
import sys
from datetime import datetime
from typing import List, Tuple
import requests

REPO_URL = "https://api.github.com/repos/Recidiviz/pulse-data"


class ReleaseNotes:
    def __init__(self, previous_tag: str, new_tag: str, github_token: str):
        self.previous_tag = previous_tag
        self.new_tag = new_tag
        self.github_token = github_token
        self.notes_name = (
            f"New Release - {datetime.today().strftime('%Y-%m-%d')} [{self.new_tag}]"
        )

    def set_body(self, body: str) -> None:
        self.body = body


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--previous_tag",
        dest="previous_tag",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--new_tag",
        dest="new_tag",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--github_token",
        dest="github_token",
        type=str,
        required=True,
    )

    return parser.parse_known_args(argv)


def generate_headers(release_notes: ReleaseNotes) -> dict:
    """Generetes appropriate headers for Github request"""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "authorization": f"Bearer {release_notes.github_token}",
        "Content-Type": "application/json",
    }
    return headers


def publish_release_notes(release_notes: ReleaseNotes) -> None:
    """
    Publishes a markdown release for the version tag
    """
    url = f"{REPO_URL}/releases"
    payload = {
        "tag_name": release_notes.new_tag,
        "target_commitish": "main",
        "name": release_notes.notes_name,
        "body": release_notes.body,
        "draft": False,
        "prerelease": False,
        "generate_release_notes": False,
    }
    headers = generate_headers(release_notes)

    response = requests.request("POST", url, json=payload, headers=headers)
    logging.info("Please review release notes: [%s]", response.json().get("html_url"))


def generate_release_notes_text(release_notes: ReleaseNotes) -> str:
    """Generates the markdown release notes according to what we have configured in
    .github/release.yml
    """
    url = f"{REPO_URL}/releases/generate-notes"
    payload = {
        "tag_name": release_notes.new_tag,
        "target_commitish": "main",
        "previous_tag_name": release_notes.previous_tag,
        "configuration_file_path": ".github/release.yml",
    }
    headers = generate_headers(release_notes)

    response = requests.request("POST", url, json=payload, headers=headers)
    return response.json().get("body")


def create_release_notes(release_notes: ReleaseNotes) -> None:
    logging.info("Creating release notes for: [%s]", {release_notes.new_tag})

    body = generate_release_notes_text(release_notes)
    release_notes.set_body(body)
    publish_release_notes(release_notes)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    known_args, _ = parse_arguments(sys.argv)

    release_notes_config = ReleaseNotes(
        known_args.previous_tag, known_args.new_tag, known_args.github_token
    )

    try:
        create_release_notes(release_notes_config)
    except Exception as e:
        logging.error(
            "Failed to auto-generate release notes: [%s].",
            e,
            exc_info=True,
        )
