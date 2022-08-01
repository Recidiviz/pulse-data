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
"""Script for cleaning up state Github build agents.

Run the following command within a pipenv shell to execute:

python -m recidiviz.tools.deploy.github.self_hosted_build_agent.cleanup_stale_github_agents --github_token [GITHUB_TOKEN]
"""
import argparse
import logging
import sys
from typing import List, Tuple
import requests

REPO_URL = "https://api.github.com/repos/Recidiviz/pulse-data"


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--github_token",
        dest="github_token",
        type=str,
        required=True,
    )

    return parser.parse_known_args(argv)


def generate_headers(github_token: str) -> dict:
    """Generetes appropriate headers for Github request"""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "authorization": f"Bearer {github_token}",
        "Content-Type": "application/json",
    }
    return headers


def kill_stale_agents(agents: List[dict], github_token: str) -> None:
    """Kill all stale agents"""
    headers = generate_headers(github_token)

    for agent in agents:
        url = f"{REPO_URL}/actions/runners/{agent['id']}"
        response = requests.request("DELETE", url, headers=headers)
        if response.status_code == 204:
            logging.info("Successfully killed agent: [%s]", {agent["id"]})
        else:
            logging.info("Failed to kill agent: [%s]", {agent["id"]})


def get_stale_build_agents(github_token: str) -> List[dict]:
    """Returns a list of stale build agents"""
    url = f"{REPO_URL}/actions/runners"
    headers = generate_headers(github_token)

    response = requests.request("GET", url, headers=headers)
    runners = response.json().get("runners")
    stale_agents = [r for r in runners if r.get("status") == "offline"]
    return stale_agents


def cleanup_stale_build_agents(github_token: str) -> None:
    logging.info("Checking for stale Github agents to delete")

    stale_agents = get_stale_build_agents(github_token)
    kill_stale_agents(stale_agents, github_token)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    known_args, _ = parse_arguments(sys.argv)

    try:
        cleanup_stale_build_agents(known_args.github_token)
    except Exception as e:
        logging.error(
            "Cleaning up stale agents failed: [%s]",
            e,
            exc_info=True,
        )
