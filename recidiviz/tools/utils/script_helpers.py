# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""General helpers for python scripts."""
import logging
import sys
from typing import Optional


def prompt_for_confirmation(
    input_text: str,
    accepted_response_override: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    input_prompt = f"{input_text}"

    accepted_response = accepted_response_override or "Y"
    if accepted_response_override:
        input_prompt += (
            f'\nPlease type "{accepted_response}" to confirm. (Anything else exits): '
        )
    else:
        input_prompt += " [y/n]: "

    if dry_run:
        logging.info("[DRY RUN] %s **DRY RUN - SKIPPED CONFIRMATION**", input_prompt)
        return

    check = input(input_prompt)
    if check.lower() != accepted_response.lower():
        logging.warning("\nResponded with [%s].Confirmation aborted.", check)
        sys.exit(1)
