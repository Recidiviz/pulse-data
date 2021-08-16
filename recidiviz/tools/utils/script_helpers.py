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
import os
import pwd
import subprocess
import sys
from typing import Callable, Generator, Optional


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


def _get_run_as_user_fn(password_record: pwd.struct_passwd) -> Callable[[], None]:
    """Returns a function that modifes the current OS user and group to those given.

    To be used in preexec_fn when creating new subprocesses."""

    def set_ids() -> None:
        # Must set group id first. If user id is set first, then that user won't have permission to modify the group.
        os.setgid(password_record.pw_gid)
        os.setuid(password_record.pw_uid)

    return set_ids


def run_command(
    command: str,
    assert_success: bool = True,
    as_user: Optional[pwd.struct_passwd] = None,
    timeout_sec: int = 15,
) -> str:
    """Runs the given command, waiting for it to complete before returning output.
    Throws if the command exits with a non-zero return code.

    Runs the command as a different OS user if `as_user` is not None. If the command succeeds, returns any output from
    stdout. If the command fails and `assert_success` is set, raises an error.
    """
    # pylint: disable=subprocess-popen-preexec-fn
    with subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=_get_run_as_user_fn(as_user) if as_user else None,
    ) as proc:
        try:
            out, err = proc.communicate(timeout=timeout_sec)
        except subprocess.TimeoutExpired as e:
            proc.kill()
            out, err = proc.communicate()
            raise RuntimeError(f"Command timed out: `{command}`\n{err}\n{out}") from e

        if assert_success and proc.returncode != 0:
            raise RuntimeError(f"Command failed: `{command}`\n{err}\n{out}")
        return out


def run_command_streaming(
    command: str, assert_success: bool = True
) -> Generator[str, None, None]:
    """Runs the given command, yielding stdout output line by line as it runs.
    Throws if the command exits with a non-zero return code.
    """
    with subprocess.Popen(
        command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        if not proc.stdout or not proc.stdin or not proc.stderr:
            raise ValueError("Found one of stdout/stdin/stderr is None")
        for stdout_line in iter(proc.stdout.readline, ""):
            yield stdout_line
        return_code = proc.wait()
        err = proc.stderr.read()
        proc.stdin.close()
        proc.stdout.close()
        proc.stderr.close()
        if assert_success and return_code != 0:
            raise RuntimeError(f"Command failed: `{command}`\n### STDERR:\n{err}\n###")
