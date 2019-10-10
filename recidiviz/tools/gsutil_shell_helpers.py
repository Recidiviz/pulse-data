# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Helpers for calling gsutil commands inside of Python scripts."""
import subprocess
from typing import List


def gsutil_ls(gs_path: str) -> List[str]:
    """Returns list of paths returned by 'gsutil ls <gs_path>.
    E.g.
    gsutil_ls('gs://recidiviz-123-state-storage') ->
        ['gs://recidiviz-123-state-storage/us_nd']

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/ls
    """
    res = subprocess.Popen(f'gsutil ls {gs_path}',
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    stdout, stderr = res.communicate()

    if stderr:
        raise ValueError(stderr.decode('utf-8'))

    return stdout.decode('utf-8').splitlines()


def gsutil_cp(from_path: str, to_path: str) -> None:
    """Copies a file/files via 'gsutil cp'.

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/cp
    """
    command = f'gsutil -q -m cp {from_path} {to_path}'
    res = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    _stdout, stderr = res.communicate()

    if stderr:
        raise ValueError(stderr.decode('utf-8'))


def gsutil_mv(from_path: str, to_path: str) -> None:
    """Moves a file/files via 'gsutil mv'.

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/mv
    """

    res = subprocess.Popen(
        f'gsutil -q -m mv {from_path} {to_path}',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    _stdout, stderr = res.communicate()

    if stderr:
        raise ValueError(stderr.decode('utf-8'))
