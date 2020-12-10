# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Serialized ThreadPoolExecutor for testing"""
from concurrent.futures import Future, Executor


class SerialExecutor(Executor):
    """
    Fakes a ThreadPoolExecutor which just runs jobs sequentially
    to avoid race conditions in tests
    """

    # disable pylint since we don't need to mock the keyword version
    # of the call for our purposes, just the positional one
    def submit(self, fn, *args, **kwargs): # pylint: disable=W0221
        f = Future()
        try:
            f.set_result(fn(*args, **kwargs))
        except Exception as e:
            f.set_exception(e)

        return f
