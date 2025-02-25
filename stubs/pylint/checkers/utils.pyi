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

from typing import Callable, Optional

from astroid import nodes
from astroid.context import InferenceContext
from astroid.typing import InferenceResult

def only_required_for_messages(*messages: str) -> Callable: ...
def safe_infer(
    node: nodes.NodeNG,
    context: Optional[InferenceContext] = ...,
    *,
    compare_constants: bool = ...
) -> Optional[InferenceResult]: ...
