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
"""Implements a flask view for impersonating a user."""
from typing import Any, Callable

from flask import redirect, session
from flask.views import View
from werkzeug.wrappers import Response

from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.exceptions import CaseTriageSecretForbiddenException


class RefreshAuthStore(View):
    """Implements a flask view for manually refreshing the auth store."""

    def __init__(
        self,
        redirect_url: str,
        authorization_store: AuthorizationStore,
        authorization_decorator: Callable,
    ):
        self.redirect_url = redirect_url
        self.authorization_store = authorization_store
        self.authorization_decorator = authorization_decorator

    def _set_refresh(self) -> Response:
        if (
            "user_info" not in session
            or not self.authorization_store.can_refresh_auth_store(
                session["user_info"]["email"]
            )
        ):
            raise CaseTriageSecretForbiddenException()
        self.authorization_store.refresh()
        return redirect(self.redirect_url)

    def dispatch_request(self, *_args: Any, **_kwargs: Any) -> Response:
        return self.authorization_decorator(self._set_refresh)()
