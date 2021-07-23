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
from typing import Any, Callable, Optional

from flask import g, jsonify, redirect, request, session
from flask.views import View
from flask_sqlalchemy_session import current_session
from werkzeug.wrappers import Response

from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.exceptions import CaseTriageSecretForbiddenException
from recidiviz.case_triage.querier.querier import CaseTriageQuerier

IMPERSONATED_EMAIL_KEY = "impersonated_email"


class ImpersonateUser(View):
    """Implements a flask view for impersonating a user."""

    def __init__(
        self,
        redirect_url: str,
        authorization_store: AuthorizationStore,
        authorization_decorator: Callable,
    ):
        self.redirect_url = redirect_url
        self.authorization_store = authorization_store
        self.authorization_decorator = authorization_decorator

    def _set_impersonation(self) -> Response:
        """Appropriately reroutes the request if a user requests to impersonate
        another user and has the permissions to do so."""
        if not hasattr(g, "user_context"):
            raise CaseTriageSecretForbiddenException()

        impersonated_email: Optional[str] = None
        if request.method == "GET" and IMPERSONATED_EMAIL_KEY in request.args:
            impersonated_email = request.args.get(IMPERSONATED_EMAIL_KEY)
        elif request.method == "POST" and request.json:
            impersonated_email = request.json.get(IMPERSONATED_EMAIL_KEY)

        if impersonated_email and g.user_context.can_impersonate(
            CaseTriageQuerier.officer_for_email(current_session, impersonated_email)
        ):
            session[IMPERSONATED_EMAIL_KEY] = impersonated_email.lower()
        elif IMPERSONATED_EMAIL_KEY in session:
            session.pop(IMPERSONATED_EMAIL_KEY)

        if request.method == "POST":
            return jsonify({"status": "ok"})
        return redirect(self.redirect_url)

    def dispatch_request(self, *_args: Any, **_kwargs: Any) -> Response:
        return self.authorization_decorator(self._set_impersonation)()


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
