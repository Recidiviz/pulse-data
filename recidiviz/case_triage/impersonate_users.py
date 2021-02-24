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
from typing import Any

from flask import redirect, request, session
from flask.views import View
from werkzeug.wrappers import Response

from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.exceptions import CaseTriageSecretForbiddenException


IMPERSONATED_EMAIL_KEY = 'impersonated_email'


class ImpersonateUser(View):
    """Implements a flask view for impersonating a user."""

    def __init__(self, redirect_url: str, authorization_store: AuthorizationStore):
        self.redirect_url = redirect_url
        self.authorization_store = authorization_store

    def dispatch_request(self, *_args: Any, **_kwargs: Any) -> Response:
        if 'user_info' not in session or session['user_info']['email'] not in self.authorization_store.admin_users:
            raise CaseTriageSecretForbiddenException()
        impersonated_email = request.args.get(IMPERSONATED_EMAIL_KEY)
        if impersonated_email:
            session[IMPERSONATED_EMAIL_KEY] = impersonated_email
        elif IMPERSONATED_EMAIL_KEY in session:
            session.pop(IMPERSONATED_EMAIL_KEY)
        return redirect(self.redirect_url)
