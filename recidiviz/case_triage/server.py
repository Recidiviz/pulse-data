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
"""Backend entry point for Case Triage API server."""
import json
import os
from typing import Dict

from flask import Flask, Response, g, jsonify, request, session
from flask_sqlalchemy_session import current_session, flask_scoped_session
from sqlalchemy.orm import sessionmaker

from recidiviz.case_triage.authorization import AuthorizationStore
from recidiviz.case_triage.case_updates.interface import CaseUpdatesInterface
from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.exceptions import (
    CaseTriageAuthorizationError,
    CaseTriageBadRequestException,
)
from recidiviz.case_triage.querier.querier import CaseTriageQuerier, PersonDoesNotExistError
from recidiviz.case_triage.util import get_local_secret
from recidiviz.persistence.database.base_schema import CaseTriageBase
from recidiviz.persistence.database.sqlalchemy_engine_manager import SQLAlchemyEngineManager, SchemaType
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils.auth.auth0 import (
    Auth0Config,
    get_userinfo,
    build_auth0_authorization_decorator
)
from recidiviz.utils.environment import in_development, in_test
from recidiviz.utils.flask_exception import FlaskException
from recidiviz.utils.timer import RepeatedTimer

static_folder = os.path.abspath(os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    "../../frontends/case-triage/build/",
))

app = Flask(__name__, static_folder=static_folder, static_url_path="/")
app.secret_key = get_local_secret("case_triage_secret_key")

if in_development():
    db_url = local_postgres_helpers.postgres_db_url_from_env_vars()
else:
    db_url = SQLAlchemyEngineManager.get_server_postgres_instance_url(schema_type=SchemaType.CASE_TRIAGE)
engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(db_url, CaseTriageBase)
session_factory = sessionmaker(bind=engine)
app.scoped_session = flask_scoped_session(session_factory, app)


def on_successful_authorization(_payload: Dict[str, str], token: str) -> None:
    """
    Memoize the user's info (email_address, picture, etc) into our session
    Expose the user on the flask request global
    """
    if 'user_info' not in session:
        session['user_info'] = get_userinfo(authorization_config.domain, token)

    if session['user_info']['email'] not in authorization_store.allowed_users:
        raise CaseTriageAuthorizationError(
            code="unauthorized",
            description="You are not authorized to access this application",
        )

    g.current_user = CaseTriageQuerier.officer_for_email(current_session, session['user_info']['email'])


auth0_configuration = get_local_secret("case_triage_auth0")

if not auth0_configuration:
    raise ValueError('Missing Case Triage Auth0 configuration secret')

authorization_store = AuthorizationStore()
authorization_config = Auth0Config(json.loads(auth0_configuration))
requires_authorization = build_auth0_authorization_decorator(authorization_config, on_successful_authorization)

store_refresh = RepeatedTimer(15 * 60, authorization_store.refresh, run_immediately=True)

if not in_test():
    store_refresh.start()


@app.errorhandler(FlaskException)
def handle_auth_error(ex: FlaskException) -> Response:
    response = jsonify({
        'code': ex.code,
        'description': ex.description,
    })
    response.status_code = ex.status_code
    return response


if not in_development():
    with open(os.path.join(static_folder, 'index.html'), 'r') as index_file:
        index_html = index_file.read()
else:
    index_html = ''


@app.route('/')
def index() -> str:
    return index_html


@app.route('/auth0_public_config.js')
def auth0_public_config() -> str:
    # Expose ONLY the necessary variables to configure our Auth0 frontend
    return f'window.AUTH0_CONFIG = {authorization_config.as_public_config()};'


@app.route('/api/clients')
@requires_authorization
def clients() -> str:
    return jsonify([
        client.to_json() for client in CaseTriageQuerier.clients_for_officer(
            current_session,
            g.current_user,
        )
    ])


@app.route('/api/record_client_action', methods=['POST'])
@requires_authorization
def record_client_action() -> str:
    """Records individual clients actions. Expects JSON body of the form:
    {
        personExternalId: str,
        actions: List[str],
        otherText: Optional[str],
    }
    `actions` must be non-empty and only contain approved actions
    """
    if 'personExternalId' not in request.json:
        raise CaseTriageBadRequestException(
            code='missing_arg',
            description='`personExternalId` keyword is missing from request',
        )
    if 'actions' not in request.json:
        raise CaseTriageBadRequestException(
            code='missing_arg',
            description='`actions` keyword is missing from request',
        )

    person_external_id = request.json['personExternalId']
    actions = request.json['actions']
    other_text = request.json.get('otherText')

    if not isinstance(actions, list):
        raise CaseTriageBadRequestException(
            code='improper_type',
            description='`actions` must be a list of CaseUpdateActionTypes',
        )

    try:
        user_initiated_actions = [CaseUpdateActionType(a) for a in actions]
    except ValueError as e:
        raise CaseTriageBadRequestException(
            code='improper_type',
            description='`actions` must be a list of CaseUpdateActionTypes',
        ) from e

    state_code = g.current_user.state_code
    try:
        client = CaseTriageQuerier.etl_client_with_id_and_state_code(
            current_session,
            person_external_id,
            state_code,
        )
    except PersonDoesNotExistError as e:
        raise CaseTriageBadRequestException(
            code='invalid_arg',
            description=f'`personExternalId` does not correspond to a known person: {person_external_id}',
        ) from e

    CaseUpdatesInterface.update_case_for_person(
        current_session,
        g.current_user,
        client,
        user_initiated_actions,
        other_text,
    )
    return ''
