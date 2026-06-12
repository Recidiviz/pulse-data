# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Local smoke test for the Edovo course-completion endpoint.

Hits the real Docker Postgres (case_triage_db on host port 5433) and mocks
BigQuery person resolution and Secret Manager.  Requires the case_triage_db
container to be running and the Alembic migrations to be at head.

The HMAC secret is generated fresh in-process each run; no GCP credentials
or GOOGLE_CLOUD_PROJECT are required.

Usage:
    uv run python -m recidiviz.tools.case_triage.edovo.smoke_test

Environment variables (all have defaults for the standard local Docker setup):
    CASE_TRIAGE_DB_HOST      default: localhost
    CASE_TRIAGE_DB_PORT      default: 5433
    CASE_TRIAGE_DB_USER      default: case_triage_user
    CASE_TRIAGE_DB_PASSWORD  default: example
    CASE_TRIAGE_DB_NAME      default: postgres
    EDOVO_HMAC_KEY_ID        default: local-test
    EDOVO_PERSON_ID          default: 9999001  (fake person_id returned by the mock)
"""
import base64
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
from unittest.mock import MagicMock, patch

from flask import Flask
from sqlalchemy.engine import URL

from recidiviz.case_triage.edovo.edovo_routes import create_edovo_api_blueprint
from recidiviz.case_triage.error_handlers import register_error_handlers
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_flask_utils import setup_scoped_sessions

_MODULE = "recidiviz.case_triage.edovo.edovo_routes"
_HMAC_MODULE = "recidiviz.case_triage.edovo.hmac_verifier"


def _build_payload() -> dict[str, object]:
    return {
        "person_id": "A123456",
        "state_code": "US_CO",
        # Randomize the course_id per run so reruns aren't dedup'd against
        # rows left behind by previous runs.
        "course_id": f"smoke-test-course-{secrets.token_hex(4)}",
        "course_name": "Smoke Test Course",
        "content_hours": 3.5,
        "completed_at": "2026-06-08T12:00:00Z",
    }


def _build_auth_header(body: bytes, key_id: str, secret_b64: str) -> str:
    timestamp = int(time.time())
    body_hash = hashlib.sha256(body).hexdigest()
    string_to_sign = f"POST\n/edovo/course-completions\n{timestamp}\n{body_hash}"
    secret = base64.b64decode(secret_b64)
    signature = base64.b64encode(
        hmac.new(secret, string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    return f"HMAC-SHA256 KeyId={key_id}, Signature={signature}, Timestamp={timestamp}"


def _db_url() -> str:
    return str(
        URL.create(
            drivername="postgresql",
            host=os.getenv("CASE_TRIAGE_DB_HOST", "localhost"),
            port=int(os.getenv("CASE_TRIAGE_DB_PORT", "5433")),
            username=os.getenv("CASE_TRIAGE_DB_USER", "case_triage_user"),
            password=os.getenv("CASE_TRIAGE_DB_PASSWORD", "example"),
            database=os.getenv("CASE_TRIAGE_DB_NAME", "postgres"),
        )
    )


def _check(label: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    suffix = f"  ({detail})" if detail else ""
    print(f"  [{status}] {label}{suffix}")
    if not condition:
        sys.exit(1)


def main() -> None:
    key_id = os.getenv("EDOVO_HMAC_KEY_ID", "local-test")
    person_id = os.getenv("EDOVO_PERSON_ID", "9999001")
    # Generate an ephemeral 256-bit HMAC secret in-process so the smoke test
    # has no dependency on real Secret Manager / GOOGLE_CLOUD_PROJECT.
    secret_b64 = base64.b64encode(secrets.token_bytes(32)).decode()

    app = Flask(__name__)
    register_error_handlers(app)
    db_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
    engine = setup_scoped_sessions(app, SchemaType.CASE_TRIAGE, _db_url())
    db_key.declarative_meta.metadata.create_all(engine)
    app.register_blueprint(create_edovo_api_blueprint(), url_prefix="/edovo")
    client = app.test_client()

    body = json.dumps(_build_payload()).encode()

    with patch(f"{_MODULE}.BigQueryClientImpl") as mock_bq_cls, patch(
        f"{_MODULE}.resolve_person_by_doc_id", return_value=person_id
    ), patch(f"{_HMAC_MODULE}.get_secret", return_value=secret_b64):
        mock_bq_cls.return_value = MagicMock()

        print("\n--- Edovo smoke test ---")

        # First POST → should be 201 CREATED
        auth = _build_auth_header(body, key_id, secret_b64)
        r1 = client.post(
            "/edovo/course-completions",
            data=body,
            content_type="application/json",
            headers={"Authorization": auth},
        )
        data1 = r1.get_json()
        print(f"\n  POST /edovo/course-completions  (first)")
        _check("status 201 CREATED", r1.status_code == 201, str(r1.status_code))
        _check("status=accepted", data1.get("status") == "accepted", str(data1))
        completion_id = data1.get("completion_id")
        _check("completion_id present", completion_id is not None, str(data1))
        print(f"       completion_id: {completion_id}")

        # Second POST with same payload → should be 200 DUPLICATE
        auth = _build_auth_header(body, key_id, secret_b64)
        r2 = client.post(
            "/edovo/course-completions",
            data=body,
            content_type="application/json",
            headers={"Authorization": auth},
        )
        data2 = r2.get_json()
        print(f"\n  POST /edovo/course-completions  (retry / duplicate)")
        _check("status 200 OK", r2.status_code == 200, str(r2.status_code))
        _check("status=duplicate", data2.get("status") == "duplicate", str(data2))
        _check(
            "same completion_id",
            data2.get("completion_id") == completion_id,
            str(data2),
        )

        # POST with missing field → should be 400
        bad_body = json.dumps(
            {k: v for k, v in _build_payload().items() if k != "course_id"}
        ).encode()
        auth = _build_auth_header(bad_body, key_id, secret_b64)
        r3 = client.post(
            "/edovo/course-completions",
            data=bad_body,
            content_type="application/json",
            headers={"Authorization": auth},
        )
        data3 = r3.get_json()
        print(f"\n  POST /edovo/course-completions  (missing course_id)")
        _check("status 400 BAD REQUEST", r3.status_code == 400, str(r3.status_code))
        _check(
            "error_code=VALIDATION_ERROR",
            data3.get("error_code") == "VALIDATION_ERROR",
            str(data3),
        )

    print("\n  All checks passed.\n")


if __name__ == "__main__":
    main()
