# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Utility for instrumenting our flask applications"""
from flask import Flask
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient

# Ignore: Mypy: Module "opentelemetry.instrumentation.instrumentor" has no attribute "BaseInstrumentor" [attr-defined]
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor


def instrument_common_libraries() -> None:
    instrumentor_integrations: list[tuple[type[BaseInstrumentor], dict]] = [
        (GrpcInstrumentorClient, {}),
        (RedisInstrumentor, {}),
        (RequestsInstrumentor, {}),
        (SQLAlchemyInstrumentor, {}),
    ]

    for instrumentor, kwargs in instrumentor_integrations:
        instrumentor().instrument(**kwargs)


def instrument_flask_app(app: Flask) -> None:
    """Instruments a Flask app by automatically creating Traces for incoming requests
    Additionally, adds spans for commonly used libraries to the trace"""
    instrument_common_libraries()

    FlaskInstrumentor.instrument_app(app)
