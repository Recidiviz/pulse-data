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

from http import HTTPStatus
from typing import Any, Callable, List, NoReturn, Optional, Type, Union

from flask import Blueprint as FlaskBlueprint
from flask import Flask
from marshmallow import Schema

def abort(
    http_status_code: int, exc: Optional[Exception] = None, **kwargs: Any
) -> NoReturn: ...

class ArgumentsMixin:
    def arguments(
        self,
        schema: Type[Schema],
        *,
        location: str = "json",
        content_type: Optional[str] = None,
        required: bool = True,
        description: Optional[str] = None,
        example: Optional[dict] = None,
        examples: Optional[dict] = None,
        headers: Optional[dict] = None,
        **kwargs: Any
    ) -> Callable: ...

class ResponseMixin:
    def response(
        self,
        status_code: Union[int, str, HTTPStatus],
        schema: Optional[Union[Type[Schema], Schema]] = None,
        *,
        content_type: Optional[str] = None,
        description: Optional[str] = None,
        example: Optional[dict] = None,
        examples: Optional[dict] = None,
        headers: Optional[dict] = None
    ) -> Callable: ...

class Blueprint(FlaskBlueprint, ArgumentsMixin, ResponseMixin):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def route(
        self,
        rule: str,
        *,
        parameters: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        **options: Any
    ) -> Callable: ...

class Api:
    def __init__(
        self, app: Optional[Flask], *, spec_kwargs: dict[str, Any]
    ) -> None: ...
    def register_blueprint(
        self, blp: Blueprint, *, parameters: Optional[List[str]] = None, **options: Any
    ) -> None: ...
