from collections import Generator
from unittest.mock import patch

import pytest
from _pytest.config import Config
from _pytest.fixtures import FixtureRequest


@pytest.fixture(autouse=True)
def google_auth_patcher(
    pytestconfig: Config, request: FixtureRequest
) -> Generator[None, None, None]:
    """For tests without the emulator, prevent them from trying to create google cloud clients."""

    google_auth_patcher = None

    if (
        not pytestconfig.getoption("with_emulator", default=None)
        and "emulator" not in request.fixturenames
    ):
        google_auth_patcher = patch("google.auth.default")
        mock_google_auth = google_auth_patcher.start()
        mock_google_auth.side_effect = AssertionError(
            "Unit test may not instantiate a Google client. Please mock the appropriate client class inside this test "
            " (e.g. `patch('google.cloud.bigquery.Client')`)."
        )

    yield

    if google_auth_patcher is not None:
        google_auth_patcher.stop()
