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
"""Tests for slides functionality"""

# disable warnings caused by mock and accessing protected class attributes
# pylint: disable=W0613, W0212

import unittest

from google.auth.credentials import Credentials
from mock import Mock, patch

from recidiviz.tools.google_slides.slides import GoogleSlidesManager

# fake objects
FAKE_DIRECTORY_ID = "abc123_directory"
FAKE_SLIDES_ID = "abc123_slides"
FAKE_NEW_SLIDES_ID = "abc123_slides_new"
FAKE_IMAGE_ID = "abc123_image"
FAKE_IMAGE_URL = "abc123_url"
FAKE_SLIDES_INFO = [{"pageElements": [{"objectId": FAKE_IMAGE_ID, "image": {""}}]}]
FAKE_FILE_NAME = "abc123.png"
FAKE_FILE_PATH = "abc123_path/" + FAKE_FILE_NAME


class TestGoogleSlidesManager(unittest.TestCase):
    """Tests for managing google slides"""

    def setUp(self) -> None:
        # Mock out the Drive and Slides services
        self.discovery_patcher = patch("recidiviz.tools.google_slides.slides.build")
        self.mock_discovery = self.discovery_patcher.start()

        self.mock_drive_service = Mock()
        self.mock_slides_service = Mock()

        def get_mock_service(
            service_name: str, version: str, credentials: Credentials
        ) -> Mock:
            if service_name == "drive":
                return self.mock_drive_service
            if service_name == "slides":
                return self.mock_slides_service
            raise ValueError(f"Unknown service {service_name}")

        self.mock_discovery.side_effect = get_mock_service

        # Mock others
        self.credentials_patcher = patch(
            "recidiviz.tools.google_slides.slides.get_credentials"
        )
        self.credentials_patcher.start()

        # Mocks used across multiple tests
        # mock return from getting _directory_id from __init__
        self.mock_drive_service.files.return_value.get.return_value.execute.return_value = {
            "parents": [FAKE_DIRECTORY_ID]
        }
        # mock return from _get_slides_info
        self.mock_slides_service.presentations.return_value.get.return_value.execute.return_value.get.return_value = (
            FAKE_SLIDES_INFO
        )

    def tearDown(self) -> None:
        self.discovery_patcher.stop()
        self.credentials_patcher.stop()

    def test_copy_slides(self) -> None:
        manager = GoogleSlidesManager("", FAKE_SLIDES_ID)

        self.mock_drive_service.files.return_value.copy.return_value.execute.return_value = {
            "id": FAKE_NEW_SLIDES_ID
        }

        # Act
        manager.copy_slides("New Slides", parent_directory_id=FAKE_DIRECTORY_ID)

        # Assert
        self.mock_drive_service.files.return_value.copy.assert_called_with(
            fileId=FAKE_SLIDES_ID,
            body={"name": "New Slides", "parents": [FAKE_DIRECTORY_ID]},
        )
        self.assertEqual(manager._slides_id, FAKE_NEW_SLIDES_ID)

    def test_copy_slide_keep_managing_template(self) -> None:
        manager = GoogleSlidesManager("", FAKE_SLIDES_ID)

        self.mock_drive_service.files.return_value.copy.return_value.execute.return_value = {
            "id": FAKE_NEW_SLIDES_ID
        }

        # Act
        manager.copy_slides(
            "New Slides",
            parent_directory_id=FAKE_DIRECTORY_ID,
            keep_managing_template=True,
        )

        # Assert
        self.mock_drive_service.files.return_value.copy.assert_called_with(
            fileId=FAKE_SLIDES_ID,
            body={"name": "New Slides", "parents": [FAKE_DIRECTORY_ID]},
        )
        self.assertEqual(manager._slides_id, FAKE_SLIDES_ID)

    def test_get_image_objects(self) -> None:
        # init manager
        manager = GoogleSlidesManager("", FAKE_SLIDES_ID)

        # Act
        image_dict = manager.get_image_objects(slide_numbers=[1])

        # Assert correct response
        self.assertEqual(image_dict, {1: [FAKE_IMAGE_ID]})

        # Assert correct warning when index out of range
        with self.assertLogs(level="WARNING") as log:
            slide_number = 2  # out of range, only one slide in FAKE_SLIDES_INFO
            manager.get_image_objects(slide_numbers=[slide_number])
            self.assertEqual(
                log.output[0],
                f"WARNING:root:Cannot access slide {slide_number}. Verify that the slide exists.",
            )

    # mock googleapiclient to avoid looking for FAKE_IMAGE_PATH
    @patch("recidiviz.tools.google_slides.slides.googleapiclient")
    def test_upload_figure(self, mock_api_client: Mock) -> None:
        # init manager
        manager = GoogleSlidesManager("", FAKE_SLIDES_ID)

        # Arrange
        self.mock_drive_service.files.return_value.create.return_value.execute.return_value = {
            "id": FAKE_IMAGE_ID
        }
        self.mock_drive_service.files.return_value.get.return_value.execute.return_value = {
            "webContentLink": FAKE_IMAGE_URL
        }

        # Act
        figure_id, figure_url = manager.upload_figure(FAKE_FILE_PATH)

        # called-with objects
        file_metadata = {
            "name": FAKE_FILE_NAME,
            "parents": [FAKE_DIRECTORY_ID],
        }
        media_metadata = mock_api_client.http.MediaFileUpload(
            FAKE_FILE_PATH, mimetype="image/png"
        )

        # Asserts
        # check that _drive_service.files().create() called correctly
        self.mock_drive_service.files.return_value.create.assert_called_with(
            body=file_metadata,
            media_body=media_metadata,
            fields="id",
        )
        # check that _drive_service.files().get() called correctly
        self.mock_drive_service.files.return_value.get.assert_called_with(
            fileId=FAKE_IMAGE_ID,
            fields="webContentLink",
        )
        # check that output as expected
        self.assertEqual(figure_id, FAKE_IMAGE_ID)
        self.assertEqual(figure_url, FAKE_IMAGE_URL)

    def test_update_slides_empty(self) -> None:
        # init manager
        manager = GoogleSlidesManager("", FAKE_SLIDES_ID)

        # Assert correct warning when attempting to update nothing
        with self.assertLogs(level="WARNING") as log:
            manager.update_slides()
            print(log.output[0])
            self.assertEqual(
                log.output[0], "WARNING:root:Nothing to update: aborting slide update."
            )

    def test_update_slides_text(self) -> None:
        # init manager
        manager = GoogleSlidesManager("", FAKE_SLIDES_ID)

        # update just text
        manager.update_slides(text_replace_dict={"abc": "123"})

        # intermediate dictionary sent in batch request
        replace_text_requests = [
            {
                "replaceAllText": {
                    "containsText": {"text": "abc", "matchCase": True},
                    "replaceText": "123",
                    "pageObjectIds": [None],
                }
            }
        ]

        # Assert everything called that should be called
        # _get_slides_info
        self.mock_slides_service.presentations.return_value.get.assert_called_with(
            presentationId=FAKE_SLIDES_ID,
            fields="slides",
        )

        # batch http request not called (because no images uploaded)
        self.mock_drive_service.new_batch_http_request.return_value.execute.assert_not_called()

        # batch update
        self.mock_slides_service.presentations.return_value.batchUpdate.assert_called_with(
            body={"requests": replace_text_requests},
            presentationId=FAKE_SLIDES_ID,
            fields="",
        )

        # Assert image deletion not called
        self.mock_drive_service.files.return_value.delete.assert_not_called()

    # mock os to avoid looking for FAKE_IMAGE_PATH
    @patch("recidiviz.tools.google_slides.slides.os")
    # set return values for upload_figure
    @patch(
        "recidiviz.tools.google_slides.slides.GoogleSlidesManager.upload_figure",
        return_value=(FAKE_IMAGE_ID, FAKE_IMAGE_URL),
    )
    def test_update_slides_image(self, mock_os: Mock, mock_upload_figure: Mock) -> None:
        # init manager
        manager = GoogleSlidesManager("", FAKE_SLIDES_ID)

        # update just image
        manager.update_slides(figure_replace_dict={0: FAKE_FILE_PATH})

        # intermediate dictionary sent in batch request
        replace_image_requests = [
            {
                "replaceImage": {
                    "imageObjectId": FAKE_IMAGE_ID,
                    "imageReplaceMethod": "CENTER_INSIDE",
                    "url": FAKE_IMAGE_URL,
                }
            }
        ]

        # Assert everything called that should be called
        # _get_slides_info
        self.mock_slides_service.presentations.return_value.get.assert_called_with(
            presentationId=FAKE_SLIDES_ID,
            fields="slides",
        )

        # batch http request
        self.mock_drive_service.new_batch_http_request.return_value.execute.assert_called()

        # batch update
        self.mock_slides_service.presentations.return_value.batchUpdate.assert_called_with(
            body={"requests": replace_image_requests},
            presentationId=FAKE_SLIDES_ID,
            fields="",
        )

        # figure deletion
        self.mock_drive_service.files.return_value.delete.assert_called_with(
            fileId=FAKE_IMAGE_ID
        )
