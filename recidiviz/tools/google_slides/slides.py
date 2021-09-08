#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""
This module includes the GoogleSlidesManager class, which can be used to update a Google
Slides deck, including replacing text and figures, using Python.

The first time you initialize a GoogleSlidesManager, you'll need to download
credentials.json from here: https://console.developers.google.com/apis/credentials.
These are long-lasting credentials so put them in a permanent directory. Pass the path
to that directory when initializing a GoogleSlidesManager, e.g.:

manager = GoogleSlidesManager(path_to_credentials.json, slides_id)

See `./GoogleSlidesManager_example.ipynb` for example implementation.
"""

# necessary packages
from __future__ import print_function

import logging
import os.path
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import googleapiclient
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from recidiviz.utils.google_drive import get_credentials

GOOGLE_AUTHENTICATION_SCOPES = [
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/drive",
]


class GoogleSlidesManager:
    """
    Class for editing a Google Slides presentation
    """

    def __init__(self, credentials_path: str, slides_id: str) -> None:
        """
        Params
        ------
        credentials_path : str
            Local path to credentials.json

        slides_id : str
            ID for the Slides deck that the object should manage. Can be found in the
            Slide deck's URL.

        """
        # Class attributes
        self._slides_id: str = slides_id
        self._logger = logging.getLogger()

        # Get Google Slides credentials from local source
        try:
            credentials = get_credentials(
                credentials_path, readonly=False, scopes=GOOGLE_AUTHENTICATION_SCOPES
            )
        except IOError:
            print(
                f"Could not get credentials. Ensure credentials.json is in {credentials_path}."
            )
            return

        # Initialize Google Slides and Drive services
        self._slide_service: googleapiclient.discovery.Resource = build(
            "slides", "v1", credentials=credentials
        )
        self._drive_service: googleapiclient.discovery.Resource = build(
            "drive", "v3", credentials=credentials
        )

        # get parent directory of Slides deck
        self._directory_id: str = (
            self._drive_service.files()
            .get(
                fileId=self._slides_id,
                fields="parents",
            )
            .execute()["parents"][0]
        )

        # print relevant info
        print(
            f"""
Managing this presentation:
https://docs.google.com/presentation/d/{self._slides_id}

in this folder:
https://drive.google.com/drive/folders/{self._directory_id}
"""
        )

    def _get_slides_info(self) -> Dict[Any, Any]:
        """
        Gets slides info, e.g. to identify slide and image IDs
        """
        # send slide info request
        slides_info = (
            self._slide_service.presentations()
            .get(presentationId=self._slides_id, fields="slides")
            .execute()
            .get("slides", [])
        )

        return slides_info

    def copy_slides(
        self,
        new_slides_name: str,
        parent_directory_id: str,
        new_directory_name: Optional[str] = None,
        keep_managing_template: bool = False,
    ) -> None:
        """
        Copies the template slides to `parent_directory_id/new_directory_name`, if
        `new_directory_name`, otherwise copies to `parent_directory_id`.

        After calling this method, the initialized GoogleSlidesManager will replace the
        template with the newly created copy as the managed file (self._slides_id) unless
        `keep_managing_template` is True.

        Params
        ------
        new_slides_name : str
            Name of the new Slides deck copied from template

        parent_directory_id : str
            Google Drive ID of the parent directory, found in the folder's URL

        new_directory_name : Optional[str]
            Name of the new directory to place the copied template. If not specified,
            the template will be placed inside the parent directory.

        keep_managing_template : bool
            If True, does not replace self._slides_id with the newly copied Slides deck

        """
        # Create new directory, if necessary
        if new_directory_name:

            # set metadata for creating new directory
            new_directory_metadata = {
                "name": new_directory_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_directory_id],
            }

            # create new directory
            drive_folder = (
                self._drive_service.files()
                .create(
                    body=new_directory_metadata,
                )
                .execute()
            )
            print(f"Created directory {new_directory_name}.")

            # update parent_directory_id with newly created folder
            parent_directory_id = drive_folder["id"]

        # set metadata for creating copy of original Slides deck
        new_slides_metadata = {
            "name": new_slides_name,
            "parents": [parent_directory_id],
        }

        # create new Slides deck
        new_slide_deck = (
            self._drive_service.files()
            .copy(
                fileId=self._slides_id,
                body=new_slides_metadata,
            )
            .execute()
        )
        new_slides_id = new_slide_deck["id"]
        print(
            f"""
Copied template as {new_slides_name} at this address:
https://docs.google.com/presentation/d/{new_slides_id}

in this folder:
https://drive.google.com/drive/folders/{parent_directory_id}
            """
        )

        # replace self._slides_id and self._directory_id unless `keep_managing_template`
        if not keep_managing_template:
            self._slides_id = new_slides_id
            self._directory_id = parent_directory_id
        else:
            print("Caution, still managing template Slides deck.")

    def get_image_objects(
        self, slide_numbers: Optional[List[int]] = None
    ) -> Dict[int, List[str]]:
        """
        Returns a dict with a list of image object IDs in the specified slides, or all
        slides in the presentation if slide_numbers is None.

        The returned dict is of the form:

        {
            1: ['image_1_id', 'image_2_id'],
            ...,
            N: ['image_k-1_id', 'image_k_id']
         }

        Params
        ------
        slide_numbers : Optional[List[int]]
            The slides in which to look for image objects. If None, defaults to all
            slides in the presentation.

        """
        # set metadata for managed Slides deck
        slides_info = self._get_slides_info()

        # initialize dict to be returned
        image_dict: Dict[int, list] = {}

        # get slide numbers if not provided
        if not slide_numbers:
            slide_numbers = list(range(1, len(slides_info) + 1))

        # loop through slides in slide_numbers and get image objects
        for slide in slide_numbers:
            image_dict[slide] = []
            try:
                for element in slides_info[slide - 1]["pageElements"]:
                    if "image" in element:
                        image_dict[slide].append(element["objectId"])

            # if slide not found, throw warning
            except IndexError:
                image_dict.pop(slide)
                self._logger.warning(
                    "Cannot access slide %s. Verify that the slide exists.", slide
                )

        return image_dict

    def update_slides(
        self,
        slide_numbers: Optional[List[int]] = None,
        text_replace_dict: Optional[Dict[str, str]] = None,
        figure_replace_dict: Optional[Dict[Union[str, int], str]] = None,
    ) -> None:
        """
        Updates slide content by changing text and figures per the provided dicts.

        Params
        ------
        slide_numbers : Optional[List[int]]
            If specified, only updates content on the provided slide. Slide numbers
            are 1-indexed to match the Slides UI convention.

        text_replace_dict : Optional[Dict[str, str]]
            Dictionary where key is the source text and value is the replacement text

        figure_replace_dict : Optional[Dict[Union[int, str], str]]
            Dictionary where key either:
                str: the source image objectID
                int: the order of image objects as they appear in `slide_numbers`
            and the value is the file name (with path) of the new figure (saved locally).
        """

        # get slide info
        slide_info = self._get_slides_info()

        # if no slide_numbers, get all slide numbers
        if not slide_numbers:
            slide_numbers = list(range(len(slide_info)))

        # get pageObjectIds for specified slide_numbers
        slide_ids = []
        for slide in slide_numbers:
            slide_ids.append(slide_info[slide - 1].get("objectId"))

        # create replaceAllText requests
        replace_text_requests: List[Dict[str, Any]] = []
        if text_replace_dict:
            for key in text_replace_dict.keys():
                replace_text_requests.append(
                    {
                        "replaceAllText": {
                            "containsText": {
                                "text": key,
                                "matchCase": True,
                            },
                            "replaceText": text_replace_dict.get(key),
                            "pageObjectIds": slide_ids,
                        }
                    }
                )
            #
        #

        # initialize list of figure IDs that are uploaded
        uploaded_figure_ids = []

        # create replaceImage request
        replace_image_requests: List[Dict[str, Any]] = []
        if figure_replace_dict:

            # get list of imageObjectIds, in case integer keys provided
            image_dict = self.get_image_objects(slide_numbers)
            image_objects = []
            for image_id_list in image_dict.values():
                if image_id_list:
                    image_objects += image_id_list

            # iteratively map image placeholder to new figure
            for figure_key in figure_replace_dict.keys():

                # get imageObjectId, if integer key provided
                image_object_id = None
                if isinstance(figure_key, str):
                    image_object_id = figure_key
                elif isinstance(figure_key, int):
                    if figure_key <= len(image_objects) - 1:
                        image_object_id = image_objects[figure_key]

                # verify that image_object_id exists
                if (not image_object_id) or (image_object_id not in image_objects):
                    raise ValueError(
                        f"Cannot find image with key: {figure_key}. Verify that it exists."
                    )

                # check if file exists locally
                local_image_path = figure_replace_dict.get(figure_key)
                if not (
                    isinstance(local_image_path, str)
                    and os.path.isfile(local_image_path)
                ):
                    raise ValueError(
                        f"Cannot find image at filepath: {local_image_path}. Verify that it is correct."
                    )

                # (temporarily) upload image and get image ID and URL
                figure_id, figure_url = self.upload_figure(local_image_path)
                uploaded_figure_ids.append(figure_id)

                # add image request
                replace_image_requests.append(
                    {
                        "replaceImage": {
                            "imageObjectId": image_object_id,
                            "imageReplaceMethod": "CENTER_INSIDE",
                            "url": figure_url,
                        }
                    }
                )
            #
        #

        # make the images publicly readable (temporarily before deleting)
        # To upload an image using Google Slides API, it is required to make the file
        # publicly available: https://developers.google.com/slides/api/guides/add-image
        # Do this as a batch to limit requests.
        if uploaded_figure_ids:
            batch = self._drive_service.new_batch_http_request()
            user_permissions = {
                "role": "reader",
                "type": "anyone",
            }
            for figure_id in uploaded_figure_ids:
                batch.add(
                    self._drive_service.permissions().create(
                        fileId=figure_id,
                        body=user_permissions,
                        fields="",
                    )
                )
            batch.execute()

        # batch updating the slide
        all_requests = replace_text_requests + replace_image_requests

        # ensure all_requests is not empty
        if len(all_requests) == 0:
            self._logger.warning("Nothing to update: aborting slide update.")
            return

        # all_requests not empty, so give replacement requests a shot.
        # Because the Drive services are independent, might have to retry until
        # the new permissions take effect.
        # This processes uses exponential backoff & retry
        time_delay = 2
        retries = 0
        max_retries = 5
        while retries <= max_retries:
            try:
                self._slide_service.presentations().batchUpdate(
                    body={"requests": all_requests},
                    presentationId=self._slides_id,
                    fields="",
                ).execute()
                print("Google Slides deck updated!")
                break

            # handle failure
            except HttpError as err:

                # handle forbidden access - retry since permissions haven't yet changed
                if err.resp.status == 400:
                    if retries == max_retries:
                        self._logger.warning("Max retries reached, Slides NOT updated.")
                        break

                    # more retires left, sleep and then try again
                    time.sleep(time_delay)
                    retries += 1
                    time_delay = 2 ** (1 + retries)  # total max wait time 62 seconds

                # some other error that's not forbidden access
                else:
                    print(err)
                    self._logger.warning(
                        "Something went wrong. Slides NOT updated. HttpError code %s",
                        err.resp.status,
                    )
                    break

            # handle some other non-http error
            except Exception as e:
                self._logger.warning(
                    "Something went wrong. Slides NOT updated. Error: %s", e
                )

        # Regardless of success, delete temporarily uploaded figures
        if replace_image_requests:
            for figure_id in uploaded_figure_ids:
                self._drive_service.files().delete(fileId=figure_id).execute()
            print("All uploaded figures deleted.")

    def upload_figure(
        self,
        figure_file_path: str,
        parent_directory_id: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Uploads the file at the specified path and returns a tuple containing
        the object ID and URL.

        Params
        ------
        figure_file_path : str
            Path to the figure where stored locally

        parent_directory_id: Optional[str], default None
            ID for the Drive folder where the figure should be uploaded. Defaults to
            self._directory_id if None supplied.
        """
        # replace self._directory_id, if necessary
        parent_directory_id = (
            parent_directory_id if parent_directory_id else self._directory_id
        )

        # extract file name
        figure_file_name = figure_file_path[figure_file_path.rfind("/") + 1 :]

        # set file metadata (name and where to put it in Drive)
        file_metadata = {
            "name": figure_file_name,
            "parents": [parent_directory_id],
        }

        # set local file-specific metadata
        media_metadata = googleapiclient.http.MediaFileUpload(
            figure_file_path, mimetype="image/png"
        )

        # upload figure
        response = (
            self._drive_service.files()
            .create(body=file_metadata, media_body=media_metadata, fields="id")
            .execute()
        )
        figure_id = response["id"]

        # get the image url of uploaded figure
        figure_url = (
            self._drive_service.files()
            .get(fileId=figure_id, fields="webContentLink")
            .execute()["webContentLink"]
        )

        # Otherwise, print image location
        if parent_directory_id != self._directory_id:
            print(
                f"""
Uploaded {figure_file_name} to this folder:
https://drive.google.com/drive/folders/{parent_directory_id}
                """
            )

        else:
            print(f"Uploaded figure {figure_file_name}")

        return figure_id, figure_url
