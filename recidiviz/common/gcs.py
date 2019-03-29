# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""GCS Utils."""
from google.cloud import storage


_client = None
def client():
    global _client
    if not _client:
        _client = storage.Client()
    return _client


def list_blobs(bucket_name, prefix):
    bucket = client().get_bucket(bucket_name)
    return [blob.name for blob in bucket.list_blobs(prefix=prefix)]


def download_to_file(bucket_name, blob_name, local_path):
    bucket = client().get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)


def copy(bucket_name, blob_name, new_bucket_name, new_blob_name):
    source_bucket = client().get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = client().get_bucket(new_bucket_name)

    source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)


def delete(bucket_name, blob_name):
    bucket = client().get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()


def move(bucket_name, blob_name, new_bucket_name, new_blob_name):
    copy(bucket_name, blob_name, new_bucket_name, new_blob_name)
    delete(bucket_name, blob_name)
