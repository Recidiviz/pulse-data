# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Unit tests for upserting terraform plan"""
import datetime
from unittest import TestCase

import freezegun
from mock import Mock, patch

from recidiviz.tests.ingest import fixtures
from recidiviz.tools.github.upsert_terraform_plan import main

PLAN_ERROR = "plan-error.txt"
PLAN_TOO_BIG = "plan-too-big.txt"
PLAN_SUCCESS = "plan-success.txt"
EMPTY = "empty.txt"


@freezegun.freeze_time(datetime.datetime(2025, 1, 1, 0, 0, 0, 0))
class TestUpsertTFPlan(TestCase):
    """Unit tests for terraform plan commenter."""

    def setUp(self) -> None:
        self.max_comment_patcher = patch(
            "recidiviz.tools.github.upsert_terraform_plan.TERRAFORM_PLAN_TEXT_MAX_LENGTH",
            2000,
        )
        self.max_comment_patcher.start()

        self.upsert_comment_patcher = patch(
            "recidiviz.tools.github.upsert_terraform_plan.upsert_helperbot_comment",
        )
        self.upsert_comment_mock = self.upsert_comment_patcher.start()

    def tearDown(self) -> None:
        self.max_comment_patcher.stop()
        self.upsert_comment_patcher.stop()

    @staticmethod
    def _get_args(*, plan_output_path: str, error_logs_path: str) -> Mock:
        mock_args = Mock()
        mock_args.terraform_plan_output_path = fixtures.as_filepath(plan_output_path)
        mock_args.terraform_plan_error_logs_path = (
            fixtures.as_filepath(error_logs_path)
            if error_logs_path
            else error_logs_path
        )
        mock_args.cloud_build_url = "https://www.build.url"
        mock_args.commit_ref = "refref"
        mock_args.pull_request_number = 123
        return mock_args

    def test_simple(self) -> None:

        args = self._get_args(plan_output_path=PLAN_SUCCESS, error_logs_path=EMPTY)
        main(args)

        expected_body = """# Terraform plan for `refref`





<details>
<summary><strong><em>generated on: 2025-01-01T00:00:00</em></strong></summary>

---

```terraform
# google_storage_bucket_object.recidiviz_source_file["recidiviz/tools/github/upsert_terraform_plan.py"] must be replaced
-/+ resource "google_storage_bucket_object" "recidiviz_source_file" {
      + content          = (sensitive value)
      ~ content_type     = "text/plain; charset=utf-8" -> (known after apply)
      ~ crc32c           = "xxx" -> (known after apply)
      ~ detect_md5hash   = "xxxxxxx" -> "different hash" # forces replacement
      - event_based_hold = false -> null
      ~ id               = "xxxxxxx/recidiviz/tools/github/upsert_terraform_plan.py" -> (known after apply)
      + kms_key_name     = (known after apply)
      ~ md5hash          = "xxxxxx" -> (known after apply)
      ~ media_link       = "https://link-to-media" -> (known after apply)
      - metadata         = {} -> null
        name             = "recidiviz/tools/github/upsert_terraform_plan.py"
      ~ output_name      = "recidiviz/tools/github/upsert_terraform_plan.py" -> (known after apply)
      ~ self_link        = "https://link-to-media" -> (known after apply)
      ~ storage_class    = "STANDARD" -> (known after apply)
      - temporary_hold   = false -> null
        # (2 unchanged attributes hidden)
    }

```
</details>


[Link to Cloud Build run](https://www.build.url)"""

        self.upsert_comment_mock.assert_called_with(
            pull_request_number=123,
            body=expected_body,
            prefix="# Terraform plan",
        )

    def test_with_failure(self) -> None:

        args = self._get_args(plan_output_path=EMPTY, error_logs_path=PLAN_ERROR)
        main(args)

        expected_body = """# Terraform plan for `refref`

## $${\\textbf{\\color{red}There was an error while generating the Terraform plan!}}$$
```terraform
Some unexpected error....
```






[Link to Cloud Build run](https://www.build.url)"""

        self.upsert_comment_mock.assert_called_with(
            pull_request_number=123,
            body=expected_body,
            prefix="# Terraform plan",
        )

    def test_with_truncate(self) -> None:

        args = self._get_args(plan_output_path=PLAN_TOO_BIG, error_logs_path=EMPTY)
        main(args)

        expected_body = """# Terraform plan for `refref`



## $${\\textbf{\\color{orange} Terraform plan is too long to comment in its entirety; please see cloud run build logs for full plan}}$$



<details>
<summary><strong><em>generated on: 2025-01-01T00:00:00</em></strong></summary>

---

```terraform
# google_storage_bucket_object.recidiviz_source_file["recidiviz/tools/github/upsert_terraform_plan.py"] must be replaced
-/+ resource "google_storage_bucket_object" "recidiviz_source_file" {
      + content          = (sensitive value)
      ~ content_type     = "text/plain; charset=utf-8" -> (known after apply)
      ~ crc32c           = "xxx" -> (known after apply)
      ~ detect_md5hash   = "xxxxxxx" -> "different hash" # forces replacement
      - event_based_hold = false -> null
      ~ id               = "xxxxxxx/recidiviz/tools/github/upsert_terraform_plan.py" -> (known after apply)
      + kms_key_name     = (known after apply)
      ~ md5hash          = "xxxxxx" -> (known after apply)
      ~ media_link       = "https://link-to-media" -> (known after apply)
      - metadata         = {} -> null
        name             = "recidiviz/tools/github/upsert_terraform_plan.py"
      ~ output_name      = "recidiviz/tools/github/upsert_terraform_plan.py" -> (known after apply)
      ~ self_link        = "https://link-to-media" -> (known after apply)
      ~ storage_class    = "STANDARD" -> (known after apply)
      - temporary_hold   = false -> null
        # (2 unchanged attributes hidden)
    }
# google_storage_bucket_object.recidiviz_source_file["recidiviz/tools/github/upsert_terraform_plan.py"] must be replaced
-/+ resource "google_storage_bucket_object" "recidiviz_source_file" {
      + content          = (sensitive value)
      ~ content_type     = "text/plain; charset=utf-8" -> (known after apply)
      ~ crc32c           = "xxx" -> (known after apply)
      ~ detect_md5hash   = "xxxxxxx" -> "different hash" # forces replacement
      - event_based_hold = false -> null
      ~ id               = "xxxxxxx/recidiviz/tools/github/upsert_terraform_plan.py" -> (known after apply)
      + kms_key_name     = (known after apply)
      ~ md5hash          = "xxxxxx" -> (known after apply)
      ~ media_link       = "h

[!!!] (truncated, see cloud build for full output)
```
</details>


[Link to Cloud Build run](https://www.build.url)"""

        self.upsert_comment_mock.assert_called_with(
            pull_request_number=123,
            body=expected_body,
            prefix="# Terraform plan",
        )
