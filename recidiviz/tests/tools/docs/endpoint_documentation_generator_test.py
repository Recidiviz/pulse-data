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
"""Tests for EndpointDocumentationGenerator"""

import os
import unittest
from typing import List

import attr
from werkzeug.routing import Rule

from recidiviz.tools.docs.endpoint_documentation_generator import (
    EndpointDocumentationGenerator,
)

DEFAULT_METHODS = ["GET", "HEAD", "OPTIONS"]
DOCS_DIRECTORY = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures")
SPECIFICATION_TEMPLATE = os.path.join(
    DOCS_DIRECTORY, "test_endpoint_specification_template.md"
)


@attr.s
class EndpointTestCase:
    endpoint: str = attr.ib()
    expected_branch: str = attr.ib()
    expected_leaf: str = attr.ib()
    expected_markdown_path: str = attr.ib()
    expected_documentation: str = attr.ib()
    expected_table_of_contents: str = attr.ib()
    expected_path_parameters: List[str] = attr.ib(default=[])

    def __attrs_post_init__(self) -> None:
        self.rule = Rule(string=self.endpoint, methods=DEFAULT_METHODS)


class EndpointDocumentationGeneratorTest(unittest.TestCase):
    """Tests for EndpointDocumentationGenerator"""

    TEST_CASES = [
        EndpointTestCase(
            endpoint="/single/",
            expected_branch="",
            expected_leaf="single",
            expected_markdown_path=f"{DOCS_DIRECTORY}/single.md",
            expected_documentation="""# /single/

- _Methods_: GET, HEAD, OPTIONS
- _Path Parameters_: """,
            expected_table_of_contents="""- [/single/](endpoints/single.md)
""",
        ),
        EndpointTestCase(
            endpoint="/single/<path:path>",
            expected_branch="single",
            expected_leaf="path",
            expected_path_parameters=["<path:path>"],
            expected_markdown_path=f"{DOCS_DIRECTORY}/single/path.md",
            expected_documentation="""# /single/&lt;path:path&gt;

- _Methods_: GET, HEAD, OPTIONS
- _Path Parameters_: &lt;path:path&gt;""",
            expected_table_of_contents="""- [/single/&lt;path:path&gt;](endpoints/single/path.md)
""",
        ),
        EndpointTestCase(
            endpoint="/nested/path_with_param/<path:path>",
            expected_branch="nested/path_with_param",
            expected_leaf="path",
            expected_path_parameters=["<path:path>"],
            expected_markdown_path=f"{DOCS_DIRECTORY}/nested/path_with_param/path.md",
            expected_documentation="""# /nested/path_with_param/&lt;path:path&gt;

- _Methods_: GET, HEAD, OPTIONS
- _Path Parameters_: &lt;path:path&gt;""",
            expected_table_of_contents="""- [/nested/path_with_param/&lt;path:path&gt;](endpoints/nested/path_with_param/path.md)
""",
        ),
        EndpointTestCase(
            endpoint="/very/nested/path",
            expected_branch="very/nested",
            expected_leaf="path",
            expected_markdown_path=f"{DOCS_DIRECTORY}/very/nested/path.md",
            expected_documentation="""# /very/nested/path

- _Methods_: GET, HEAD, OPTIONS
- _Path Parameters_: """,
            expected_table_of_contents="""- [/very/nested/path](endpoints/very/nested/path.md)
""",
        ),
        EndpointTestCase(
            endpoint="/very/nested/path_with_param/<path>",
            expected_branch="very/nested/path_with_param",
            expected_leaf="path",
            expected_path_parameters=["<path>"],
            expected_markdown_path=f"{DOCS_DIRECTORY}/very/nested/path_with_param/path.md",
            expected_documentation="""# /very/nested/path_with_param/&lt;path&gt;

- _Methods_: GET, HEAD, OPTIONS
- _Path Parameters_: &lt;path&gt;""",
            expected_table_of_contents="""- [/very/nested/path_with_param/&lt;path&gt;](endpoints/very/nested/path_with_param/path.md)
""",
        ),
        EndpointTestCase(
            endpoint="/very/nested/<param:filled>/path/<schema>",
            expected_branch="very/nested/param/path",
            expected_leaf="schema",
            expected_path_parameters=["<param:filled>", "<schema>"],
            expected_markdown_path=f"{DOCS_DIRECTORY}/very/nested/param/path/schema.md",
            expected_documentation="""# /very/nested/&lt;param:filled&gt;/path/&lt;schema&gt;

- _Methods_: GET, HEAD, OPTIONS
- _Path Parameters_: &lt;param:filled&gt;, &lt;schema&gt;""",
            expected_table_of_contents="""- [/very/nested/&lt;param:filled&gt;/path/&lt;schema&gt;](endpoints/very/nested/param/path/schema.md)
""",
        ),
    ]

    def test_endpoint_documentation_generation(self) -> None:
        generator = EndpointDocumentationGenerator()
        for testcase in self.TEST_CASES:
            self.assertEqual(
                generator.get_path_parameters(testcase.endpoint),
                testcase.expected_path_parameters,
            )
            self.assertEqual(
                generator.get_leaf_url(testcase.endpoint), testcase.expected_leaf
            )
            self.assertEqual(
                generator.get_branch_of_url(testcase.endpoint), testcase.expected_branch
            )
            self.assertEqual(
                generator.generate_documentation_markdown_for_endpoint(
                    specification_template_path=SPECIFICATION_TEMPLATE,
                    rule=testcase.rule,
                ),
                testcase.expected_documentation,
            )
            self.assertEqual(
                generator.generate_markdown_path_for_endpoint(
                    docs_directory=DOCS_DIRECTORY, endpoint=testcase.endpoint
                ),
                testcase.expected_markdown_path,
            )
            self.assertEqual(
                generator.generate_markdown_table_of_contents_for_endpoint(
                    markdown_path=generator.generate_markdown_path_for_endpoint(
                        docs_directory=DOCS_DIRECTORY, endpoint=testcase.endpoint
                    ),
                    docs_directory=DOCS_DIRECTORY,
                    endpoint=testcase.endpoint,
                ),
                testcase.expected_table_of_contents,
            )
