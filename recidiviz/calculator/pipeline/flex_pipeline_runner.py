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
"""Util for launching a single Dataflow pipeline with flex templates."""
from __future__ import absolute_import

import argparse
import logging
import sys
from typing import List, Type

from recidiviz.calculator.pipeline.base_pipeline import BasePipeline
from recidiviz.calculator.pipeline.utils.pipeline_run_utils import (
    collect_all_pipeline_classes,
)


def pipeline_cls_for_pipeline_name(pipeline_name: str) -> Type[BasePipeline]:
    """Finds the Pipeline class corresponding to the pipeline with the given |pipeline_name|."""
    all_pipelines = collect_all_pipeline_classes()
    pipeline_with_name = [
        pipeline
        for pipeline in all_pipelines
        if pipeline.pipeline_name().lower() == pipeline_name
    ]

    if len(pipeline_with_name) != 1:
        raise ValueError(
            f"Expected exactly one Pipeline with the pipeline_name: {pipeline_name}. Found: {pipeline_with_name}"
        )

    return pipeline_with_name[0]


def run_flex_pipeline(pipeline_name: str, argv: List[str]) -> None:
    """Runs the given pipeline_module with the arguments contained in argv."""
    pipeline_cls = pipeline_cls_for_pipeline_name(pipeline_name)
    pipeline_cls.build_from_args(argv).run()


# TODO(#18108): consider creating a main for each type of pipeline (metric, normalization, supplemental)
# to avoid needing to dynamically collect delegates to use one
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pipeline", dest="pipeline", type=str, help="name of pipeline to run"
    )

    args, other_args = parser.parse_known_args()

    run_flex_pipeline(pipeline_name=args.pipeline, argv=sys.argv[1:])
