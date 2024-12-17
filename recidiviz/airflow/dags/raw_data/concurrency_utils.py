# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Constants and utility functions for limiting concurrency in the raw data import dag.

The constants in this file are set on a per-state level. Since we don't expect to be 
importing a large number of files on any given day, we don't expect to ever need to run 
more than the DEFAULT_*_NUM_TASKS for each sub-step. These constants are flexible mainly
for the purpose of being able to scale up during a secondary re-import where we do expect
a large volume of files.
"""

# NAIVE MAX FILES PER IMPORT
# TODO(#35694): make this value more dynamic
MAX_NUMBER_OF_FILES_FOR_TEN_WORKERS = 1500

# --------------------------------------------
# ------ PRE-IMPORT NORMALIZATION STEP -------
# --------------------------------------------
# -- PRE-IMPORT FILE CHUNKING STEP --
# During a typical import, we don't expect to ever need more than DEFAULT_CHUNKING_NUM_TASKS
# airflow tasks at once to complete all necessary file chunking work for a single state.
# We want to ensure that we don't exceed python's max command size when build the
# entrypoint arguments, which is about 1000 files per pod. If, in the course of batching
# the input files, we find more files than we can satisfy while maintaining
# MAX_CHUNKS_PER_CHUNKING_TASK, we will scale the number of tasks to a value greater than
# DEFAULT_CHUNKING_NUM_TASKS. We will scale the number of tasks as necessary, with each
# task having approximately MAX_CHUNKS_PER_CHUNKING_TASK. Irrespective of the number of
# total tasks we scale to, we will instruct airflow to not execute more than CHUNKING_MAX_CONCURRENT_TASKS
# mapped airflow tasks at one time to ensure that we do not overwhelm airflow.
DEFAULT_CHUNKING_NUM_TASKS = 10
CHUNKING_MAX_CONCURRENT_TASKS = 48
MAX_CHUNKS_PER_CHUNKING_TASK = 1000

# -- PRE-IMPORT CHUNK NORMALIZATION STEP --
# During a typical import, we don't expect to ever need more than DEFAULT_NORMALIZATION_NUM_TASKS
# airflow tasks at once to complete all necessary chunk normalization work for a single state.
# We want to ensure that we don't exceed python's max command size when build the
# entrypoint arguments, which is about 500 chunks per pod. If, in the course of batching
# the input file chunks, we find more chunks than we can satisfy while maintaining
# MAX_FILE_CHUNKS_PER_NORMALIZATION_TASK, we will scale the number of tasks to a value greater than
# DEFAULT_NORMALIZATION_NUM_TASKS. We will scale the number of tasks as necessary, with each
# task having approximately MAX_FILE_CHUNKS_PER_NORMALIZATION_TASK. Irrespective of the number of
# total tasks we scale to, we will instruct airflow to not execute more than NORMALIZATION_MAX_CONCURRENT_TASKS
# mapped airflow tasks at one time to ensure that we do not overwhelm airflow.
DEFAULT_NORMALIZATION_NUM_TASKS = 10
NORMALIZATION_MAX_CONCURRENT_TASKS = 64
MAX_FILE_CHUNKS_PER_NORMALIZATION_TASK = 500
# --------------------------------------------

# -----------------------------
# ------ BIG QUERY STEP -------
# -----------------------------
# -- BQ LOAD STEP --
# The BQ Load Step will distribute work across MAX_BQ_LOAD_AIRFLOW_TASKS mapped Airflow
# tasks running at the same time for a given state, with each task having up to
# MAX_BQ_LOAD_CLIENT_THREADS_PER_TASK concurrent load jobs running at the same time.
# Thus, we can run MAX_BQ_LOAD_CLIENT_THREADS_PER_TASK * MAX_BQ_LOAD_AIRFLOW_TASKS load
# jobs for a single state at a time.

# TODO(#36073) refine this default and consider expanding for secondary re-imports
MAX_BQ_LOAD_AIRFLOW_TASKS = 8
MAX_BQ_LOAD_CLIENT_THREADS_PER_TASK = 8

# -- BQ APPEND STEP --
# The BQ Append Step will distribute work across MAX_BQ_APPEND_AIRFLOW_TASKS mapped Airflow
# tasks running at the same time for a given state, with each task having up to
# MAX_BQ_APPEND_CLIENT_THREADS_PER_TASK concurrent load jobs running at the same time.
# Thus, we can run MAX_BQ_APPEND_CLIENT_THREADS_PER_TASK * MAX_BQ_APPEND_AIRFLOW_TASKS load
# jobs for a single state at a time.

# TODO(#36073) refine this default and consider expanding for secondary re-imports
MAX_BQ_APPEND_AIRFLOW_TASKS = 8
MAX_BQ_APPEND_CLIENT_THREADS_PER_TASK = 8
# -----------------------------
