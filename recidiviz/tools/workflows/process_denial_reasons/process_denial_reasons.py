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
"""
Script to process denial reasons from an input file through an LLM instructed to enumerate all
denial reasons that apply to an opportunity, given the user provided reason(s) and free-form "Other" text.

A pre-defined set of clustered denial reasons is provided to the LLM, and it produces an output file which
labels each opportunity with an explanation of category (cluster) to enable structured analysis

Test run:
python -m recidiviz.tools.workflows.process_denial_reasons.process_denial_reasons \
    --input input.json \
    --output test_output.json \
    --start_row 0 \
    --num_rows 5 \
    --project_id recidiviz-staging \
    --location us-central1 \
    --max_retries 3 \
    --model_id 'gemini-2.0-flash-001' \
    
Full run: 
python -m recidiviz.tools.workflows.process_denial_reasons.process_denial_reasons \
    --input input.json \
    --output test_output.json \
    --start_row 0 \
    --num_rows 0 \
    --project_id recidiviz-staging \
    --location us-central1 \
    --max_retries 5 \
    --model_id 'gemini-2.0-flash-001' \
    --parallel_requests 10

You'll want to: 
- Ensure the location is one where the GCP AI model you want to use is deployed (see https://cloud.google.com/vertex-ai/generative-ai/docs/learn/locations)
"""
import argparse
import json
import logging
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

import vertexai
from tqdm import tqdm
from vertexai.generative_models import GenerativeModel

import recidiviz
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tools.workflows.process_denial_reasons.prompt import (
    possible_denial_reasons_dict,
    prompt_template,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter

PROCESS_DENIAL_REASONS_BUCKET = "recidiviz-staging-process-denial-reasons"

clusters_path = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "tools/workflows/process_denial_reasons/clusters.json",
)

SingularCluster = dict[str, str]
Cluster = dict[str, list[str]]


def validate_clusters(
    assigned_clusters: List[SingularCluster], opportunity_clusters: Cluster
) -> Tuple[List[dict[str, str]], List[dict[str, str]]]:
    """Function to validate clusters"""
    valid: List[SingularCluster] = []
    invalid: List[SingularCluster] = []
    for cluster in assigned_clusters:
        if isinstance(cluster, dict):
            for key, value in cluster.items():
                if key in opportunity_clusters and value in opportunity_clusters[key]:
                    valid.append(cluster)
                else:
                    invalid.append(cluster)
        else:
            invalid.append(cluster)
    return valid, invalid


def validate_denial_reasons(
    assigned_reasons: List[str], possible_reasons: List[str]
) -> Tuple[List[str], List[str]]:
    """Function to validate denial reasons"""
    valid = [
        reason for reason in assigned_reasons if reason.upper() in possible_reasons
    ]
    invalid = [
        reason for reason in assigned_reasons if reason.upper() not in possible_reasons
    ]
    return valid, invalid


def call_gemini_api(
    prompt: str, max_retries: int, model_id: str
) -> Tuple[Optional[str], int]:
    """Function to call Gemini API"""
    for attempt in range(max_retries):
        try:
            chat_model = GenerativeModel(model_id)
            chat = chat_model.start_chat()
            response = chat.send_message(prompt)
            return response.text, response.usage_metadata.total_token_count
        except Exception as e:
            # Log the error and attempt number
            logging.warning("API call failed on attempt %i: %s", attempt + 1, e)

            # Check if it's a resource exhaustion error
            if "resource exhausted" in str(e).lower():
                # Exponential backoff with jitter
                backoff_time = random.uniform(
                    0, (2**attempt)
                )  # Random delay between 0 and 2^attempt seconds
                logging.info(
                    "Resource exhausted. Retrying in %f seconds...",
                    round(backoff_time, 2),
                )
                time.sleep(backoff_time)
            else:
                # For non-resource exhaustion errors, apply a standard exponential backoff
                backoff_time = 2**attempt
                logging.info("Retrying in %f seconds...", backoff_time)
                time.sleep(backoff_time)

    logging.error("API call failed after %i attempts.", max_retries)
    return None, 0


def process_indices(output_data: list[str]) -> Set:
    """Process the existing output file to determine if prior run has already processed some rows"""
    processed_indices = set()
    # Check if prior run has already processed some rows
    for line in output_data:
        try:
            processed_row = json.loads(line)
            processed_indices.add(
                processed_row["index"]
            )  # Assuming the index is stored in the row
        except json.JSONDecodeError:
            logging.warning("Skipping a corrupted line in the output file")
    return processed_indices


def process_row(
    idx: int,
    row: dict,
    clusters: dict,
    processed_indices: set,
    max_retries: int,
    model_id: str,
) -> Tuple[Optional[Dict[Any, Any]], int]:
    """Function to process a single row"""
    if idx in processed_indices:  # Skip rows already processed
        logging.info("Skipping already processed row %i", idx)
        return None, 0

    retries = 0
    total_tokens_used = 0
    while retries < max_retries:
        free_text_reason = row.get("Free Text", "")
        selected_denial_reasons = row.get("Ineligibility Reasons", [])
        opportunity_type = row.get("opportunity_type", "")

        # Ensure selected_denial_reasons is a list
        if isinstance(selected_denial_reasons, str):
            selected_denial_reasons = [selected_denial_reasons]

        # Get possible denial reasons and clusters
        possible_denial_reasons = possible_denial_reasons_dict.get(
            opportunity_type, ["OTHER"]
        )
        opportunity_clusters = clusters.get(opportunity_type, {})
        clusters_formatted = "\n".join(
            f"{high_level}:\n  - " + "\n  - ".join(low_levels)
            for high_level, low_levels in opportunity_clusters.items()
        )

        # Prepare the prompt
        prompt = StrictStringFormatter().format(
            prompt_template,
            opportunity_type=opportunity_type,
            free_text_reason=free_text_reason,
            selected_denial_reasons=selected_denial_reasons,
            possible_denial_reasons="\n".join(
                f"- {reason}" for reason in possible_denial_reasons
            ),
            clusters_formatted=clusters_formatted,
        )

        # Call the API
        response_text, tokens_used = call_gemini_api(
            prompt, max_retries=max_retries, model_id=model_id
        )
        total_tokens_used += tokens_used

        if response_text:
            json_str = None
            # Use regex to extract JSON from response
            match = re.search(r"\{.*\}", response_text, re.DOTALL)
            if match:
                json_str = match.group(0)
            else:
                logging.error("No JSON object found in response at row %i", idx)
                json_str = response_text.strip()  # Fallback to the entire response

            if json_str is None:
                logging.error("No JSON string found for row %i", idx)
                json_str = response_text.strip()

            try:
                result = json.loads(json_str)
                # Copy original denial reasons into the result in case LLM did not follow those instructions
                result["assigned_denial_reasons"] = list(
                    set(
                        reason.upper()
                        for reason in result.get("assigned_denial_reasons")
                        + row.get("Ineligibility Reasons")
                    )
                )

                # Validate clusters and denial reasons
                valid_clusters, invalid_clusters = validate_clusters(
                    result.get("assigned_clusters", []), opportunity_clusters
                )
                valid_reasons, invalid_reasons = validate_denial_reasons(
                    result.get("assigned_denial_reasons", []), possible_denial_reasons
                )

                if invalid_clusters or invalid_reasons:
                    logging.warning(
                        "Row %i validation failed. Retrying... "
                        "Invalid clusters: %s, Invalid reasons: %s. "
                        "Full response: %s",
                        idx,
                        invalid_clusters,
                        invalid_reasons,
                        json.dumps(result, indent=2),
                    )
                    retries += 1
                    time.sleep(2**retries)
                    continue

                # Add the validated result to the row
                result["assigned_clusters"] = valid_clusters
                result["assigned_denial_reasons"] = valid_reasons
                row["processed_result"] = result
                row["index"] = idx  # Add index for tracking

                logging.info("Processed and saved row %i", idx)
                return row, total_tokens_used

            except json.JSONDecodeError as e:
                logging.error(
                    "JSON parsing error at row %i: %s. Response: %s", idx, e, json_str
                )
                retries += 1
                time.sleep(2**retries)
                continue

        else:
            logging.error("Failed to process row %i: Empry response", idx)
            retries += 1
            time.sleep(2**retries)
            continue

    logging.error("Skipping row %i after %i retries", idx, max_retries)
    return None, total_tokens_used


def main(  # pylint: disable=too-many-positional-arguments
    project_id: str,
    location: str,
    input_file: str,
    output_file: str,
    num_rows: int,
    start_row: int,
    parallel_requests: int,
    cost_per_token: float,
    max_retries: int,
    model_id: str,
) -> None:
    """Load and process input data"""

    # Initialize Vertex AI
    vertexai.init(project=project_id, location=location)

    # Load clusters
    with open(clusters_path, "r", encoding="utf-8") as f:
        clusters_data = json.load(f)
    clusters = clusters_data["clusters"]

    gcsfs_client = GcsfsFactory.build()

    intput_uri = GcsfsFilePath.from_absolute_path(
        f"gs://{PROCESS_DENIAL_REASONS_BUCKET}/{input_file}"
    )
    input_data = json.loads(gcsfs_client.download_as_string(intput_uri))

    output_uri = GcsfsFilePath.from_absolute_path(
        f"gs://{PROCESS_DENIAL_REASONS_BUCKET}/{output_file}"
    )
    output_data = []
    if gcsfs_client.exists(output_uri):
        with gcsfs_client.open(output_uri) as f:
            output_data = [line.replace("\n", "") for line in f]

    # Check if prior run has already processed some rows
    processed_indices = process_indices(output_data)

    # Handle full input processing based on num_rows
    if num_rows in (0, -1):  # Check for the "process all rows" condition
        input_data = input_data[
            start_row:
        ]  # Slice from start_row to the end of the file
    else:
        input_data = input_data[start_row : start_row + num_rows]

    # Process data in parallel
    total_tokens_used = 0

    with ThreadPoolExecutor(max_workers=parallel_requests) as executor:
        futures = {
            executor.submit(
                process_row,
                idx + start_row,
                row,
                clusters,
                processed_indices,
                max_retries,
                model_id,
            ): row
            for idx, row in enumerate(input_data)
        }
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Processing rows"
        ):
            processed_row, tokens_used = future.result()
            total_tokens_used += tokens_used
            if processed_row:
                output_data.append(json.dumps(processed_row))

    gcsfs_client.upload_from_string(
        path=output_uri,
        contents="\n".join(output_data),
        content_type="application/json",
    )

    # Calculate and log the cost
    total_cost = total_tokens_used * cost_per_token
    logging.info("Total tokens used: %o", total_tokens_used)
    logging.info("Estimated total cost: $%f", round(total_cost, 4))


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments"""
    parser = argparse.ArgumentParser(description="Process denial reasons.")
    parser.add_argument(
        "--input", required=True, help="Input JSON file located in GCS bucket"
    )
    parser.add_argument(
        "--output", required=True, help="Output JSON file located in GCS bucket"
    )
    parser.add_argument(
        "--start_row", type=int, default=0, help="Row number to start processing from"
    )
    parser.add_argument(
        "--num_rows",
        type=int,
        default=100,
        help="Number of rows to process. Use 0 or -1 to process all rows.",
    )
    parser.add_argument(
        "--project_id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project against which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--location", required=True, help="Location of your model (e.g., us-central1)"
    )
    parser.add_argument(
        "--model_id",
        default="publishers/google/models/gemini",
        help="Model ID of Gemini model",
    )
    parser.add_argument(
        "--max_retries",
        type=int,
        default=3,
        help="Maximum number of retries for API calls and validation",
    )
    parser.add_argument(
        "--cost_per_token",
        type=float,
        default=0.000000375,
        help="Cost per token in dollars",
    )
    parser.add_argument(
        "--parallel_requests",
        type=int,
        default=1,
        help="Number of simultaneous requests",
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = parse_arguments()
    with local_project_id_override(args.project_id):
        main(
            project_id=args.project_id,
            location=args.location,
            input_file=args.input,
            output_file=args.output,
            num_rows=args.num_rows,
            start_row=args.start_row,
            parallel_requests=args.parallel_requests,
            cost_per_token=args.cost_per_token,
            max_retries=args.max_retries,
            model_id=args.model_id,
        )
