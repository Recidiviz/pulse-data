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
LLM-based validation and ranking functionality for resource search.

This module provides functions to validate resource candidates and rank
them using large language models (LLMs). It includes utilities for mapping
data to LLM input formats, processing LLM responses, and running validation
and ranking prompts against resource candidates.
"""
import asyncio
import json
import logging
from typing import Optional

import tiktoken
from langsmith import traceable
from llama_index.core.base.llms.types import CompletionResponse
from llama_index.llms.openai import OpenAI

from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceSubcategory,
)
from recidiviz.resource_search.src.modules.llm.prompts import get_validation_prompts
from recidiviz.resource_search.src.settings import settings
from recidiviz.resource_search.src.typez.crud.resources import ResourceCandidateWithURI
from recidiviz.resource_search.src.typez.modules.llm.validate import (
    LlmInput,
    MergedOutput,
    RankingOutput,
    ValidationOutput,
)

MODEL = "gpt-3.5-turbo"
CONTEXT_LENGTH_LIMIT = 16385
llm = OpenAI(model=MODEL, api_key=settings.openai_api_key)
model_encoding = tiktoken.encoding_for_model(MODEL)


@traceable()
def check_context_limit_exceeded(string: str) -> bool:
    """Check if the context length of prompt is greater than the limit."""
    num_tokens = len(model_encoding.encode(string))
    return num_tokens > CONTEXT_LENGTH_LIMIT


@traceable(run_type="llm")
async def llm_completion(prompt: str) -> Optional[CompletionResponse]:
    if check_context_limit_exceeded(prompt):
        logging.warning("Context length limit exceeded for prompt: %s", prompt)
        return None

    try:
        return await llm.acomplete(prompt)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Error running LLM completion: %s", exc)
        return None


@traceable()
def map_to_llm_input(data: ResourceCandidateWithURI) -> LlmInput:
    """Map the data to LLmInput type"""
    return LlmInput(
        id=data.uri,
        name=data.name,
        tags=data.tags,
        rating=data.rating,
        ratingCount=data.ratingCount,
        reviews=data.reviews,
        operationalStatus=data.operationalStatus,
    )


@traceable()
def map_llm_output(
    llm_output: dict[str, MergedOutput],
    original: list[ResourceCandidateWithURI],
) -> list[ResourceCandidateWithURI]:
    """Map the LLM output to ResourceCandidate type"""

    # Update original results with rank value
    for result in original:
        matched_output = llm_output.get(
            result.uri,
            MergedOutput(),
        )

        if matched_output.rank is not None:
            result.llm_rank = matched_output.rank
        if matched_output.valid is not None:
            result.llm_valid = matched_output.valid

    return original


@traceable()
async def run_validation(
    data: list[ResourceCandidateWithURI],
    # criteria_prompt: str,
    # rank_prompt: str,
    category: ResourceCategory,
    subcategory: Optional[ResourceSubcategory] = None,
) -> list[ResourceCandidateWithURI]:
    """Validate a list of data"""
    llm_input_results = [map_to_llm_input(result) for result in data]

    llm_outputs = await asyncio.gather(
        ranking_prompt(llm_input_results, category, subcategory),
        validation_prompt(llm_input_results, category, subcategory),
    )

    # Merge LLM outputs by id
    merged_outputs: dict[str, MergedOutput] = {}

    # Process each LLM output list
    for output_list in llm_outputs:
        if output_list is not None:
            for output in output_list:
                if output.id not in merged_outputs:
                    merged_outputs[output.id] = MergedOutput()

                # Add rank or valid field based on output type
                if isinstance(output, RankingOutput):
                    merged_outputs[output.id].rank = output.rank
                elif isinstance(output, ValidationOutput):
                    merged_outputs[output.id].valid = output.valid

    # Map merged outputs back to original data
    return map_llm_output(merged_outputs, data)


@traceable()
async def validation_prompt(
    data: list[LlmInput],
    category: ResourceCategory,
    subcategory: Optional[ResourceSubcategory] = None,
) -> Optional[list[ValidationOutput]]:
    """Validate a list of data"""

    example_output = """
    [{"id": "7a8b9c3d", "valid": true}, {"id": "1a2b3c4d", "valid": false}]
    """

    prompts = get_validation_prompts(category, subcategory)

    prompt = f"""
        Here is a list of resources:

        ```
        {[result.model_dump() for result in data]}
        ```

        For each resource in the list, determine if it meets the following criteria.
        {prompts.criteria}

        ENSURE that you include all the resources in your output.

        Return the id of the resource and if it meets the criteria as a boolean.

        Here is an example output:
        ```
        {example_output}
        ```
    """

    response = await llm_completion(prompt)
    if not response:
        return None

    # Check that the response is valid JSON
    try:
        response_json = json.loads(response.text)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("Error parsing JSON response: %s with error: %s", response, exc)
        return None

    # Check that the response is a list of dictionaries with id and valid keys
    if not isinstance(response_json, list) or not all(
        isinstance(item, dict) and "id" in item and "valid" in item
        for item in response_json
    ):
        return None

    return [ValidationOutput(**item) for item in response_json]


@traceable()
async def ranking_prompt(
    data: list[LlmInput],
    category: ResourceCategory,
    subcategory: Optional[ResourceSubcategory] = None,
) -> Optional[list[RankingOutput]]:
    """Rank a list of data using an LLM.

    Args:
        data: List of dictionaries containing data to rank
        category: Category of the data to rank

    Returns:
        List of dictionaries with added rank values between 1-5
    """

    example_output = """
    [{"id": "7a8b9c3d", "rank": 5}, {"id": "1a2b3c4d", "rank": 2}]
    """

    prompts = get_validation_prompts(category, subcategory)

    prompt = f"""
        Here is a list of resources:

        ```
        {[result.model_dump() for result in data]}
        ```

        For each resource in the list, assign a rank from 1 to 5 based on how well it meets the following criteria.
        {prompts.criteria}

        {prompts.ranking}

        ENSURE that you include all the resources in your output.

        Return the id of the resource and its rank value (1-5).

        Here is an example output:
        ```
        {example_output}
        ```
    """

    response = await llm_completion(prompt)
    if not response:
        return None

    # Check that the response is valid JSON
    try:
        response_json = json.loads(response.text)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning(
            "Error parsing LLM JSON response: %s with error: %s", response, exc
        )
        return None

    # Check that the response is a list of dictionaries with id and rank keys
    if not isinstance(response_json, list) or not all(
        isinstance(item, dict) and "id" in item and "rank" in item
        for item in response_json
    ):
        return None

    return [RankingOutput(**item) for item in response_json]
