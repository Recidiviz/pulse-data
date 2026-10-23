"""
This script analyzes code changes in a pull request for potential Personally Identifiable Information (PII).
It uses a generative AI model to detect PII and updates the pull request with findings.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from copy import copy
from typing import Iterable, List

import google.auth
import requests
from google.auth.transport.requests import AuthorizedSession
from tabulate import tabulate

# NOTE: If you change the model, re-check every limit below against it.
MODEL_NAME = "openai/gpt-oss-120b-maas"
# Vertex serves gpt-oss only from regional endpoints; the "us" multi-region returns 404
# for it. us-central1 keeps inference on partner diffs in a US region.
MODEL_LOCATION = "us-central1"
# The API rejects longer requests with "The input (N tokens) is longer than the model's
# context length (131072 tokens)." The context holds the prompt, the reasoning, and the
# answer, so the input limit leaves room for MODEL_MAX_OUTPUT_TOKENS plus a margin for
# estimate error.
MODEL_CONTEXT_TOKEN_LIMIT = 131_072
MODEL_MAX_OUTPUT_TOKENS = 24_000
MODEL_INPUT_TOKEN_LIMIT = 100_000
# At the default "medium" effort, a replay of 50 recent PRs showed the model flagging
# synthetic test fixtures that the prompt says to ignore. "high" stopped that without
# missing any of the planted PII.
MODEL_REASONING_EFFORT = "high"
# The gpt-oss tokenizer packs code diffs at about 2.2 characters per token, much denser
# than the 4 characters per token typical of prose, so 2 keeps estimates on the safe side.
CHARS_PER_TOKEN = 2
CONTEXT_LENGTH_ERROR_TEXT = "longer than the model's context length"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 3


def model_endpoint(project: str) -> str:
    """Returns the Vertex AI chat completions URL that serves MODEL_NAME for project."""
    return (
        f"https://{MODEL_LOCATION}-aiplatform.googleapis.com/v1/projects/{project}"
        f"/locations/{MODEL_LOCATION}/endpoints/openapi/chat/completions"
    )


def estimate_tokens_from_chars(text: str) -> int:
    """Returns a conservative estimate of how many model tokens text uses."""
    return len(text) // CHARS_PER_TOKEN


FIND_PII_PROMPT = """
Review the following code changes for Personally Identifiable Information (PII). For each potential piece of PII found, determine:
- the type of PII (e.g., email address, phone number, name, address, IP address, standardized ID number like SSN),
- a risk ranking of LOW, MEDIUM, or HIGH,
- and which file it's in, including the line number

Use context clues to ignore obviously fake/test data:
- Names, emails, and phone numbers that appear fictional or comedic or famous individuals (e.g., "Homer Simpson", "Lizzo").
- File names and paths that indicate test data (e.g., paths containing `test`, `mock`, `sample`, etc.).
- Common testing constants or placeholder values (e.g., "fake@fake.com", "123-45-6789", "USER_001").
- Generic or randomized numerical sequences that do not conform to known standardized ID formats.

We really want to avoid false positives. Please carefully consider whether the data is indeed likely to be PII (could you use it in a sentence or to perform a search on somebody with that data)? The bar should be more likely than not before you report it back. Specifically for ID numbers, prioritize identification of formats that are widely recognized and standardized (e.g., Social Security Numbers). Be cautious about flagging simple numerical strings that could be database IDs or other non-sensitive identifiers unless there is strong contextual evidence suggesting they are PII.

CRITICAL: DO NOT flag database schema references, column names, field names, or SQL query structures. Only flag actual PII VALUES, not metadata about PII. Specifically, ignore:
- Database column names (e.g., "first_name", "email_address", "phone_number", "assessment_score")
- SQL SELECT statements that reference columns but don't contain actual values
- Variable names that describe PII fields but don't contain actual PII data
- JSON field names or dictionary keys that reference PII concepts
- Function names or method names that reference PII handling
- Comments that describe PII fields or data processing
- Table names, view names, or schema definitions
- Field descriptions or data type definitions

Only flag content that contains ACTUAL PII values like:
- Real email addresses (john.doe@company.com)
- Real phone numbers (555-123-4567)
- Real names when used as data values
- Real addresses when used as data values
- Real SSNs or other ID numbers when used as data values
- Real IP addresses when used as data values

We also do not want to be alerted to PII of Recidiviz employees or contractors, so please ignore any PII that is clearly related to Recidiviz employees or contractors. This includes email addresses, names, computer names, usernames and other identifiers that are clearly associated with Recidiviz.

Also, we frequently have files that reference data fields names and descriptions, and files that describe data schemas but do not actually contain PII. Please do not report on these metadata findings but only files whose content themselves constitute PII.


Additionally, please ignore these specific types of false positives:
- Variables or expressions within print statements or logging statements (e.g., print(variable_name) or console.log(some_variable)) - we clear output so this is not a concern. Do not flag the variable names or expressions themselves as PII, even if they might contain PII values at runtime.
- Common database column names that refer to PII but are not PII values themselves, such as: person_id, officer_id, offender_id, staff_id, user_id, race, gender, ethnicity, firstname, lastname, middlename, email, phone, address, date_of_birth, DOB, birthdate, and similar column identifiers. Only flag these if they contain actual PII values, not just the column name references.
- Variable names, function names, or code identifiers that reference PII concepts but do not contain actual PII data.

If you find PII, return your findings as a JSON array where each object has the following keys:
- "content": The potential PII content.
- "type": The type of PII detected.
- "risk": The risk ranking.
- "rationale":  A very short, 1 sentence explanation of why this is considered PII and why it has the risk ranking.
- "context": the context in which the PII was found (e.g., "in a comment", "in a string literal", "in a csv file", "in a variable in a Python file").
- "file": name of the file.
- "line": line number in the file.

If you find NO PII, return exactly: "NO PII FOUND"

Code Changes to Analyze:
"""


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--head", required=True, help="Head commit SHA")
    parser.add_argument("--pr", required=True, help="Pull Request number")
    return parser.parse_args(argv)


def extract_json_from_markdown(text: str) -> str:
    """
    Extracts JSON content from a markdown code block or returns the text if it's already JSON.
    """
    text = text.strip()

    # Check for explicit "no PII found" response
    if "NO PII FOUND" in text.upper():
        return "[]"

    # First try to extract from markdown code block
    match = re.search(r"```json\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()

    # If no markdown block found, check if the text itself is valid JSON
    if text.startswith("[") or text.startswith("{"):
        return text

    # An answer with no JSON is not a clean result; raising fails the check instead of
    # reporting a batch nobody could read as free of PII.
    raise ValueError("Model answer contained neither JSON nor NO PII FOUND.")


def write_output(name: str, value: str) -> None:
    """
    Writes a key-value pair to the GitHub Actions output file.
    """
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
        f.write(f"{name}={value}\n")


def generate_found_pii_table(
    findings: List[dict],
    *,
    include_content: bool,
    include_links: bool = False,
    repo: str = "",
    head_sha: str = "",
) -> str:
    """
    Generates a markdown-formatted table of PII findings.
    If include_content is False, leaves out the Content column, which holds the found
    values themselves. If include_links is True, adds GitHub file links to the table.
    """
    table_data = []
    for item in findings:
        file = item["file"]
        line = item["line"]
        file_link = (
            f"[{file}:{line}](https://github.com/{repo}/blob/{head_sha}/{file}#L{line})"
            if include_links and file
            else f"{file}:{line}"
            if file
            else ""
        )
        row = [
            item["type"],
            item["risk"],
            item["rationale"],
            item["context"],
            file_link,
        ]
        table_data.append([f"`{item['content']}`", *row] if include_content else row)

    headers = ["Type", "Risk", "Rationale", "Context", "File"]
    if include_content:
        headers = ["Content", *headers]
    return tabulate(table_data, headers=headers, tablefmt="github")


def print_table(findings: List[dict]) -> None:
    """
    Prints a table of PII findings to the console, without the found values, because
    GitHub Actions keeps job logs and anyone with read access to the repo can open them.
    """
    table = generate_found_pii_table(findings, include_content=False)
    print("\n\033[33m📋 PII findings (values are in the PR comment):\033[0m")
    print(table)


def build_markdown_comment(findings: List[dict], repo: str, head_sha: str) -> str:
    """
    Builds a markdown comment summarizing the PII findings.
    """
    table_md = generate_found_pii_table(
        findings,
        include_content=True,
        include_links=True,
        repo=repo,
        head_sha=head_sha,
    )

    return f"""<!-- pii-bot-comment -->
### ⚠️ Potential PII detected

The following items may contain Personally Identifiable Information (PII). Please review and take appropriate action.

{table_md}

_This comment is automatically updated on new commits to this PR._"""


def update_pr_comment(
    findings: bool, pr_number: str, body: str, repo: str, token: str
) -> None:
    """
    Deletes an existing PII comment (if any) and creates a new comment on a pull request with the provided body content.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    comments_url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    resp = requests.get(comments_url, headers=headers, timeout=10)
    resp.raise_for_status()

    comments = resp.json()
    existing = next(
        (c for c in comments if c["body"].startswith("<!-- pii-bot-comment -->")), None
    )

    if existing:
        comment_id = existing["id"]
        delete_url = f"https://api.github.com/repos/{repo}/issues/comments/{comment_id}"
        requests.delete(delete_url, headers=headers, timeout=10)

    # Add a new comment if there are findings
    if findings:
        requests.post(comments_url, headers=headers, json={"body": body}, timeout=10)


def _get_changed_file_path_to_diff(base_ref: str, head_ref: str) -> dict[str, str]:
    """
    Returns a dictionary mapping file paths to their diffs between the base and head references.
    """
    try:
        changed_files = subprocess.check_output(  # nosec B603, B607
            ["git", "diff", "--name-only", f"{base_ref}..{head_ref}"], text=True
        ).splitlines()
    except subprocess.CalledProcessError as e:
        print(f"::error::Failed to get changed files: {e}")
        return {}

    file_to_diff = {}
    for file_path in changed_files:
        try:
            diff = subprocess.check_output(  # nosec B603, B607
                ["git", "diff", f"{base_ref}..{head_ref}", "--", file_path], text=True
            )
            if diff.strip():
                file_to_diff[file_path] = diff
        except subprocess.CalledProcessError as e:
            print(f"::warning::Failed to get diff for {file_path}: {e}")
    return file_to_diff


def _split_file_diff(file_path: str, diff: str, max_tokens: int) -> list[str]:
    """
    Returns the diff for file_path as one or more "# File:" sections, each at most
    max_tokens estimated tokens, cut at line boundaries. A single line longer than the
    budget is cut into budget-sized pieces, so no part of the diff is dropped.
    """
    header = f"\n# File: {file_path}\n"
    max_chars = (max_tokens - estimate_tokens_from_chars(header)) * CHARS_PER_TOKEN
    if max_chars <= 0:
        raise ValueError(f"No room for any diff content of {file_path} in a batch.")

    sections: list[str] = []
    current = ""
    for line in diff.splitlines(keepends=True):
        pieces = [line[i : i + max_chars] for i in range(0, len(line), max_chars)]
        for piece in pieces:
            if current and len(current) + len(piece) > max_chars:
                sections.append(header + current)
                current = ""
            current += piece
    if current:
        sections.append(header + current)
    return sections


def generate_prompts(
    changed_file_to_diff: dict[str, str],
) -> Iterable[str]:
    """
    Generates one or more prompts for the model based on the changed files and their diffs.
    We continually add diffs to the prompt until we reach the token limit. A diff that
    does not fit in one prompt on its own is split across prompts by line.
    """
    room_for_diffs = MODEL_INPUT_TOKEN_LIMIT - estimate_tokens_from_chars(
        FIND_PII_PROMPT
    )
    prompt = copy(FIND_PII_PROMPT)
    for file_path, diff in changed_file_to_diff.items():
        for this_diff in _split_file_diff(file_path, diff, room_for_diffs):
            if (
                estimate_tokens_from_chars(prompt)
                + estimate_tokens_from_chars(this_diff)
                > MODEL_INPUT_TOKEN_LIMIT
            ):
                # The diff that overflowed the batch starts the next one — dropping it
                # here would silently exclude that file from the PII scan.
                yield prompt
                prompt = copy(FIND_PII_PROMPT) + this_diff
            else:
                prompt += this_diff
    # This case is if all unprompted diffs haven't reached the size limit by the end
    if prompt != FIND_PII_PROMPT:
        yield prompt


def run_prompt_and_collect_findings(
    session: AuthorizedSession, endpoint: str, prompt: str
) -> list[dict]:
    """
    Runs a prompt against the model at endpoint and returns the PII findings.

    Retries rate limits, server errors, timeouts, and empty answers with exponential
    backoff, and raises once MAX_ATTEMPTS is spent, so the check fails instead of
    reporting a batch it never scanned as clean.
    """
    request_body = {
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": MODEL_MAX_OUTPUT_TOKENS,
        "reasoning_effort": MODEL_REASONING_EFFORT,
    }
    last_error = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session.post(endpoint, json=request_body, timeout=600)
        except (requests.Timeout, requests.ConnectionError) as e:
            last_error = f"{type(e).__name__}: {e}"
        else:
            if response.status_code == 200:
                choice = response.json()["choices"][0]
                model_output = choice["message"].get("content") or ""
                # Do not print model_output: it repeats any PII it found, and GitHub
                # Actions keeps job logs.
                print(
                    f"📨 Model response (attempt {attempt}): {len(model_output)} characters,"
                    f" finish_reason={choice.get('finish_reason')}"
                )
                if model_output:
                    # TODO(#55197): Request structured JSON output instead of parsing the model's text.
                    json_text = extract_json_from_markdown(model_output)
                    findings = json.loads(json_text) or []
                    if not isinstance(findings, list):
                        raise ValueError(
                            f"Unexpected type for findings: {type(findings)}"
                        )
                    return findings
                # The endpoint sometimes answers 200 with finish_reason "abort" and no
                # content, which is a server-side failure worth retrying.
                last_error = (
                    f"empty response, finish_reason={choice.get('finish_reason')}"
                )
            elif response.status_code in RETRYABLE_STATUS_CODES:
                last_error = f"HTTP {response.status_code}"
            else:
                raise RuntimeError(
                    f"PII detection request failed with HTTP {response.status_code}:"
                    f" {response.text[:500]}"
                )

        if attempt < MAX_ATTEMPTS:
            backoff_time = 2**attempt
            print(
                f"::warning::Model call failed on attempt {attempt} ({last_error}). Retrying in {backoff_time}s..."
            )
            time.sleep(backoff_time)

    raise RuntimeError(
        f"PII detection failed after {MAX_ATTEMPTS} attempts: {last_error}"
    )


def main(head: str, pr_number: str) -> int:
    """
    Main function to analyze code changes for PII and update the pull request with findings.
    """
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    session = AuthorizedSession(credentials)
    endpoint = model_endpoint(
        os.environ.get("GOOGLE_CLOUD_PROJECT", "recidiviz-security")
    )

    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GITHUB_TOKEN"]

    base = "origin/main"
    subprocess.check_call(["git", "fetch", "origin", "main"])  # nosec B603, B607

    changed_file_to_diff = _get_changed_file_path_to_diff(base, head)
    if not changed_file_to_diff:
        print("::warning::No code changes provided for analysis.")
        write_output("pii_found", "false")
        return 0

    findings = []
    for prompt in generate_prompts(changed_file_to_diff):
        try:
            findings.extend(run_prompt_and_collect_findings(session, endpoint, prompt))
        except Exception as e:
            # Handle token limit specifically
            if CONTEXT_LENGTH_ERROR_TEXT in str(e):
                print(
                    "::warning::Input too large for PII detection; posting manual-inspect comment."
                )
                write_output("pii_found", "false")
                manual_body = """<!-- pii-bot-comment -->
🚧 The diff was too large to be processed by our PII detection tool.
Please manually review these files for potential PII."""
                update_pr_comment(True, pr_number, manual_body, repo, token)
                return 0

            # For other errors, fail the check
            print(f"::error::PII check failed: {e}")
            write_output("pii_found", "false")
            return 1

    if not findings:
        print("\033[32m✔ No potential PII found.")
        write_output("pii_found", "false")
        update_pr_comment(
            False,
            pr_number,
            "<!-- pii-bot-comment -->🧼 No PII found in this PR.",
            repo,
            token,
        )
        return 0

    print("\033[33m⚠ Potential PII DETECTED (non-blocking).\033[0m")
    print_table(findings)
    write_output("pii_found", "true")

    markdown_body = build_markdown_comment(findings, repo, head)
    update_pr_comment(True, pr_number, markdown_body, repo, token)

    print("::warning::Potential PII found — please review the PR comment for details.")
    return 1


if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    sys.exit(main(args.head, args.pr))
