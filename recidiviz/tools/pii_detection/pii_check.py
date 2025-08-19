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

import google.generativeai as genai  # type: ignore # pylint: disable = no-name-in-module
import requests
from tabulate import tabulate

# NOTE: If you change the model, you may need to change update the token limit!
MODEL_NAME = "gemini-2.5-flash"
MODEL_INPUT_TOKEN_LIMIT = 2_097_152


def estimate_tokens_from_chars(text: str) -> int:
    """Estimate token count using Google's 1 token ≈ 4 characters guideline."""
    return len(text) // 4


FIND_PII_GEMINI_PROMPT = """
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

    # Return empty array as fallback
    return "[]"


def write_output(name: str, value: str) -> None:
    """
    Writes a key-value pair to the GitHub Actions output file.
    """
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
        f.write(f"{name}={value}\n")


def generate_found_pii_table(
    findings: List[dict],
    include_links: bool = False,
    repo: str = "",
    head_sha: str = "",
) -> str:
    """
    Generates a markdown-formatted table of PII findings.
    If include_links is True, adds GitHub file links to the table.
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
        table_data.append(
            [
                f"`{item['content']}`",
                item["type"],
                item["risk"],
                item["rationale"],
                item["context"],
                file_link,
            ]
        )

    headers = ["Content", "Type", "Risk", "Rationale", "Context", "File"]
    return tabulate(table_data, headers=headers, tablefmt="github")


def print_table(findings: List[dict]) -> None:
    """
    Prints a table of PII findings to the console.
    """
    table = generate_found_pii_table(findings)
    print("\n\033[33m📋 Gemini PII Findings:\033[0m")
    print(table)


def build_markdown_comment(findings: List[dict], repo: str, head_sha: str) -> str:
    """
    Builds a markdown comment summarizing the PII findings.
    """
    table_md = generate_found_pii_table(
        findings, include_links=True, repo=repo, head_sha=head_sha
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


def generate_prompts(
    changed_file_to_diff: dict[str, str],
) -> Iterable[str]:
    """
    Generates one or more prompts for the model based on the changed files and their diffs.
    We continually add diffs to the prompt until we reach the token limit.
    If a single diff is larger than the token limit, we raise an error.
    """
    prompt = copy(FIND_PII_GEMINI_PROMPT)
    for file_path, diff in changed_file_to_diff.items():
        this_diff = f"\n# File: {file_path}\n{diff}"
        if estimate_tokens_from_chars(this_diff) > MODEL_INPUT_TOKEN_LIMIT:
            raise ValueError(
                f"Diff for {file_path} exceeds the model's input token limit of {MODEL_INPUT_TOKEN_LIMIT} tokens."
            )
        if (
            estimate_tokens_from_chars(prompt) + estimate_tokens_from_chars(this_diff)
            > MODEL_INPUT_TOKEN_LIMIT
        ):
            yield prompt
            prompt = copy(FIND_PII_GEMINI_PROMPT)
        else:
            prompt += this_diff
    # This case is if all unprompted diffs haven't reached the size limit by the end
    if prompt != FIND_PII_GEMINI_PROMPT:
        yield prompt


def run_prompt_and_collect_findings(
    model: genai.GenerativeModel, prompt: str
) -> list[dict]:
    """
    Runs a prompt against the Gemini model and collects PII findings.
    Retries on transient errors with exponential backoff.
    """
    if len(prompt) > MODEL_INPUT_TOKEN_LIMIT:
        print(
            f"::warning::Prompt exceeds the model's input token limit of {MODEL_INPUT_TOKEN_LIMIT} tokens. Skipping this batch."
        )
        return []

    # Retry with exponential backoff for transient errors
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            response = model.generate_content(prompt)
            gemini_output = response.text
            print(f"📨 Gemini API Response (attempt {attempt}):")
            print(f"Raw response: {repr(gemini_output)}")

            json_text = extract_json_from_markdown(gemini_output)
            print(f"Extracted JSON: {repr(json_text)}")

            findings = json.loads(json_text) or []
            if not isinstance(findings, list):
                raise ValueError(f"Unexpected type for findings: {type(findings)}")
            return findings

        except Exception as e:
            error_msg = str(e).lower()

            # Check for timeout/internal errors - retry these
            if any(
                keyword in error_msg for keyword in ["timeout", "internal error", "500"]
            ):
                if attempt < max_attempts:
                    backoff_time = 2**attempt
                    print(
                        f"::warning::Gemini timeout/error on attempt {attempt}: {e}. Retrying in {backoff_time}s..."
                    )
                    time.sleep(backoff_time)
                    continue

                # Max retries exceeded for timeout - treat as no findings
                print(
                    "::warning::Gemini consistently timing out. Treating as no PII found."
                )
                return []

            # For other errors (token limit, JSON parse, etc.), re-raise immediately
            raise RuntimeError(
                f"PII detection failed after {attempt} attempts: {e}"
            ) from e

    # This should never be reached due to the logic above, but satisfies mypy
    return []


def main(head: str, pr_number: str) -> int:
    """
    Main function to analyze code changes for PII and update the pull request with findings.
    """
    genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))
    model = genai.GenerativeModel(MODEL_NAME)

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
            findings.extend(run_prompt_and_collect_findings(model, prompt))
        except Exception as e:
            error_msg = str(e).lower()

            # Handle token limit specifically
            if "exceeds the maximum number of tokens" in error_msg:
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
    write_output("gemini_pii_findings", json.dumps(findings))

    markdown_body = build_markdown_comment(findings, repo, head)
    update_pr_comment(True, pr_number, markdown_body, repo, token)

    print("::warning::Potential PII found — please review the PR comment for details.")
    return 1


if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    sys.exit(main(args.head, args.pr))
