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
from copy import copy
from typing import Iterable, List

import google.generativeai as genai  # type: ignore # pylint: disable = no-name-in-module
import requests
from google.api_core import retry
from google.generativeai.types import RequestOptions
from tabulate import tabulate

# NOTE: If you change the model, you may need to change update the token limit!
MODEL_NAME = "gemini-2.5-pro-preview-03-25"
MODEL_INPUT_TOKEN_LIMIT = 1_048_576

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

Also, we frequently have files that reference data fields names and descriptions, and files that describe data schemas but do not actually contain PII. Please do not report on these metadata findings but only files whose content themselves constitute PII.

Return your findings as a JSON array where each object has the following keys:
- "content": The potential PII content.
- "type": The type of PII detected.
- "risk": The risk ranking.
- "rationale":  A very short, 1 sentence explanation of why this is considered PII and why it has the risk ranking.
- "context": the context in which the PII was found (e.g., "in a comment", "in a string literal", "in a csv file", "in avariable in a Pyhton file").
- "file": name of the file.
- "line": line number in the file.

Code Changes to Analyze:
"""


def parse_arguments(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--head", required=True, help="Head commit SHA")
    parser.add_argument("--pr", required=True, help="Pull Request number")
    return parser.parse_args(argv)


def extract_json_from_markdown(text: str) -> str:
    """
    Extracts JSON content from a markdown code block.
    """
    match = re.search(r"```json\n(.*?)```", text, re.DOTALL)
    return match.group(1).strip() if match else ""


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
        if len(this_diff) > MODEL_INPUT_TOKEN_LIMIT:
            raise ValueError(
                f"Diff for {file_path} exceeds the model's input token limit of {MODEL_INPUT_TOKEN_LIMIT} tokens."
            )
        if len(prompt) + len(this_diff) > MODEL_INPUT_TOKEN_LIMIT:
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
    Runs a single prompt with the model and collects the findings into a list.
    See the RequestOptions for retry and timeout settings.
    """
    if len(prompt) > MODEL_INPUT_TOKEN_LIMIT:
        raise ValueError(
            f"Prompt exceeds the model's input token limit of {MODEL_INPUT_TOKEN_LIMIT} tokens."
        )
    response = model.generate_content(
        prompt,
        request_options=RequestOptions(
            # Retry will happen for any of these errors Google defined as 'transient' (Found in _BaseRetry):
            # exceptions.InternalServerError,
            # exceptions.TooManyRequests,
            # exceptions.ServiceUnavailable,
            # requests.exceptions.ConnectionError,
            # requests.exceptions.ChunkedEncodingError,
            # auth_exceptions.TransportError,
            retry=retry.Retry(
                # Waits 3 seconds before retrying, then doubles the wait time for each subsequent retry.
                # Stops retrying after 3 minutes
                initial=3,
                multiplier=2,
                timeout=60 * 3,
            ),
            # Individual requests time out after 1 minute
            timeout=60,
        ),
    )
    gemini_output = response.text
    print(f"📨 Gemini API Response:\n{gemini_output}")

    json_text = extract_json_from_markdown(gemini_output)
    findings = json.loads(json_text) or []
    if not isinstance(findings, list):
        raise ValueError(f"Unexpected type for findings: {type(findings)}")
    return findings


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
            print(f"::error::Error: {e}")
            write_output("pii_found", "false")
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
