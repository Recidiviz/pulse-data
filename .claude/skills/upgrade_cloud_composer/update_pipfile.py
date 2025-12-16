#!/usr/bin/env python3
"""
Script to update recidiviz/airflow/Pipfile with new Cloud Composer package versions.

Usage:
    python update_pipfile.py <packages_file> <pipfile_path>

Where:
    packages_file: Text file with package==version on each line (from Google Cloud docs)
    pipfile_path: Path to the Pipfile to update

This script replaces the pinned packages section (after the "We pin all dependencies"
comment) with packages from the input file. It preserves:
- Everything before the marker comment (including manually specified packages)
- The full multi-line marker comment
- Original package name formatting for existing packages
- Special comment blocks (like the tensorflow MacOS workaround)
- The [requires] section at the end
"""

import re
import sys
from pathlib import Path

PACKAGE_PATTERN = re.compile(r"^([a-zA-Z0-9._-]+)==(.+)$")
PIPFILE_PACKAGE_PATTERN = re.compile(r'^([a-zA-Z0-9._-]+)\s*=\s*"==.+"$')
MARKER_START = "# We pin all dependencies"


def normalize_key(name: str) -> str:
    """Normalize package name for matching: lowercase, replace dots/underscores with dashes."""
    return re.sub(r"[._]", "-", name).lower()


def clean_version(version: str) -> str:
    """Clean version string: remove +composer suffix."""
    return re.sub(r"\+composer$", "", version)


def parse_input_packages(packages_text: str) -> list[tuple[str, str]]:
    """Parse input packages. Returns list of (name, version) tuples in order."""
    packages = []
    for line in packages_text.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        if match := PACKAGE_PATTERN.match(line):
            name = match.group(1)
            version = clean_version(match.group(2))
            packages.append((name, version))

    return packages


def find_marker_end(lines: list[str], marker_start_idx: int) -> int:
    """Find the end of the multi-line marker comment (returns index of last comment line)."""
    idx = marker_start_idx
    # The marker comment continues while lines start with #
    while idx + 1 < len(lines) and lines[idx + 1].strip().startswith("#"):
        # But stop if it looks like a different comment block (blank line before it)
        if idx > marker_start_idx and not lines[idx].strip():
            break
        idx += 1
    return idx


def extract_comment_blocks(
    lines: list[str], start_idx: int, end_idx: int
) -> list[tuple[str, list[str]]]:
    """
    Extract special comment blocks from the packages section.
    Returns list of (preceding_package_key, comment_lines) tuples.
    """
    blocks = []
    current_block = []
    preceding_package = None

    for line in lines[start_idx:end_idx]:
        stripped = line.strip()
        if stripped.startswith("#"):
            current_block.append(line)
        elif PIPFILE_PACKAGE_PATTERN.match(stripped):
            if current_block:
                blocks.append((preceding_package, current_block))
                current_block = []
            match = PIPFILE_PACKAGE_PATTERN.match(stripped)
            preceding_package = normalize_key(match.group(1))
        elif not stripped:
            # Empty line - could be part of a block
            if current_block:
                current_block.append(line)

    # Don't forget any trailing block
    if current_block:
        blocks.append((preceding_package, current_block))

    return blocks


def get_existing_package_names(
    lines: list[str], start_idx: int, end_idx: int
) -> dict[str, str]:
    """
    Build a mapping from normalized name -> original name for existing packages.
    This lets us preserve the original formatting (e.g., kubernetes_asyncio vs kubernetes-asyncio).
    """
    name_map = {}
    for line in lines[start_idx:end_idx]:
        line = line.strip()
        if match := PIPFILE_PACKAGE_PATTERN.match(line):
            original_name = match.group(1)
            normalized = normalize_key(original_name)
            name_map[normalized] = original_name
    return name_map


def update_pipfile(pipfile_path: Path, new_packages: list[tuple[str, str]]) -> None:
    """Update the pinned packages section of the Pipfile."""
    content = pipfile_path.read_text()
    lines = content.split("\n")

    # Find the marker comment start
    marker_start_idx = None
    for i, line in enumerate(lines):
        if MARKER_START in line:
            marker_start_idx = i
            break

    if marker_start_idx is None:
        print(f"Error: Could not find marker comment '{MARKER_START}' in Pipfile")
        sys.exit(1)

    # Find the end of the marker comment (it spans multiple lines)
    marker_end_idx = find_marker_end(lines, marker_start_idx)

    # Find the end of [packages] section
    packages_end_idx = None
    for i in range(marker_end_idx + 1, len(lines)):
        stripped = lines[i].strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            packages_end_idx = i
            break

    if packages_end_idx is None:
        packages_end_idx = len(lines)

    # Get existing package names for preserving original formatting
    existing_names = get_existing_package_names(
        lines, marker_end_idx + 1, packages_end_idx
    )

    # Extract comment blocks to preserve (like tensorflow workaround)
    comment_blocks = extract_comment_blocks(lines, marker_end_idx + 1, packages_end_idx)

    # Build a set of packages to skip (those that are commented out, like tensorflow)
    skip_packages = set()
    for _, block_lines in comment_blocks:
        for bl in block_lines:
            # Look for commented-out package lines like "# tensorflow = ..."
            match = re.match(r"^#\s*([a-zA-Z0-9._-]+)\s*=", bl.strip())
            if match:
                skip_packages.add(normalize_key(match.group(1)))

    # Build the new packages section
    new_package_lines = []
    packages_written = set()

    for input_name, version in new_packages:
        normalized = normalize_key(input_name)

        # Skip packages that are commented out in the original
        if normalized in skip_packages:
            continue

        # Use existing name format if available, otherwise normalize to dashes
        name = existing_names.get(normalized, default=re.sub(r"[._]", "-", input_name))

        new_package_lines.append(f'{name} = "=={version}"')
        packages_written.add(normalized)

        # Check if there's a comment block that should follow this package
        for preceding_pkg, block_lines in comment_blocks:
            if preceding_pkg == normalized:
                new_package_lines.extend(block_lines)

    # Reconstruct the file
    updated_lines = (
        lines[: marker_end_idx + 1]
        + new_package_lines  # Everything up to and including marker comment
        + [""]  # New packages
        + lines[  # Blank line before [requires]
            packages_end_idx:
        ]  # [requires] section and beyond
    )

    pipfile_path.write_text("\n".join(updated_lines))
    print(f"✅ Wrote {len(packages_written)} packages to {pipfile_path}")


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    packages_file = Path(sys.argv[1])
    pipfile_path = Path(sys.argv[2])

    if not packages_file.exists():
        print(f"Error: Packages file not found: {packages_file}")
        sys.exit(1)

    if not pipfile_path.exists():
        print(f"Error: Pipfile not found: {pipfile_path}")
        sys.exit(1)

    packages_text = packages_file.read_text(encoding="utf-8")
    packages = parse_input_packages(packages_text)

    print(f"📦 Parsed {len(packages)} packages from {packages_file}")

    update_pipfile(pipfile_path, packages)


if __name__ == "__main__":
    main()
