#!/usr/bin/env python3
"""
Script to update recidiviz/airflow/pyproject.toml with new Cloud Composer package versions.

Usage:
    python update_pyproject.py <packages_file> <pyproject_path>

Where:
    packages_file: Text file with package==version on each line (from Google Cloud docs)
    pyproject_path: Path to the pyproject.toml to update

This script replaces the pinned packages section (after the "We pin all dependencies"
comment) with packages from the input file. It preserves:
- Everything before the marker comment (including manually specified packages)
- The full multi-line marker comment
- The [project.optional-dependencies] section
- The [build-system] section and everything after
"""

import re
import sys
from pathlib import Path

PACKAGE_PATTERN = re.compile(r"^([a-zA-Z0-9._-]+)==(.+)$")
PYPROJECT_PACKAGE_PATTERN = re.compile(r'^    "([a-zA-Z0-9._-]+)==.+",?$')
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


def get_existing_package_names(
    lines: list[str], start_idx: int, end_idx: int
) -> dict[str, str]:
    """
    Build a mapping from normalized name -> original name for existing packages.
    This lets us preserve the original formatting (e.g., kubernetes_asyncio vs kubernetes-asyncio).
    """
    name_map = {}
    for line in lines[start_idx:end_idx]:
        if match := PYPROJECT_PACKAGE_PATTERN.match(line):
            original_name = match.group(1)
            normalized = normalize_key(original_name)
            name_map[normalized] = original_name
    return name_map


def update_pyproject(pyproject_path: Path, new_packages: list[tuple[str, str]]) -> None:
    """Update the pinned packages section of the pyproject.toml."""
    content = pyproject_path.read_text()
    lines = content.split("\n")

    # Find the marker comment start
    marker_start_idx = None
    for i, line in enumerate(lines):
        if MARKER_START in line:
            marker_start_idx = i
            break

    if marker_start_idx is None:
        print(
            f"Error: Could not find marker comment '{MARKER_START}' in pyproject.toml"
        )
        sys.exit(1)

    # Find the end of marker comment (it spans multiple lines ending with the URL)
    marker_end_idx = marker_start_idx
    while marker_end_idx + 1 < len(lines) and lines[
        marker_end_idx + 1
    ].strip().startswith("#"):
        marker_end_idx += 1

    # Find the end of dependencies section (the closing bracket ']')
    deps_end_idx = None
    for i in range(marker_end_idx + 1, len(lines)):
        stripped = lines[i].strip()
        if stripped == "]":
            deps_end_idx = i
            break

    if deps_end_idx is None:
        print("Error: Could not find end of dependencies section")
        sys.exit(1)

    # Get existing package names for preserving original formatting
    existing_names = get_existing_package_names(lines, marker_end_idx + 1, deps_end_idx)

    # Build the new packages section
    new_package_lines = []
    packages_written = set()

    for input_name, version in new_packages:
        normalized = normalize_key(input_name)

        # Use existing name format if available, otherwise normalize to dashes
        name = existing_names.get(normalized, re.sub(r"[._]", "-", input_name))

        new_package_lines.append(f'    "{name}=={version}",')
        packages_written.add(normalized)

    # Reconstruct the file
    updated_lines = (
        lines[: marker_end_idx + 1]  # Everything up to and including marker comment
        + new_package_lines  # New packages
        + lines[deps_end_idx:]  # Closing bracket and everything after
    )

    pyproject_path.write_text("\n".join(updated_lines))
    print(f"✅ Wrote {len(packages_written)} packages to {pyproject_path}")


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    packages_file = Path(sys.argv[1])
    pyproject_path = Path(sys.argv[2])

    if not packages_file.exists():
        print(f"Error: Packages file not found: {packages_file}")
        sys.exit(1)

    if not pyproject_path.exists():
        print(f"Error: pyproject.toml not found: {pyproject_path}")
        sys.exit(1)

    packages_text = packages_file.read_text(encoding="utf-8")
    packages = parse_input_packages(packages_text)

    print(f"📦 Parsed {len(packages)} packages from {packages_file}")

    update_pyproject(pyproject_path, packages)


if __name__ == "__main__":
    main()
