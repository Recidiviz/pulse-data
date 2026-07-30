#!/usr/bin/env bash

#
# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
#

# Mirrors the checked-out working tree to the analyst Colab support bucket prefix
# passed as the only argument (e.g.
# gs://recidiviz-staging-repositories/pulse-data). Analyst notebooks
# gcsfuse-mount that prefix and `%run` files out of it, so it must track main.
#
# Run from the root of a pulse-data checkout; in Cloud Build that is
# /workspace. Only tracked files are mirrored, so this is also safe to run from a
# local checkout with a populated .venv.
#
# Deliberately does NOT use `gcloud storage rsync`. The bucket is CMEK-encrypted,
# and GCS omits object hashes from list responses for CMEK objects, so rsync
# issues one metadata GET per destination object — ~26k serial requests — before
# copying anything. That enumeration grew from ~1 minute to ~26 minutes and timed
# out every run of this job. See #93765.
#
# Instead: upload the tracked tree unconditionally, which needs no destination
# enumeration at all, then delete mirror objects that are no longer tracked,
# using one cheap ListObjects pass diffed against the git index.

set -euo pipefail

# Ceiling on how much of the mirror a single run may delete, as a share of the
# objects currently in it. Guards against wiping the mirror if the tracked file
# list or the object listing ever comes back wrong.
MAX_DELETION_PERCENT=10

# Deletions always allowed regardless of MAX_DELETION_PERCENT, so that ordinary
# merges removing a handful of files are not blocked.
MIN_ALLOWED_DELETIONS=50

MIRROR_URL="${1:-}"
if [[ -z "${MIRROR_URL}" ]]; then
  echo "Usage: $(basename "$0") <mirror-url>" >&2
  echo "  e.g. $(basename "$0") gs://recidiviz-staging-repositories/pulse-data" >&2
  exit 1
fi
MIRROR_URL="${MIRROR_URL%/}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

TRACKED_PATHS="${WORK_DIR}/tracked_paths.txt"
TOP_LEVEL_PATHS="${WORK_DIR}/top_level_paths.txt"
MIRROR_URLS="${WORK_DIR}/mirror_urls.txt"
MIRROR_PATHS="${WORK_DIR}/mirror_paths.txt"
STALE_PATHS="${WORK_DIR}/stale_paths.txt"

# Read the tracked file set from the git index rather than from history, so this
# works on the shallow checkout Cloud Build may produce. quotepath=false keeps
# non-ASCII paths unescaped, so they compare equal to their mirror objects
# instead of looking stale and being deleted.
git -c core.quotepath=false ls-files | LC_ALL=C sort > "${TRACKED_PATHS}"

if [[ ! -s "${TRACKED_PATHS}" ]]; then
  echo "Found no tracked files in $(pwd); refusing to sync." >&2
  exit 1
fi

# Upload by top-level tracked entry. Naming each entry explicitly avoids the
# ambiguity in `cp -r .`, where the source directory's own name can be appended
# to the destination, and it keeps untracked paths such as .git and .venv out of
# the mirror without having to delete them from the checkout.
cut -d/ -f1 "${TRACKED_PATHS}" | LC_ALL=C sort -u > "${TOP_LEVEL_PATHS}"
COPY_SOURCES=()
while IFS= read -r entry; do
  COPY_SOURCES+=("./${entry}")
done < "${TOP_LEVEL_PATHS}"

echo "Uploading $(wc -l < "${TRACKED_PATHS}" | tr -d ' ') tracked files to ${MIRROR_URL}..."
gcloud storage cp -r "${COPY_SOURCES[@]}" "${MIRROR_URL}"

# Listing the whole prefix costs seconds; it is only per-object metadata GETs
# that are slow against this bucket. Skip deletion entirely if the listing fails,
# rather than deleting against a partial view of the mirror.
if ! gcloud storage ls -r "${MIRROR_URL}/**" > "${MIRROR_URLS}"; then
  echo "Could not list ${MIRROR_URL}; leaving stale objects in place." >&2
  exit 0
fi

# Strip the "<MIRROR_URL>/" prefix by character count rather than by regex, since
# bucket and object names may contain regex metacharacters.
{ grep -v '/$' "${MIRROR_URLS}" || true; } \
  | cut -c "$(( ${#MIRROR_URL} + 2 ))-" \
  | LC_ALL=C sort > "${MIRROR_PATHS}"

comm -23 "${MIRROR_PATHS}" "${TRACKED_PATHS}" > "${STALE_PATHS}"

MIRROR_COUNT=$(wc -l < "${MIRROR_PATHS}" | tr -d ' ')
STALE_COUNT=$(wc -l < "${STALE_PATHS}" | tr -d ' ')

if (( STALE_COUNT == 0 )); then
  echo "Mirror matches the working tree; no objects to delete."
  exit 0
fi

MAX_DELETIONS=$(( MIRROR_COUNT * MAX_DELETION_PERCENT / 100 ))
if (( MAX_DELETIONS < MIN_ALLOWED_DELETIONS )); then
  MAX_DELETIONS="${MIN_ALLOWED_DELETIONS}"
fi

if (( STALE_COUNT > MAX_DELETIONS )); then
  echo "Refusing to delete ${STALE_COUNT} of ${MIRROR_COUNT} mirror objects; the limit is ${MAX_DELETIONS}." >&2
  echo "The first objects that would have been deleted:" >&2
  head -10 "${STALE_PATHS}" >&2
  exit 1
fi

echo "Deleting ${STALE_COUNT} mirror objects that are no longer tracked..."
awk -v prefix="${MIRROR_URL}/" '{ print prefix $0 }' "${STALE_PATHS}" \
  | gcloud storage rm -I
