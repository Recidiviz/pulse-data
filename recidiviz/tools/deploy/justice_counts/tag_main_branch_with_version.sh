#!/usr/bin/env bash

: "Script that applies a Git tag to the HEAD commit of the main branch within the Github repository.

Example usage:
./recidiviz/tools/deploy/justice_counts/tag_main_branch_with_version.sh -t <TAG>  
"

BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/../../script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/../deploy_helpers.sh"

function print_usage {
    echo_error "usage: $0 -v VERSION"
    echo_error "  -v: Version to be deployed to production (e.g. v1.XXX.0)."
    run_cmd exit 1
}

while getopts "v:" flag; do
  case "${flag}" in
    v) VERSION="$OPTARG" ;;
    *) print_usage
       run_cmd exit 1 ;;
  esac
done

if [[ -z ${VERSION} ]]; then
    echo_error "Missing/empty version argument"
    print_usage
    run_cmd exit 1
fi

if [[ ! ${VERSION} =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo_error "Invalid version - must be of the format vX.Y.Z"
    run_cmd exit 1
fi

# Get the name of the repository.
git_remote_url=$(git config --get remote.origin.url)
repository_name=$(basename -s .git "$git_remote_url")
TAG=jc.${VERSION}

echo "Creating tag [${TAG}] for $repository_name..."
run_cmd git tag -m "Justice Counts version [$VERSION] release - $(date +'%Y-%m-%d %H:%M:%S')" "${TAG}"

echo "Pushing tag [${TAG}] to remote..."
run_cmd git push origin "${TAG}"
