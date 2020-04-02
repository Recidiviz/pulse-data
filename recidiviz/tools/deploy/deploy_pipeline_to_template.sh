#!/usr/bin/env bash

project=$1
# Shift moves the next arg to the front of the arg list. These shift statements
# are here so that we can read in `metric_types` as a list using $@ after
# reading in all other args.
shift
bucket=$1
shift
pipeline=$1
shift
job_name=$1
shift
input=$1
shift
reference_input=$1
shift
output=$1
shift

# Optional arguments
state_code=$1
shift
calculation_month_limit=$1

# The metric_types arg needs to be the last argument, if present, because it is
# read in as a list.
shift
metric_types=$@


# Hard-code to this region until there's a need to allow deploying into separate regions
region=us-west1

if [ x"$project" == x -o x"$bucket" == x -o x"$pipeline" == x -o x"$job_name" == x -o x"$input" == x -o x"$output" == x -o x"$reference_input" == x ]; then
    echo "usage: $0 <project> <bucket> <pipeline> <job_name> <input> <output> <reference_input>"
    exit 1
fi

function run_cmd {
    cmd="$1"
    echo "Running \`$cmd\`"
    $cmd

    ret_code=$?
    if [ $ret_code -ne 0 ]; then
        echo "Failed with exit code $ret_code"
        exit $ret_code
    fi
}

# We hard-code to n1-standard-4 below because we don't want low-level config like worker provisioning to be a high-level
# parameter for configuration. Similarly, we hard-code to using non-public IP addresses in a specific subnet where our
# NAT is enabled.
echo "Deploying $pipeline pipeline to template"
command="python -m recidiviz.tools.run_calculation_pipelines
    --pipeline $pipeline
    --job_name $job_name
    --project $project
    --save_as_template
    --region $region
    --input $input
    --reference_input $reference_input
    --output $output"

# Append optional arguments if they are not empty
[[ -z "$state_code" ]]  || command="$command --state_code $state_code"
[[ -z "$calculation_month_limit" ]]  || command="$command --calculation_month_limit=$calculation_month_limit"
[[ -z "$metric_types" ]] || command="$command --metric_types $metric_types"


run_cmd "${command}"
