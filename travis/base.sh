# Prepends '> ' to each line of input.
# Append `2>&1 | ind` to all commands so that all output goes to stdout and gets
# run through this function.
BLUE=$(tput setaf 4)
RESET=$(tput sgr0)
function ind {
    sed "s/^/$BLUE>$RESET /"
}

# Use `set -e` to exit the script if any command fails and pass along its exit
# status.
set -e

# Fails the command if any command in a pipeline fails.
set -o pipefail

# Fail if a variable is unset, instead of substituting a blank.
set -u

# Use `set -v` so that the commands are printed to Travis. We should never use
# `set -x` as that expands variables before printing the commands, and some of
# the variables are secrets.
set -v
