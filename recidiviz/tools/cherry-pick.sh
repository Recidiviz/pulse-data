#!/usr/bin/env bash
#
# Usage: ./cherry-pick.sh [PR_URL]
#   PR_URL    GitHub PR URL (e.g., https://github.com/Recidiviz/pulse-data/pull/50644)
#             Optional if resuming after resolving conflicts
#
# Requirements:
#   - gh (GitHub CLI) authenticated
#   - jq
#   - git
#
# This script will:
#   1. Extract PR info from the GitHub URL
#   2. Let user select target RC branch
#   3. Create a new branch off RC named {username}/cp-{PR-number}
#   4. Cherry-pick the merge commit from main
#   5. Create a new PR with [CHERRYPICK] prefix
#   6. Post notification to #cherry-pick-requests
#
# If conflicts occur:
#   1. Resolve conflicts manually
#   2. Run: git add <files> && git cherry-pick --continue
#   3. Re-run this script (with or without PR URL) to push and create PR
#

set -euo pipefail

# Source the deployment helpers to access utility functions
BASH_SOURCE_DIR=$(dirname "${BASH_SOURCE[0]}")
# shellcheck source=recidiviz/tools/script_base.sh
source "${BASH_SOURCE_DIR}/script_base.sh"
# shellcheck source=recidiviz/tools/deploy/deploy_helpers.sh
source "${BASH_SOURCE_DIR}/deploy/deploy_helpers.sh"

# Helper function to convert version tag to release branch name
function version_tag_to_release_branch {
    local VERSION_TAG=$1
    # Remove the 'v' prefix and extract major.minor version
    local VERSION_WITHOUT_V=${VERSION_TAG#v}
    local MAJOR_MINOR
    MAJOR_MINOR=$(echo "$VERSION_WITHOUT_V" | cut -d. -f1,2)
    echo "releases/v${MAJOR_MINOR}-rc"
}

# Helper function to prompt user for cherry-pick details
function prompt_cherry_pick_details {
    local PR_NUMBER=$1
    local PR_TITLE=$2
    local RC_CANDIDATE=$3
    
    echo ""
    echo "Please provide additional details for the cherry-pick request:"
    echo ""
    
    read -p "1. What is the issue? " -r ISSUE_DESCRIPTION
    echo ""
    
    read -p "2. What version introduced this issue to production? " -r ISSUE_VERSION
    echo ""
    
    echo "3. What is the type of cherry-pick request?"
    CHERRYPICK_TYPE=$(select_one "Select type" "Revert" "Minor configuration change" "Other forward fix")
    echo ""
    
    REVERT_REASON=""
    if [[ "$CHERRYPICK_TYPE" != "Revert" ]]; then
        read -p "4. Why were you unable to do a revert to fix your issue? " -r REVERT_REASON
        echo ""
    fi
    
    echo "5. Is the issue resolved in main? Select what you have done:"
    MAIN_STATUS=$(select_one "Select status" "Reverted the offending commit(s) in main" "Issued a PR for a forward fix in main" "Filed a task to fix the issue (provide link)")
    echo ""
    
    TASK_LINK=""
    if [[ "$MAIN_STATUS" == "Filed a task to fix the issue (provide link)" ]]; then
        read -p "   Please provide the task link: " -r TASK_LINK
        echo ""
    fi
    
    echo "6. Select the required PR label type:"
    PR_LABEL=$(select_one "Select label" "Type: Bug" "Type: Feature" "Type: Breaking Change" "Type: Non-breaking refactor" "Type: Configuration Change" "Type: Dependency Upgrade")
    echo ""

    # Export variables for use in PR body creation
    export ISSUE_DESCRIPTION ISSUE_VERSION CHERRYPICK_TYPE REVERT_REASON MAIN_STATUS TASK_LINK PR_LABEL
}

# Check if there's an active cherry-pick in progress
if [ -f .git/CHERRY_PICK_HEAD ]; then
    echo "⚠️  Cherry-pick in progress detected!"
    echo "Please complete the cherry-pick first:"
    echo "  - Resolve conflicts"
    echo "  - Run: git add <files> && git cherry-pick --continue"
    echo "  - Then re-run this script"
    exit 1
fi

# Get current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

# Check if we're resuming from a cherry-pick branch
if [[ "$CURRENT_BRANCH" =~ ^(.+)/cp-([0-9]+)(-.*)?$ ]]; then
    PR_AUTHOR="${BASH_REMATCH[1]}"
    PR_NUMBER="${BASH_REMATCH[2]}"

    echo "🔄 Detected existing cherry-pick branch: $CURRENT_BRANCH"
    echo "📝 Found cherry-pick for PR #${PR_NUMBER}"

    # Get PR details first
    REMOTE_URL=$(git remote get-url origin)
    if [[ "$REMOTE_URL" =~ github\.com[:/]([^/]+)/([^/.]+) ]]; then
        ORG="${BASH_REMATCH[1]}"
        REPO="${BASH_REMATCH[2]}"
    else
        echo "Error: Could not determine GitHub org/repo from remote"
        exit 1
    fi

    echo "🔍 Fetching PR information..."
    PR_DATA=$(gh pr view "$PR_NUMBER" --repo "${ORG}/${REPO}" --json title,author)
    PR_TITLE=$(echo "$PR_DATA" | jq -r '.title')
    echo "📝 PR Title: $PR_TITLE"
    echo ""

    # Present options to user
    echo "What would you like to do?"
    RESUME_ACTION=$(select_one "Select action" "Resume this cherry-pick (finish conflicts and create PR)" "Create a new cherry-pick PR for a different RC branch" "Exit")
    echo ""

    case "$RESUME_ACTION" in
        "Resume this cherry-pick (finish conflicts and create PR)")
            echo "🔄 Resuming cherry-pick..."

            # Get the base branch by finding merge base with RC branches
            git fetch

            # Get all RC branches
            IFS=$'\n' read -d '' -r -a RC_BRANCHES < <(git branch -r | grep -- '-rc$' | sed 's|origin/||' | sed 's/^[[:space:]]*//' | sort -V && printf '\0')

            if [ ${#RC_BRANCHES[@]} -eq 0 ]; then
                echo "Error: No RC candidate branches found"
                exit 1
            fi

            RC_CANDIDATE=""
            # Find which RC branch this cherry-pick branch is based on
            # Check if current branch contains any RC branch commits
            for rc_branch in "${RC_BRANCHES[@]}"; do
                # Check if the RC branch is an ancestor of the current branch
                if git merge-base --is-ancestor "origin/$rc_branch" HEAD 2>/dev/null; then
                    # Among ancestors, pick the most recent RC branch (highest version)
                    RC_CANDIDATE="$rc_branch"
                fi
            done

            # Fallback: if no exact merge base match, use the upstream branch
            if [ -z "$RC_CANDIDATE" ]; then
                RC_CANDIDATE=$(git rev-parse --abbrev-ref --symbolic-full-name "@{u}" 2>/dev/null | sed 's|origin/||' || echo "")
            fi

            # Final fallback: use latest RC branch
            if [ -z "$RC_CANDIDATE" ]; then
                RC_CANDIDATE=${RC_BRANCHES[$((${#RC_BRANCHES[@]}-1))]}
            fi

            echo "🎯 Target RC branch: $RC_CANDIDATE"

            # Prompt for cherry-pick details in resume mode too
            prompt_cherry_pick_details "$PR_NUMBER" "$PR_TITLE" "$RC_CANDIDATE"

            # Skip to push and PR creation
            NEW_BRANCH="$CURRENT_BRANCH"
            RESUME_MODE=true
            ;;

        "Create a new cherry-pick PR for a different RC branch")
            echo "🌿 Creating new cherry-pick for different RC branch..."

            # Continue with normal mode flow but skip URL parsing since we have PR info
            RESUME_MODE=false

            # We already have PR_NUMBER, PR_TITLE, ORG, REPO from above
            # Set PR_AUTHOR from the API data we fetched
            PR_AUTHOR=$(echo "$PR_DATA" | jq -r '.author.login')

            # Get merge commit
            PR_DATA_FULL=$(gh pr view "$PR_NUMBER" --repo "${ORG}/${REPO}" --json mergeCommit,state,mergedAt)
            IS_MERGED=$(echo "$PR_DATA_FULL" | jq -r '.mergedAt != null')
            if [ "$IS_MERGED" != "true" ]; then
                echo "Error: PR #${PR_NUMBER} has not been merged to main yet"
                exit 1
            fi
            MERGE_COMMIT=$(echo "$PR_DATA_FULL" | jq -r '.mergeCommit.oid')

            echo "👤 Author: $PR_AUTHOR"
            echo "📌 Merge Commit: $MERGE_COMMIT"

            # Continue to normal cherry-pick flow
            ;;

        "Exit")
            echo "👋 Exiting without making changes"
            exit 0
            ;;
    esac

    # If we're resuming the existing cherry-pick, skip to push/PR creation
    if [[ "$RESUME_ACTION" == "Resume this cherry-pick (finish conflicts and create PR)" ]]; then
        # Skip to push and PR creation section below
        :
    fi
else
    # Normal mode: require PR URL
    if [ $# -eq 0 ]; then
        echo "Error: Please provide a GitHub PR URL"
        echo "Usage: $0 <PR_URL>"
        exit 1
    fi

    PR_URL="$1"
    RESUME_MODE=false

    # Parse the PR URL to extract org, repo, and PR number
    if [[ "$PR_URL" =~ github\.com/([^/]+)/([^/]+)/pull/([0-9]+) ]]; then
        ORG="${BASH_REMATCH[1]}"
        REPO="${BASH_REMATCH[2]}"
        PR_NUMBER="${BASH_REMATCH[3]}"
    else
        echo "Error: Invalid GitHub PR URL format"
        echo "Expected format: https://github.com/org/repo/pull/number"
        exit 1
    fi

    echo "🔍 Fetching PR information for #${PR_NUMBER}..."

    # Get PR details using gh CLI
    PR_DATA=$(gh pr view "$PR_NUMBER" --repo "${ORG}/${REPO}" --json title,author,mergeCommit,state,mergedAt)

    # Check if PR is merged
    IS_MERGED=$(echo "$PR_DATA" | jq -r '.mergedAt != null')
    if [ "$IS_MERGED" != "true" ]; then
        echo "Error: PR #${PR_NUMBER} has not been merged to main yet"
        exit 1
    fi

    # Extract PR details
    PR_TITLE=$(echo "$PR_DATA" | jq -r '.title')
    PR_AUTHOR=$(echo "$PR_DATA" | jq -r '.author.login')
    MERGE_COMMIT=$(echo "$PR_DATA" | jq -r '.mergeCommit.oid')

    echo "📝 PR Title: $PR_TITLE"
    echo "👤 Author: $PR_AUTHOR"
    echo "📌 Merge Commit: $MERGE_COMMIT"
fi

# Shared cherry-pick flow for both normal mode and "new cherry-pick" mode
if [[ "$RESUME_MODE" == false ]]; then
    # Fetch latest changes
    echo ""
    echo "🔄 Fetching latest changes..."
    git fetch

    # Find available RC candidate branches and get deployment status
    echo ""
    echo "🔍 Finding available RC branches and checking deployment status..."

    # Get all RC branches sorted by version
    # Using read -a for compatibility with older bash versions, trim whitespace
    IFS=$'\n' read -d '' -r -a RC_BRANCHES < <(git branch -r | grep -- '-rc$' | sed 's|origin/||' | sed 's/^[[:space:]]*//' | sort -V && printf '\0')

    if [ ${#RC_BRANCHES[@]} -eq 0 ]; then
        echo "Error: No RC candidate branches found (expected format: *-rc)"
        exit 1
    fi

    # Get the latest two RC branches for selection
    LATEST_RC=${RC_BRANCHES[$((${#RC_BRANCHES[@]}-1))]}
    PREVIOUS_RC=""
    if [ ${#RC_BRANCHES[@]} -gt 1 ]; then
        PREVIOUS_RC=${RC_BRANCHES[$((${#RC_BRANCHES[@]}-2))]}
    fi

    # Get deployment status for staging
    echo "🚀 Checking deployment status for production..."
    DEPLOYED_VERSION_TAG=$(last_deployed_version_tag "recidiviz-123")
    DEPLOYED_BRANCH=$(version_tag_to_release_branch "$DEPLOYED_VERSION_TAG")

    echo "📋 Currently deployed version: $DEPLOYED_VERSION_TAG (branch: $DEPLOYED_BRANCH)"
    echo ""

    # Present branch options to user
    BRANCH_OPTIONS=()
    BRANCH_LABELS=()

    # Add latest RC with deployment indicator
    if [[ "$LATEST_RC" == "$DEPLOYED_BRANCH" ]]; then
        BRANCH_LABELS+=("$LATEST_RC (🛬 currently deployed)")
    else
        BRANCH_LABELS+=("$LATEST_RC (🛫 next)")
    fi
    BRANCH_OPTIONS+=("$LATEST_RC")

    # Add previous RC if it exists
    if [ -n "$PREVIOUS_RC" ]; then
        if [[ "$PREVIOUS_RC" == "$DEPLOYED_BRANCH" ]]; then
            BRANCH_LABELS+=("$PREVIOUS_RC (🛬 currently deployed)")
        fi
        BRANCH_OPTIONS+=("$PREVIOUS_RC")
    fi

    # Let user select target branch
    echo "Please select the target RC branch for cherry-picking:"
    RC_CANDIDATE=$(select_one "Select RC branch" "${BRANCH_LABELS[@]}")

    # Extract the actual branch name (remove deployment indicators)
    RC_CANDIDATE=${RC_CANDIDATE%% (*}

    echo ""
    echo "🎯 Selected RC branch: $RC_CANDIDATE"

    # Checkout the RC candidate branch
    echo ""
    echo "📦 Checking out $RC_CANDIDATE..."
    git checkout "$RC_CANDIDATE"
    git pull origin "$RC_CANDIDATE"

    # Create new branch name with RC branch identifier for uniqueness
    RC_VERSION=$(echo "$RC_CANDIDATE" | sed 's/releases\/v//' | sed 's/-rc$//')
    NEW_BRANCH="${PR_AUTHOR}/cp-${PR_NUMBER}-${RC_VERSION}"
    echo "🌿 Creating new branch: $NEW_BRANCH"
    git checkout -b "$NEW_BRANCH"

    # Cherry-pick the merge commit
    echo ""
    echo "🍒 Cherry-picking commit $MERGE_COMMIT..."
    if git cherry-pick "$MERGE_COMMIT"; then
        echo "✅ Cherry-pick successful!"
    else
        echo ""
        echo "❌ Cherry-pick failed with conflicts."
        echo ""
        echo "To resolve:"
        echo "  1. Fix conflicts in the listed files"
        echo "  2. Run: git add <files>"
        echo "  3. Run: git cherry-pick --continue"
        echo "  4. Re-run this script (without URL - it will detect your branch)"
        exit 1
    fi

    # Prompt for cherry-pick details
    prompt_cherry_pick_details "$PR_NUMBER" "$PR_TITLE" "$RC_CANDIDATE"
fi

# Push the new branch
echo ""
if [ "$RESUME_MODE" = true ]; then
    echo "⬆️  Pushing resolved changes to remote..."
else
    echo "⬆️  Pushing branch to remote..."
fi
git push -u origin "$NEW_BRANCH"

# Create the new PR
echo ""
echo "🎉 Creating cherry-pick PR..."
NEW_PR_TITLE="[CHERRYPICK] $PR_TITLE"

# Build the PR body using the template (both normal and resume mode)
# Format the checklist items based on user input
REVERT_CHECKBOX=""
CONFIG_CHECKBOX=""
OTHER_CHECKBOX=""
case "$CHERRYPICK_TYPE" in
    "Revert")
        REVERT_CHECKBOX="- [x] Revert"
        CONFIG_CHECKBOX="- [ ] Minor configuration change"
        OTHER_CHECKBOX="- [ ] Other forward fix"
        ;;
    "Minor configuration change")
        REVERT_CHECKBOX="- [ ] Revert"
        CONFIG_CHECKBOX="- [x] Minor configuration change"
        OTHER_CHECKBOX="- [ ] Other forward fix"
        ;;
    "Other forward fix")
        REVERT_CHECKBOX="- [ ] Revert"
        CONFIG_CHECKBOX="- [ ] Minor configuration change"
        OTHER_CHECKBOX="- [x] Other forward fix"
        ;;
esac

# Format main branch status
MAIN_REVERT_CHECKBOX=""
MAIN_FIX_CHECKBOX=""
MAIN_TASK_CHECKBOX=""
case "$MAIN_STATUS" in
    "Reverted the offending commit(s) in main")
        MAIN_REVERT_CHECKBOX="- [x] Revert the offending commit(s) in main"
        MAIN_FIX_CHECKBOX="- [ ] Issue a PR for a forward fix in main"
        MAIN_TASK_CHECKBOX="- [ ] If we can't do either of the above fast enough, file a task to fix the issue and link here:"
        ;;
    "Issued a PR for a forward fix in main")
        MAIN_REVERT_CHECKBOX="- [ ] Revert the offending commit(s) in main"
        MAIN_FIX_CHECKBOX="- [x] Issue a PR for a forward fix in main"
        MAIN_TASK_CHECKBOX="- [ ] If we can't do either of the above fast enough, file a task to fix the issue and link here:"
        ;;
    "Filed a task to fix the issue (provide link)")
        MAIN_REVERT_CHECKBOX="- [ ] Revert the offending commit(s) in main"
        MAIN_FIX_CHECKBOX="- [ ] Issue a PR for a forward fix in main"
        MAIN_TASK_CHECKBOX="- [x] If we can't do either of the above fast enough, file a task to fix the issue and link here: $TASK_LINK"
        ;;
esac

# Build revert reason section
REVERT_REASON_SECTION=""
if [[ "$CHERRYPICK_TYPE" != "Revert" ]]; then
    REVERT_REASON_SECTION="**4. If this is not a revert, why were you unable to do a revert to fix your issue?**
$REVERT_REASON

"
fi

NEW_PR_BODY=$(cat << EOF
## Description of the change

Cherry-pick [#${PR_NUMBER}] into \`${RC_CANDIDATE}\`.

**1. What is the issue?**
$ISSUE_DESCRIPTION

**2. What version introduced this issue to production?**
$ISSUE_VERSION

**3. What is the type of cherry-pick request?**
$REVERT_CHECKBOX
$CONFIG_CHECKBOX
$OTHER_CHECKBOX

${REVERT_REASON_SECTION}**5. Is the issue resolved in main? If not, please select which you have done:**
$MAIN_REVERT_CHECKBOX
$MAIN_FIX_CHECKBOX
$MAIN_TASK_CHECKBOX
EOF
)

NEW_PR_URL=$(gh pr create \
    --repo "${ORG}/${REPO}" \
    --base "$RC_CANDIDATE" \
    --head "$NEW_BRANCH" \
    --title "$NEW_PR_TITLE" \
    --body "$NEW_PR_BODY" \
    --label "$PR_LABEL")

# Post notification to #cherry-pick-requests
echo ""
echo "📢 Posting notification to #cherry-pick-requests..."


SLACK_CHANNEL_CHERRY_PICK="C019ZQZ5EHK"
SLACK_MESSAGE="🌸 <${NEW_PR_URL}|#${PR_NUMBER}> $PR_TITLE into \`${RC_CANDIDATE}\` from <https://github.com/${PR_AUTHOR}|@${PR_AUTHOR}>"

deployment_bot_message "recidiviz-staging" "$SLACK_CHANNEL_CHERRY_PICK" "$SLACK_MESSAGE" > /dev/null || {
    echo "⚠️  Failed to post Slack notification. Please notify #cherry-pick-requests manually."
}

echo ""
echo "✨ Done! Cherry-pick PR created successfully."
echo "🌐 Opening PR in browser: $NEW_PR_URL"
open "$NEW_PR_URL"
