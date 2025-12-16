---
name: upgrade-cloud-composer
description: Upgrade Cloud Composer (Apache Airflow) to a new version. Use when the user asks to upgrade, update, or bump Airflow, Cloud Composer, or Airflow dependencies to a new version. Handles updating terraform config and Python dependencies.
---

# Skill: Upgrade Cloud Composer

## Overview

This skill defines how to upgrade Cloud Composer (Google Cloud's managed Apache Airflow) to a new version in the pulse-data repository. This involves updating the Composer image version and syncing all Python dependencies.

**What Claude does**: Updates terraform config and Pipfile with new versions.
**What the user does**: Provides package list, commits/pushes changes, triggers GitHub Actions, creates PR, verifies DB size.

## Instructions

### Step 1: Identify Current and Target Versions

1. **Check current version**: Read `recidiviz/tools/deploy/terraform/cloud-composer.tf` and find the `image_version` field (e.g., `composer-2.13.8-airflow-2.10.5`)

2. **Suggest latest version**: Check https://docs.cloud.google.com/composer/docs/composer-versions#images-composer-2 for the latest Cloud Composer 2 version available. Suggest this version to the user but allow them to specify a different target version.

   Version format: `composer-X.Y.Z-airflow-A.B.C`

3. **Search for Dependabot alerts**: Check for Dependabot security alerts that might be resolved by this upgrade:
   - **Get unfixed alerts**: `gh api --paginate /repos/Recidiviz/pulse-data/dependabot/alerts --jq '.[] | select(.state == "open" or .state == "dismissed" or .state == "auto_dismissed") | select(.dependency.manifest_path | contains("recidiviz/airflow/Pipfile")) | {number, state, package: .dependency.package.name, manifest: .dependency.manifest_path, severity: .security_advisory.severity, url: .html_url}'`
     - **IMPORTANT**: Use `--paginate` to get all alerts (the API paginates at 30 items per page)
     - Get all unfixed alerts by filtering for states: "open", "dismissed", and "auto_dismissed" (everything except "fixed")
     - **Why check dismissed/auto_dismissed?** These represent unfixed vulnerabilities. Dismissing an alert doesn't update the Composer environment. Only a Cloud Composer upgrade can resolve them.
     - Filter for alerts where `manifest_path` contains `recidiviz/airflow/Pipfile` - these are relevant to Cloud Composer upgrades
   - **Also check for open Dependabot PRs**: `gh pr list --author app/dependabot --state open`
     - Look for PRs that update packages in `recidiviz/airflow/Pipfile` (these often fail to actually fix the issue because they only update the Pipfile, not the production Composer environment)
     - Cross-reference PR package names with the alerts from above
   - Note the Dependabot alert URLs (format: `https://github.com/Recidiviz/pulse-data/security/dependabot/XXXX`)
   - Keep track of which packages have alerts, their states, what versions are needed to resolve them, and any associated PRs

4. **Gather upgrade context**: Ask the user for the reason for this upgrade:
   - Security alerts (from Dependabot search above)?
   - New features or bug fixes?
   - Maintenance (e.g., approaching support end date)? If so, get the issue number
   - Any other context?

   This information will be used in the PR description later

### Step 2: Get Package Versions for Target Cloud Composer Version

**CRITICAL**: Cloud Composer environments include specific pinned versions of all Python packages. You MUST use the exact versions from Google's documentation.

Construct the URL for the package list by replacing dots with dashes in the version string:
- URL pattern: `https://docs.cloud.google.com/composer/docs/versions-packages#composer-X-Y-Z-airflow-A-B-C`
- Example: For `composer-2.15.3-airflow-2.10.5`, the URL is:
  `https://docs.cloud.google.com/composer/docs/versions-packages#composer-2-15-3-airflow-2-10-5`

**Ask the user to**:
1. Open the URL above
2. Find the section "Packages in composer-X.Y.Z-airflow-A.B.C"
3. Copy-paste all the package names and versions from that section

Once the user provides the package list:
1. Parse the package versions
2. **Verify which Dependabot alerts will be resolved**: Compare the new package versions against the alerts found in Step 1.3
   - Check if the new versions meet the minimum required versions from the alerts
   - Note which Dependabot alert URLs will be resolved (these will go in the PR's "Related issues" section)
3. Proceed to update the files

### Step 3: Update Terraform Configuration

Update `recidiviz/tools/deploy/terraform/cloud-composer.tf`:

Find the `image_version` line under `google_composer_environment.default_v2.config.software_config` and update it:

```terraform
image_version = "composer-X.Y.Z-airflow-A.B.C"
```

Use the Edit tool to make this change.

### Step 4: Update Python Dependencies in Pipfile

**CRITICAL**: The `recidiviz/airflow/Pipfile` contains ALL dependencies pinned to exact versions. These MUST match the Cloud Composer environment.

**Use the update script** to update the Pipfile efficiently:

1. **Save the package list** that the user provided to a temporary file:
   ```bash
   cat > /tmp/composer_packages.txt << 'EOF'
   [paste user-provided package list here]
   EOF
   ```

2. **Run the update script**:
   ```bash
   python .claude/skills/upgrade_cloud_composer/update_pipfile.py \
     /tmp/composer_packages.txt \
     recidiviz/airflow/Pipfile
   ```

3. **Review the script output**:
   - Check how many packages were updated
   - Check if any new packages were added
   - **Important**: Review any normalization warnings - these indicate potential package name mismatches

**Package Name Normalization**:
- Google Cloud docs sometimes use dots (`.`) or underscores (`_`) in package names
- Pipfile uses dashes (`-`) for the same packages
- Examples:
  - `backports.tarfile` in docs → `backports-tarfile` in Pipfile
  - `jaraco.classes` in docs → `jaraco-classes` in Pipfile
- The script automatically handles this normalization

**Note**: The Pipfile has a comment explaining why packages are pinned:
```python
# We pin all dependencies to make sure they remain on versions supported by our current
# Cloud Composer environment. We must manually update the dependencies when we update
# our Cloud Composer version.
```

### Step 5: User Action - Commit, Push, and Regenerate Pipfile.lock

**DO NOT regenerate Pipfile.lock locally**.

Prompt the user to perform the following steps:

1. Create a new branch for this upgrade
2. Commit the changes (terraform + Pipfile)
3. Push the branch to GitHub
4. Trigger the "Re-lock Airflow Pipenv" GitHub Action for their branch:
   - Go to: https://github.com/Recidiviz/pulse-data/actions/workflows/pipenv-lock-airflow.yaml
   - Click "Run workflow" and select their branch
5. Wait for the action to complete
6. Pull the updated `Pipfile.lock` from their branch

### Step 6: Check for Test Updates

Major Airflow version changes may require test updates due to API changes.

Look at recent upgrade PRs (use `git log` to find PRs that upgraded Cloud Composer) to see if similar test changes are needed:

Common test files that may need updates:
- `recidiviz/airflow/tests/utils/dag_helper_functions.py`
- `recidiviz/airflow/tests/utils/kubernetes_helper_functions.py`
- Specific DAG test files

If the Airflow version jump is small (e.g., patch version only), test updates are less likely needed.

### Step 7: User Action - Verify DB Size and Create Pull Request

**Prompt the user** to check the Airflow metadata DB sizes before creating the PR:

1. Ask them to check DB sizes in Cloud Monitoring:
   - Staging: https://console.cloud.google.com/monitoring/metrics-explorer?project=recidiviz-staging (filter for `composer.googleapis.com/environment/database/airflow/size`)
   - Production: https://console.cloud.google.com/monitoring/metrics-explorer?project=recidiviz-123 (same filter)

2. Ask them to report back the sizes (e.g., "staging is ~1.3 GiB, prod is ~2.2 GiB")

3. If either DB size > 3 GiB, warn that the upgrade may timeout and suggest running the metadata maintenance DAG first

Once the user provides the DB sizes, use those values to fill in the PR description template:

Provide the user with a draft PR description using the repo's PR template:

**Title format**: `[Build] Upgrade Cloud Composer to composer-X.Y.Z-airflow-A.B.C`

**Labels**: `Type: Dependency Upgrade`

**Body template**:
```markdown
## Description of the change

Upgrades from Cloud Composer version `composer-OLD-VERSION` to `composer-NEW-VERSION`.
[Explain reason based on context from Step 1.4: security alerts, new features, maintenance, etc.]

[If there are Dependabot alerts being resolved, mention them here, e.g.: "This upgrade addresses security alerts for the `tornado` and `protobuf-python` packages."]

The Airflow metadata DB size in staging is ~X.X GiB and in prod is ~Y.Y GiB,
so we should be ok to upgrade without doing any further DB pruning.

## Type of change

| Label                       	| Description                                                                                               	|
|-----------------------------	|-----------------------------------------------------------------------------------------------------------	|
| Type: Bug                   	| non-breaking change that fixes an issue                                                                   	|
| Type: Feature               	| non-breaking change that adds functionality                                                               	|
| Type: Breaking Change       	| fix or feature that would cause existing functionality to not work as expected                            	|
| Type: Non-breaking refactor 	| change addresses some tech debt item or prepares for a later change, but does not change functionality    	|
| Type: Configuration Change  	| adjusts configuration to achieve some end related to functionality, development, performance, or security 	|
| Type: Dependency Upgrade      | upgrades a project dependency - these changes are not included in release notes                             |

## Related issues

Closes #XXXXX
[Include any Dependabot alert URLs that will be resolved, e.g.: "Closes https://github.com/Recidiviz/pulse-data/security/dependabot/1393"]
[Include any open Dependabot PRs that will be superseded, e.g.: "Supersedes #51122"]

## Checklists

### Development

**This box MUST be checked by the submitter prior to merging**:
- [x] **Double- and triple-checked that there is no Personally Identifiable Information (PII) being mistakenly added in this pull request**

These boxes should be checked by the submitter prior to merging:
- [ ] Tests have been written to cover the code changed/added as part of this pull request

### Code review

These boxes should be checked by reviewers prior to merging:

- [ ] This pull request has a descriptive title and information useful to a reviewer
- [ ] Potential security implications or infrastructural changes have been considered, if relevant
```

The user can create the PR using `gh pr create` or through the GitHub web UI.

**Important Note for PR**: Remind the user to add a comment to the PR or mention in the description that:
- The next staging and production deploys after this PR merges will potentially take a long time (as long as an hour)
- Platform on-call should be notified before merging

### Step 8: User Action - Notify On-Call and Update Deploy Log

**Before merging the PR**, remind the user to:

1. **Notify platform on-call** that the next staging and production deploys will potentially take a long time (up to an hour) due to the Composer upgrade
2. **Update the deploy log** at https://go/platform-deploy-log:
   - Add a note on the appropriate staging/production deploy pages
   - Mention that the Composer upgrade is in progress and may take up to an hour

### Step 9: User Action - Post-Merge Monitoring

After the PR is merged, remind the user to:

1. Watch the Terraform plan output in CI
2. Monitor the Cloud Composer environment upgrade in GCP console (this can take up to an hour)
3. Verify Airflow UI is accessible after upgrade
4. Check that DAGs run successfully
5. Verify any security alerts are resolved
6. Update the deploy log at https://go/platform-deploy-log once the upgrade completes successfully

## Common Gotchas

- **Large Metadata DB**: If DB > 3 GiB, upgrade may timeout. Run maintenance DAG first.
- **Breaking API Changes**: Major Airflow version jumps may require test updates.
- **Never regenerate Pipfile.lock locally**: Always use the GitHub Action.
- **Version Availability**: Cloud Composer versions aren't released simultaneously across regions.
- **Failed Upgrades**: If upgrade fails (timeout, etc.), you may need to revert and address root cause first.

## Examples

### Example: Minor Version Bump (Patch Update)

**Scenario**: Upgrading from `composer-2.13.1-airflow-2.10.5` to `composer-2.13.8-airflow-2.10.5` to fix security alerts.

**Claude actions**:
1. Check current version: `composer-2.13.1-airflow-2.10.5`
2. Check for unfixed Dependabot alerts related to airflow/Pipfile (open/dismissed/auto_dismissed), and cross-reference with open Dependabot PRs
3. Suggest target: `composer-2.13.8-airflow-2.10.5`
4. Provide package list URL to user: `https://docs.cloud.google.com/composer/docs/versions-packages#composer-2-13-8-airflow-2-10-5`
5. After user provides packages, update `cloud-composer.tf`: `image_version = "composer-2.13.8-airflow-2.10.5"`
6. Save package list to `/tmp/composer_packages.txt` and run update script to update Pipfile
7. Review script output for normalization warnings
8. Note: No test updates likely needed (minor version bump)
9. Provide PR template with draft description

**User actions**:
1. Copy-paste package list from Google docs
2. Create branch, commit, and push changes
3. Trigger "Re-lock Airflow Pipenv" action
4. Verify DB size in Cloud Monitoring
5. Create PR referencing security alerts and noting the long deploy time
6. Notify platform on-call and update deploy log before merging
7. Monitor the upgrade (up to 1 hour)

See: PR #45946

### Example: Major Version Jump

**Scenario**: Upgrading from `composer-2.9.7-airflow-2.7.3` to `composer-2.13.1-airflow-2.10.5` before support end date.

**Claude actions**:
1. Check current version: `composer-2.9.7-airflow-2.7.3`
2. Check for unfixed Dependabot alerts related to airflow/Pipfile (open/dismissed/auto_dismissed), and cross-reference with open Dependabot PRs
3. Suggest target: `composer-2.13.1-airflow-2.10.5`
4. Provide package list URL to user
5. After user provides packages, update `cloud-composer.tf`
6. Save package list and run update script to update Pipfile (many changes expected)
7. Review script output for new packages and normalization warnings
8. Review test changes from similar PRs - major version jump may need:
   - Updates to `kubernetes_helper_functions.py`
   - Updates to DAG test files
9. Provide PR template with draft description

**User actions**:
1. Copy-paste package list from Google docs
2. Create branch, commit, and push changes
3. Trigger "Re-lock Airflow Pipenv" action
4. Verify DB size < 3 GiB (critical for major upgrades)
5. Create PR referencing support end date issue and noting the long deploy time
6. Notify platform on-call and update deploy log before merging
7. Monitor the upgrade (up to 1 hour, potentially longer for major version jumps)

See: PR #42821

## Related Documentation

- [Cloud Composer Versions (Google Cloud)](https://docs.cloud.google.com/composer/docs/composer-versions#images-composer-2)
- [Airflow README](../../recidiviz/airflow/README.md)
- [Cloud Composer Terraform Config](../../recidiviz/tools/deploy/terraform/cloud-composer.tf)
- Historical upgrade PRs: #45946, #42821, #38979
