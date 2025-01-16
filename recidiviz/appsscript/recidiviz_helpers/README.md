# Recidiviz Helpers Apps Script Library

Files within an Apps Script project share global variables and functions, but by
default, projects are isolated from each other. This directory contains utility
functions that have been made accessible to different projects as an Apps Script
Library.

## Using Recidiviz Helpers

To gain access to Recidiviz Helpers functions in a project that doesn't already
use it, follow
[the instructions in the Libraries documentation](https://developers.google.com/apps-script/guides/libraries#add_a_library_to_your_script_project).

## Developing on Recidiviz Helpers

The library project is owned by the `apps-script@recidiviz.org` email and can be
found in the Apps Script UI by logging into that account.

After making changes to this library, you should deploy the changes, bump the
version of the library in all dependent projects, and include the version bumps
in the PR. There are two ways to do this:

1. Locally: First, run `clasp deploy` to create a new deployment. Then, edit the
   `appsscript.json` files of all dependent projects to reference the new
   deployment's version number. Finally, make sure to both commit/push these
   changes and use `clasp push -f` to overwrite the remote `appsscript.json`
   files for all of the dependent projects.

2. Through the Apps Script UI: First, click "Deploy" and create a new
   deployment, of the type Library. Then, open all the dependent projects and
   change the version number of this library, under "Libraries" in the left
   sidebar. Finally, `clasp pull` and commit/push these changes so they are
   included in your PR.
