# Dev Container

This directory contains configurations for creating a [Dev Container](https://code.visualstudio.com/docs/devcontainers/containers) for pulse-data. Dev containers are an optional way to set up a VSCode environment, where development occurs inside a container (still on the user's machine) that is configured in a standardized way.

## Pre-reqs
1. If you do not already have gcloud configured on your computer, log in to gcloud by running the following two commands:

```bash
gcloud auth login # Gets credentials to interact with services via the CLI
gcloud auth application-default login # Gets credentials which will be automatically read by our client libraries
```
1. Verify that the directory `~/.config/gcloud` exists. It should have been created when you authenticated to Gcloud

## How to use

Dependencies:

1. [VSCode](https://code.visualstudio.com/download)
1. Docker as per [VSCode requirements](https://code.visualstudio.com/docs/devcontainers/containers#_system-requirements)
1. Install the Docker VSCode extension (`ms-azuretools.vscode-docker`)
1. Install the Microsoft Dev Containers VSCode extension (`ms-vscode-remote.remote-containers`)

To use:

1. Open the pulse-data repository inside of VSCode
1. You may see a pop-up that says you can open this workspace inside a dev container. If you do, click on "Reopen In Container". If not, open the command palette (cmd-shift-P on a mac) and select "Dev Containers: Reopen In Container".
1. Be patient; it takes some time to create the container and install all dependencies. You can click on "Starting Dev Container (show log)" (or "Dev Containers: Show Container Log" from the command palette) to see progress. When it's done, you'll see the message "Done. Press any key to close the terminal." in the logs.
1. [Optional] Open a new terminal window by choosing "Terminal: Create New Terminal" from the command palette.

After the first time you build the container, you'll also need to do the following:

1. Select the uv python interpreter by choosing "Python: Select Interpreter" from the command palette and selecting the one from the `.venv` directory

Limitations:

1. Running services via docker (e.g. admin panel, case triage backend, etc.) does not work (yet)
1. The container might not build on windows due to the way we share gcloud credentials between your machine and the container.
1. As of June 2023, this is new functionality for Recidiviz there are likely to be some rough edges! If you use this and encounter any weirdness, please post in Slack in #eng or file a ticket!
