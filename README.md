# Recidiviz Data Platform

At the center of Recidiviz is our platform for tracking granular criminal
justice metrics in real time. It includes a system for the ingest of corrections
records from different source data systems, and for calculation of various
metrics from the ingested records.

## License

This project is licensed under the terms of the GNU General Public License as
published by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

## Forking

The Recidiviz data system is provided as open source software - for transparency
and collaborative development, to help jump-start similar projects in other
spaces, and to ensure continuity if Recidiviz itself ever becomes inactive.

If you plan to fork the project for work in the criminal justice space (to
ingest from the same systems we are, or similar), we ask that you first
[contact us](mailto:hello@recidiviz.org) for a quick consultation. We work
carefully to ensure that our ingest activities don't disrupt other users'
experiences with the public data services we read, but if multiple ingest
processes are running against the same systems, without knowing about one
another, it may place excessive strain on them and impact the services those
systems provide.

If you have ideas or new work for the same data we're collecting, let us know
and we'll work with you to find the best way to get it done.

## Development

If you are contributing to this repository regularly for an extended period of
time,
[request GitHub collaborator access](mailto:hello@recidiviz.org?subject=GitHub%20collaborator%20request&body=)
to commit directly to the main repository.

### Local Development

#### Environment setup

##### Option 1: Local Python installation

If you can install `python3.9` locally, do so. For local Python development, you
will also need to install the `libpq` PostgreSQL client library and `openssl`.

On a Mac with [Homebrew](https://brew.sh/), you can install `python3.9` by first
installing `pyenv` with:

```bash
brew install pyenv
brew install xz
mkdir ~/.pyenv
```

Then, add the following to your `~/.zshrc` (or equivalent):

```
export PATH="$HOME/.local/bin:$PATH"
if command -v pyenv 1>/dev/null 2>&1; then
eval "$(pyenv init -)"
fi
```

Then run:

```
pyenv install 3.9.12
pyenv global 3.9.12
```

Verify that you have the correct version of python across contexts by opening a
new terminal window and running:

```
python -V
```

Once python is installed, you can install `libpq` and `openssl` with:

```bash
$ brew install postgresql@13 openssl
```

and add the following to your `~/.zshrc` (or equivalent):

```
export PATH="/opt/homebrew/opt/postgresql@13/bin:$PATH"
```

On Ubuntu 18.04,`openssl` is installed by default, you can install `python3.9`
and `libpq` with:

```bash
$ apt update -y && apt install -y python3.9-dev python3-pip libpq-dev
```

Install [`uv`](https://docs.astral.sh/uv/) for dependency management:

```bash
$ pip install uv
# or: curl -LsSf https://astral.sh/uv/install.sh | sh
```

[Fork this repository](https://github.com/Recidiviz/pulse-data/fork), clone
it locally, and enter its directory:

```
$ git clone git@github.com:your_github_username/pulse-data.git
$ cd pulse-data
```

To create a new virtual environment and install all project and development
dependencies, run the `initial_setup` script:

```bash
$ ./initial_setup.sh
```

On a Linux machine, you can also run:

```bash
$ uv sync --all-extras
```

To activate your virtual environment, run:

```bash
$ source .venv/bin/activate
```

Or run commands directly with `uv run`:

```bash
$ uv run pytest recidiviz/tests/path/to/test.py
```

On a Mac with [Homebrew](https://brew.sh/), you can install the JRE with:

```bash
$ brew install java
```

On Ubuntu 18.04, you can install the JRE with:

```bash
$ apt update -y && apt install -y default-jre
```

On a Mac with [Homebrew](https://brew.sh/), you can install jq (needed to deploy
calculation pipelines) with:

```bash
$ brew install jq
```

On Ubuntu 18.04, you can install jq with:

```bash
$ apt update -y && apt install -y jq
```

Finally, run `pytest`. As of Feb 2022, one might expect ~200 tests to fail
locally, with errors mainly falling into one of two categories:
`Receiver() takes no arguments` and
`Already initialized database/ValueError: Accessing SQLite in-memory database on multiple threads`.
The former error is due to an incompatibility with Cython that may be due to
newer Mac models or python versions, and the latter is due to tests not properly
cleaning up after themselves. All of these tests pass in CI. You can ignore any
failing tests with (for example):

```bash
$ pytest --ignore=recidiviz/tests/path/to/tests
```

##### Option 2: Docker container

If you can't install `python3.9` locally, you can use Docker instead.

See [below](#docker) for installation instructions. Once Docker is installed,
[fork this repository](https://github.com/Recidiviz/pulse-data/fork), clone
it locally, and enter its directory:

```bash
$ git clone git@github.com:your_github_username/pulse-data.git
$ cd pulse-data
```

Build the image:

```bash
$ docker build -t recidiviz-image . --build-arg DEV_MODE=True
```

Stop and delete previous instances of the image if they exist:

```bash
$ docker stop recidiviz && docker rm recidiviz
```

Run a new instance, mounting the local working directory within the image:

```bash
$ docker run --name recidiviz -d -t -v $(pwd):/app recidiviz-image
```

Open a `bash` shell within the instance:

```bash
$ docker exec -it recidiviz bash
```

Once in the instance's `bash` shell, sync your dependencies:

```bash
$ uv sync --all-extras
```

To activate your virtual environment, run:

```bash
$ source .venv/bin/activate
```

Finally, run `pytest`. If no tests fail, you are ready to develop!

Using this Docker container, you can edit your local repository files and use
`git` as usual within your local shell environment, but execute code and run
tests within the Docker container's shell environment. Depending on your IDE,
you may need to install additional plugins to allow running tests in the
container from the IDE.

#### Google Cloud

Recidiviz interacts with Google Cloud services using
[`google-cloud-*` Python client libraries](https://cloud.google.com/python/docs/reference).
During development, you may find it useful to verify the integration with these
services. First,
[install the Google Cloud SDK](https://cloud.google.com/sdk/docs/install), then
login to the SDK:

```bash
gcloud auth login --enable-gdrive-access --update-adc # Gets credentials to interact with services via the CLI
gcloud auth application-default login # Gets credentials which will be automatically read by our client libraries
```

Lastly, in a test script, use the
[`local_project_id_override` helper](https://github.com/Recidiviz/pulse-data/blob/c6972e132a6e68453e2d0baeb617b1f446a3e94f/recidiviz/utils/metadata.py#L69)
to override configuration used by our client library wrappers:

```python
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.environment import GCP_PROJECT_STAGING

# Override configuration used by our client libraries
with local_project_id_override(GCP_PROJECT_STAGING):
    # Google Cloud Client libraries will use `recidiviz-staging` in this context
```

Now the code run in the above context will interact directly with our staging
services. Use conservatively & exercise caution!

#### Terraform

Run the following to install Terraform:

```
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
```

To test your installation, run:

```
terraform -chdir=recidiviz/tools/deploy/terraform init -backend-config "bucket=recidiviz-staging-tf-state"
recidiviz/tools/deploy/terraform_plan.sh recidiviz-staging
```

If the above commands succeed, the installation was successful. For employees,
see more information on running Terraform at go/terraform.

#### Docker (🐳 [go/docker](http://go/docker))

Docker is needed for deploying new versions of our applications.

Follow these instructions to install Docker on Linux:

- [Debian](https://docs.docker.com/install/linux/docker-ce/debian/#install-using-the-repository)
- [Ubuntu](https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-using-the-repository)

Go to [this page](https://www.docker.com/products/docker-desktop) to download
Docker Desktop for Mac and Windows.

Once installed, increase the memory available to Docker to ensure it has enough
resources to build the container. On Docker Desktop, you can do this by going to
Settings > Resources and increasing Memory to 4GB.

#### Adding secrets

Recidiviz depends on sensitive information to run. This data is stored in Cloud
Datastore, which should be added manually to your production environment (see
`utils/secrets` for more information on the Datastore kind used).

#### Running tests

Individual tests can be run via `pytest filename.py`. To run all tests, go to
the root directory and run `pytest recidiviz`.

The configuration in `setup.cfg` and `.coveragerc` will ensure the right code is
tested and the proper code coverage metrics are displayed.

[A bug in the google client](https://github.com/googleapis/google-cloud-python/issues/5738)
requires that you have default application credentials. This should not be
necessary in the future. For now, make sure that you have done both
`gcloud config set project recidiviz` and
`gcloud auth application-default login`.

#### Checking code style

Run Pylint across the main body of code, in particular: `pylint recidiviz`.

The output will include individual lines for all style violations, followed by a
handful of reports, and finally a general code score out of 10. Fix any new
violations in your commit. If you believe there is cause for a rule change, e.g.
if you believe a particular rule is inappropriate in the codebase, then submit
that change as part of your inbound pull request.

#### Autoformatting

We use `black` to ensure consistent formatting across the code base and `isort`
to sort imports. There is a pre-commit hook that will format all of your files
automatically. It is defined in `githooks/pre-commit` and is installed by
`./initial_setup.sh`.

You can also set up your editor to run `black` and `isort` on save. See
[the black docs](https://black.readthedocs.io/en/stable/integrations/editors.html)
for how to configure external tools (both `black` and `isort`) to run in PyCharm
(more info in PyCQA/isort#258).

In VSCode just add the following to your `.vscode/settings.json`:

```json
    "editor.formatOnSave": true,
    "python.formatting.provider": "black",
    "[python.editor.codeActionsOnSave]": {
        "source.organizeImports": true
    },
```

#### Static type checking

Run Mypy across all code to check for static type errors: `mypy recidiviz`.

#### Static security checking

We use `bandit` to check for static security errors within the `recidiviz`
folder. This is run in the CI. Adding `# nosec` to the effected line will ignore
false positive issues.

### Deployment

Install the GCloud SDK using the
[interactive installer](https://cloud.google.com/sdk/docs/downloads-interactive).

Note: make sure the installer did not add
`google-cloud-sdk/platform/google_appengine` or subdirectories thereof to your
`$PYTHONPATH`, e.g. in your bash profile. This could break attempts to run tests
within the virtual environment by hijacking certain dependencies.

Make sure you have docker installed (see instructions above), then configure
docker authentication:

```bash
$ gcloud auth login
$ gcloud auth configure-docker
```

### Troubleshooting

If you see an error installing `psycopg2`:

```
An error occurred while installing psycopg2==...
```

On a Mac:

1. Ensure `postgresql` and `openssl` are installed with:
   `brew install postgresql openssl`
2. Run the setup script: `./initial_setup.sh`

On Linux: Ensure `libpq` is installed with:
`apt update -y && apt install -y libpq-dev`
