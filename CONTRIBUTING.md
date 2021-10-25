# Contributing to Recidiviz

Welcome and thank you for contributing to Recidiviz! We believe that our mission
is critical to the long-term health of society. All contributions help to
realize the goal of a more fair and effective criminal justice system.

_From little things big things grow_. No contribution is minor.

The following are some brief guidelines to help make your experience smooth and
impactful, and to keep Recidiviz healthy as we grow.

## Code of Conduct

The Recidiviz project and everyone participating in it are governed by the
[Recidiviz Code of Conduct](CODE_OF_CONDUCT.md). By contributing, you agree to
uphold this code and to treat others with dignity and respect. Please report
unacceptable behavior to [core-team@recidiviz.com](mailto:core-team@recidiviz.com).
This is a small group of project leads who will maintain confidentiality.

## Providing Feedback

**Note #1**: Please do not include any individualized criminal justice data, or
Personally Identifiable Information (PII) of any kind, as part of your filed
issue, be it bug, feature request, or otherwise. If you would like to share
such data as part of conveying your issue, and are permitted to do so, reach out
to our team without submitting private information, and request a direct contact
with whom to communicate securely.

**Note #2**: If you feel that you cannot meet any of the suggested requirements
for reporting bugs or feature requests below, err on the side of reporting them
anyway! We'd rather take the time to read something with less information than
miss out on your help.

### Reporting Bugs

Send any bugs you find our way by filing an issue against one of our
repositories.

Please provide as much information as you can to help us reproduce and fix the
bug. We have issue templates from which to choose when filing new issues.
Generally, they require:

* A concise title and clear description of the bug and how it manifests
* Precise, ordered steps to reproduce it
* A description of what you expected to occur and how it differed from reality
* A description of any relevant data that was present and//or used (_sans PII of
any kind_)
* Environment information, such as your OS or available memory, or versions of
Python, Google Cloud SDK, or any relevant libraries

### Submitting Feature Requests

We welcome any and all feature requests! Submit these by filing an issue, again
to what you believe to be the right repository.

Provide as much information as you can to help us consider and prioritize the
feature. Again, we have issue templates to help. They include:

* A concise title and clear description of the suggested feature
* Why you believe it would be valuable, and the impact you believe it would
provide
* If a change in existing functionality, why you believe this is advantageous to
the current functionality

### Questions, Comments, Concerns

Please do not submit issues to ask questions about the project or our
organization, provide general comment, or express concerns. If you are in our
Slack space, then feel free to reach out there. Otherwise, we'd love to hear
from you at [team@recidiviz.org](mailto:team@recidiviz.org).

## Contributing Code

### Selecting an Issue

Our backlog of tasks is labeled and open for browsing under the [project issues](https://github.com/Recidiviz/pulse-data/issues).
All issues which are not currently assigned are open for contribution. Please
assign an issue to yourself if you intend to submit a pull request soon. If you
are interested but unsure about contributing to a particular issue, leave a
comment on that issue.

In general, you will find that our issues are separated into vertical
functionality, e.g. `Subject: Entity Matching`, and also into horizontal type,
e.g. `Type: Feature` or `Type: Bug`. Take a look at the code and consider 
jumping first into an area where you feel more comfortable, and then perhaps 
into an area where you feel less comfortable or need to learn something new.

**Note**: at present, our repository is structured as a private repo on which
development takes place, with commits mirrored out to a public repo. If you are
not yet a collaborator on the private repo, the notes above do not apply. We are
always evaluating other modes of structuring work for volunteers and welcome 
input!

### Doing the Work

**tl;dr:**
1. Work on a feature branch
1. Run linting and tests often
1. Ensure your change has test coverage (_ideally complete coverage_)
1. Submit a descriptive pull request

Once you have selected an issue and assigned it to yourself, you will start
writing the code on an appropriately named branch on your local machine. If you
are part of the Recidiviz Team, you may push branches directly to the
`recidiviz/pulse-data` remote. Otherwise, you may push branches to a [fork](https://help.github.com/articles/fork-a-repo/)
of the repository.

Before issuing a pull request, run our linting and test suite over your changes
to ensure no regression, as described in [our Readme](README.md).

These same commands will be run by our continuous integration (CI) suite and
your pull request will be "built" automatically to show whether it passes.
Stay ahead of the curve by running these throughout the development process, 
which will help you avoid any surprises.

Related, include unit and//or integration tests to cover any new code. Update or
remove existing test cases where appropriate.

When you are passing linting and testing locally and are satisfied with the
current state of your work, submit a pull request against `main`. However,
you can also submit Work In Progress ("WIP") pull requests to gather early
feedback if you believe it would be helpful. If you do so, please format the
title as "WIP: My title here" and place the PR in a "Draft" state via Github's UI.

Make sure the pull request description explicitly lists everything that the work
does, and references any issues it contributes to.

### Before Merging

Before merging, all commits should be rebased and squashed down into a single
atomic commit that does one thing. Squashing can be done either locally by hand
or by an approver with the "Squash and Merge" button.

If your pull request does two separate things, it should likely (but not
necessarily) be multiple pull requests. We prefer for pull requests to be
focused on one feature, issue, etc.

We will not merge any pull requests that do not pass our CI suite, particularly
those that do not pass linting, that do not pass all tests, or that avoidably
lower our test coverage. (As noted above, our CI suite is run automatically for
each new commit to each new pull request and will output the results on the pull
request itself.) We will explicitly grant exceptions to this rule where
appropriate. Modifying lint rules will not be granted without a well-reasoned
justification.

Once the above conditions are met, someone from the Recidiviz Team will merge
your pull request (and delete the branch if issued against our remote).

## Style

Follow the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html).

The [linting configuration](.pylintrc) should adhere to this style guide and
help you follow it automatically. You can use Pylint support for warning
suppression if you find it necessary, but this may be argued against during
code review.

### Python Quoting Style

For small, symbol-like strings we use single quotes:

```
hold = 'Federal'
tablename = 'booking_history'
```

For strings that are used for interpolation or that are natural language messages, we use double quotes:

```
logging.info("Creating new scrape session for: [%s]", scrape_key)
```
```
msg = "Finished loading background target list to docket."
```

For docstrings, we use triple quotes:

```
def test_create_scrape_task(self, mock_client):
    """Tests that a task is created."""
```
