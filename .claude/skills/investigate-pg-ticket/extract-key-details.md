# Extracting Key Details from a PG Ticket

#### a. Determine the state code

Look for the state code in the ticket title (e.g. `[US_ID]`), body ("State(s)" field),
or labels (e.g. `Region: US_TX`).

If neither the title nor the body contains a recognizable state code, **ASK the
user (use AskUserQuestion tool)** with common state codes as options.

#### b. Determine the product area

Look for the product area in the ticket title (e.g. `[Workflows]`), body ("Product" field),
or labels (e.g. `Project: Workflows`, `Project: Tasks`, `Project: Insights`).

If neither the title nor the body contains a recognizable product area, **ASK
the user (use AskUserQuestion tool)** with options like "Tasks - contacts",
"Workflows", "Insights", "PSI".

#### c. Identify the task or opportunity name

If the issue is relevant to Tasks or Workflows (also referred to as
Opportunities — they're the same product area), use keywords from the ticket
(e.g., "face to face", "contact", "LSI-R", "discharge") to search the codebase
and identify the task or opportunity name:

- For **Tasks**, search in
  `recidiviz/task_eligibility/compliance_task_eligibility_spans/<state_code>/`.
- For **Workflows**, search in
  `recidiviz/task_eligibility/eligibility_spans/<state_code>/`.

If you're not confident which one is the right match, **ASK the user (use
AskUserQuestion tool)** with the candidates you found as options.
