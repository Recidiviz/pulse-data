---
name: Ingest Rerun
about: Request to perform an ingest rerun for a given state in a given environment.
title: "[US_XX] [Full/Partial] [Prod/Staging] Ingest Rerun to [Do Something]"
labels: "Subject: Ingest Rerun"
assignees: ""
---

> BEFORE YOU BEGIN!! PLEASE CHECK OFF AND COMPLETE THE FOLLOWING.

- [ ] I have added the corresponding `Region: US_XX` label associated with my rerun.
- [ ] I have added the corresponding `Team: *` label for the group tracking progress of this rerun.

**Describe the set of changes that require this rerun**

_Fill me in_

**What set of PR(s) are involved in the rerun?**

- [#XXXX]
- _Add more PRs if needed_

**If this is a full rerun, follow the instructions below**

1. Start a rerun for region in secondary ([docs](https://paper.dropbox.com/doc/Performing-an-ingest-rerun--BZ3wI11J6QfFp1dEY1bSReq9Ag-ZO7eFf4c57uZuWWRjdZCx#:uid=798830243539082117498680&h2=How-to-start-a-rerun)).
2. Confirm that an ingest run starts, then wait for the run to complete ([docs](https://paper.dropbox.com/doc/Monitoring-Ingest-Progress--BZ0sQJ4yhbsxogF0xKIcbeAAAg-rK2JK9aP9BKnbzJVdQHif))
3. Land and deploy a PR that releases / gates the changes to the appropriate PRIMARY instance. For instance, if in staging, ungate the logic to staging only and deploy an alpha deploy. If in prod, ungate the logic for every instance and deploy a cherrypick. This step is optional if you are inclined to follow the regular deploy process with the gated changes.
4. Finish the rerun by flashing the data to the PRIMARY instance ([docs](https://paper.dropbox.com/doc/Performing-an-ingest-rerun--BZ3wI11J6QfFp1dEY1bSReq9Ag-ZO7eFf4c57uZuWWRjdZCx#:h2=Completing-a-rerun))
5. Adjust / remove custom validation thresholds (`us_xx_validation_config.yaml`) for any validations tagged with this task number.

**If this is a partial rerun, fill out the checklist and follow the instructions below. For a more detailed understanding of how to proceed through the checklist, check out this [doc.](https://paper.dropbox.com/doc/Partial-Rerun-Checklist--B1G79UKS41jO~koOeWHwDCJOAg-cRn4IQn09zqihknysUAmH)**

Q1. Which ingest view(s) do you need this rerun for:

Q2. Will you need to delete anything from the state tables?

- [ ] Yes
- [ ] No

Q3. Will you need to pause the BQ refresh?

- [ ] Yes
- [ ] No

Steps:

- [ ] 1. Pause the ingest queues
- [ ] 2. If you answered Yes to Q3, pause the BQ refresh

_Staging config lock:_
  
  Download file: 
  `gsutil cp gs://recidiviz-staging-configs/cloud_sql_to_bq_config.yaml .`
  
  Upload file:
  `gsutil cp cloud_sql_to_bq_config.yaml gs://recidiviz-staging-configs/cloud_sql_to_bq_config.yaml`

_Production config lock:_

  Download file: 
  `gsutil cp gs://recidiviz-123-configs/cloud_sql_to_bq_config.yaml .`

  Upload file:
  `gsutil cp cloud_sql_to_bq_config.yaml gs://recidiviz-123-configs/cloud_sql_to_bq_config.yaml`
  
- [ ] 3. Land the PR with the changes you want to make
- [ ] 4. Delete the ingest view results for the view(s) you want to rerun

_Insert all commands you will run to delete the results here_

```

```

- [ ] 5. Invalidate the ingest view metadata rows in the operations database

_Insert all commands you will run to invalidate the results here_

```

```

- [ ] 6. If you answered Yes to Q2, clear out the state tables for the relevant entities

_Insert all commands you will run to clear out state tables for relevant entities here_

```

```

- [ ] 7. Unpause the ingest queues and trigger the task scheduler
- [ ] 8. Wait for the partial rerun to complete
- [ ] 9. If you answered Yes to Q3, unpause the BQ refresh
**If you will have to do any non-standard database operations before starting this rerun, please write the plan you intend to do below.**

_Plan here - fill this out before starting the rerun_
