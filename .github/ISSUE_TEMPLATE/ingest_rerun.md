---
name: Ingest Rerun
about: Request to perform an ingest rerun for a given state in a given environment.
title: '[US_XX] [Full/Partial] [Prod/Staging] Ingest Rerun to [Do Something]'
labels: 'Subject: Ingest Rerun'
assignees: ''
---
> BEFORE YOU BEGIN!! PLEASE CHECK OFF AND COMPLETE THE FOLLOWING.
- [ ] I have added the corresponding `REGION: US_XX` label associated with my rerun.
- [ ] I have added the corresponding `Team: *` or `State Pod: *` label for the group tracking progress of this rerun.

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

**If this is a partial rerun or you will have to do any non-standard database operations before starting this rerun, please write the plan you intend to do below.**

_Plan here - fill this out before starting the rerun_
