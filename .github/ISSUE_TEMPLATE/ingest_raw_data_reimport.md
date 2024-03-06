---
name: Ingest Raw Data Re-import
about: Request to re-import raw data for a given state in a given environment.
title: "[US_XX] [Prod/Staging] Ingest Raw Data Re-import to [Do Something]"
labels: "Subject: Ingest Raw Data Re-import"
assignees: ""
---

> BEFORE YOU BEGIN!! PLEASE CHECK OFF AND COMPLETE THE FOLLOWING.

- [ ] I have added the corresponding `Region: US_XX` label associated with my rerun.
- [ ] I have added the corresponding `Team: *` label for the group tracking progress of this rerun.

**Describe the set of changes that require this raw data re-import**

_Fill me in_

**Follow the instructions below**

- [ ] 1. Pause the ingest queues.
- [ ] 2. Land the PR with the changes you want to make (only relevant for code changes that impact how raw data import works, e.g. infrastructure changes)
- [ ] 3. Move ALL raw data files to the SECONDARY bucket that should be re-imported. 
    * Generally you'll use scripts `copy_raw_state_files_between_projects` and `move_raw_state_files_from_storage` but you should follow the most recent guidance in the "Start Raw Data Reimport" modal that can be reached from the SECONDARY admin panel page.
- [ ] 4. Unpause the ingest queues
- [ ] 5. Allow SECONDARY to start processing data by submitting "Start Raw Data Reimport" modal in the SECONDARY admin panel page.
- [ ] 5. Wait for raw data to finish importing and for the SECONDARY instance to show a READY_TO_FLASH status in the admin panel.
- [ ] 6. Finish the reimport by running the flashing checklist ("Flash Databases" page in the admin panel).
- [ ] 7. Adjust / remove custom validation thresholds (`us_xx_validation_config.yaml`) for any validations tagged with this task number.

6. **If you will have to do any non-standard operations before starting this raw data re-import, please write the plan you intend to do below.**

_Plan here - fill this out before starting the re-import_
