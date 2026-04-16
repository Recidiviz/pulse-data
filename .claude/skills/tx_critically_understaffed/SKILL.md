# Skill: TX Critically Understaffed Locations Upload

## Overview

This skill transforms a TX critically understaffed locations Excel file into a
formatted CSV and uploads it to the TX ingest GCS buckets (staging and prod).

## Instructions

### Step 1: Get the xlsx file path

The user should provide the path to the xlsx file. If they haven't, ask them:

"Please provide the path to the Critically Understaffed xlsx file."

Expected filename format: `Critically Understaffed - <Month> <Year>.xlsx`
(e.g., `Critically Understaffed - March 2026 (1).xlsx`)

### Step 2: Ask for the upload date

**Always ask the user** for the upload date to use in the GCS filename. Do NOT
assume it is today's date.

"What date should be used for the GCS upload filename? (YYYY-MM-DD format)"

### Step 3: Do a dry run first

Run with `--dry_run` to preview the output before uploading:

```bash
uv run python -m recidiviz.tools.ingest.operations.state_data_manual_uploads.us_tx_transform_and_upload_understaffed --xlsx_path "<xlsx_path>" --upload_date "<upload_date>" --dry_run
```

The dry run prints the full transformed table and shows what GCS commands would
be run, without uploading anything.

**Verify the output looks correct:**
- The row count matches the number of individual DPO offices in the xlsx (should
  be ~130–160 rows; excludes region header rows like "REGION I" and totals rows
  like "Region I Totals")
- `OFFC_REGION` and `OFFC_DISTRICT` are zero-padded 2-digit numbers (e.g., `01`,
  `12`)
- `PERCENTAGE_NON_CASE_CARRYING` values are formatted as percentages (e.g.,
  `42.86%`)
- `MONTH` and `YEAR` match the month/year in the filename
- No rows are missing `OFFC_REGION` or `OFFC_DISTRICT` values

If there are unmatched DPO names, the script raises a `ValueError` listing them.
You'll need to either:
1. Add the new office to `OfficeDescriptions.csv`
2. Update the normalization logic in `transform_and_upload_tx_understaffed.py`

### Step 4: Run the actual upload

Once the dry run looks good, run without `--dry_run`:

```bash
uv run python -m recidiviz.tools.ingest.operations.state_data_manual_uploads.us_tx_transform_and_upload_understaffed --xlsx_path "<xlsx_path>" --upload_date "<upload_date>"
```

The script uploads to both:
- `gs://recidiviz-staging-direct-ingest-state-us-tx/unprocessed_{upload_date}T00:00:00:000000_raw_manual_upload_critically_understaffed_locations.csv`
- `gs://recidiviz-123-direct-ingest-state-us-tx/unprocessed_{upload_date}T00:00:00:000000_raw_manual_upload_critically_understaffed_locations.csv`

Confirm both uploads print `✓ Uploaded to gs://...` before finishing.

## Files

- **Script**: `recidiviz/tools/ingest/operations/state_data_manual_uploads/us_tx_transform_and_upload_understaffed.py`
- **Office lookup**: `recidiviz/tools/ingest/operations/state_data_manual_uploads/OfficeDescriptions.csv`
- **GCS file tag**: `manual_upload_critically_understaffed_locations`
