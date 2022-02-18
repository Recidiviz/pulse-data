# Prototypes Admin

## Environment

Copy the value of the "service_account" entry in `../functions/.runtimeconfig.json` into a file in this directory called `service_account.json` (it will be untracked).

## ETL Scripts

### `yarn users`, `yarn users:prod`

These scripts will take a CSV downloaded from the user mapping spreadsheet and ETL it into Firestore collections for known users and known officers. The bare command will load the data to your local emulator so be sure it is running.

Has a fixture file in `fixtures/users.csv` that can be used for local testing.

### `yarn cr`, `yarn cr:prod`

These scripts will take a JSON file of compliant reporting case data exported from BigQuery and ETL it into a Firestore collection. The bare command will load the data to your local emulator so be sure it is running. (JSON can accommodate the schema better than CSV because it contains arrays.)

Has a fixture file in `fixtures/cr.json` that can be used for local testing.
