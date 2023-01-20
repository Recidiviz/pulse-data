# Prototypes Admin

## Environment

Copy the value of the "service_account" entry in `../functions/.runtimeconfig.json` into a file in this directory called `service_account.json` (it will be untracked).

## ETL Scripts

None available at the moment.

To run a script against the local Firestore emulator, you **MUST** set the `FIRESTORE_EMULATOR_HOST`
environment variable (the correct value is probably `localhost:8080` but this is managed by the
`functions` package, which starts an emulator alongside its development server).

To run a script against the live Firestore deployment, omit `FIRESTORE_EMULATOR_HOST` from the environment.

In both cases you will also need `GOOGLE_APPLICATION_CREDENTIALS=service_account.json` to be set
in the script's environment.