# Prototypes Admin

## Yarn Security Configuration

This project includes a security configuration that requires npm packages to be at least 72 hours old before installation (to help prevent supply chain attacks). To enable this security feature:

```bash
corepack enable
corepack prepare yarn@stable --activate
```

You can verify the security gate is active with:
```bash
yarn config get npmMinimalAgeGate  # Should return: 4320
```

See `YARN_SECURITY_MIGRATION.md` in the repository root for more details.

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