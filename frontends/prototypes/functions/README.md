## Run locally

You'll need access to the Firebase project at https://console.firebase.google.com/project/recidiviz-prototypes.

### Sign into Firebase

Make sure you have the latest version of Firebase Tools installed! This is a global dependency but versions below 10.x will not work for local development.

```
yarn global add firebase-tools
# OR npm install -g firebase-tools
```

Then:

```
firebase login
```

### Start the functions

Install `nvm` if you don't have it already: https://github.com/nvm-sh/nvm#installing-and-updating

Run `nvm use`. If you get an error, you may need to install the configured node version using the `nvm` command in the
error.

The Firebase emulators will also require a Java runtime, which you can install via Homebrew (`brew install java`).

```
yarn install
firebase functions:config:get > .runtimeconfig.json
yarn serve
```

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

#### If you are using a macOS with the latest version for Monterey, take care to disable `Airplay Receiver` in `Sharing` settings in `System Preferences` from being on port `5000`.

## Linting

```
yarn lint
```

## Set config

See https://firebase.google.com/docs/functions/config-env.

Example: `firebase functions:config:set auth0.domain='model-login.recidiviz.org'`

## Deploy the functions

```
yarn deploy
```
