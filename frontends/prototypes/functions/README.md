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
