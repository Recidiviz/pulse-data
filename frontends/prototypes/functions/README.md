## Run locally

You'll need access to the Firebase project at https://console.firebase.google.com/project/recidiviz-prototypes.

### Sign into Firebase

```
yarn global add firebase-tools
firebase login
```

### Start the functions

Install `nvm` if you don't have it already: https://github.com/nvm-sh/nvm#installing-and-updating

Run `nvm use`. If you get an error, you may need to install the configured node version using the `nvm` command in the
error.

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
