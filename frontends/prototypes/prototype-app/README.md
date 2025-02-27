# Prototypes frontend

A React application for rapid prototyping.

## Setup

`yarn install`. That's it! No other configuration required thus far.

## Development

This application uses [Vite](https://vitejs.dev/) as its build system. `yarn dev` will start a local development server.

The development application is pointed at the local development functions endpoint, so be sure you are running the dev server in `../functions/` as well.

### Observing MobX state changes in a React Component

If your React component needs to update when the value of MobX observable state changes (e.g. when `RootStore.firestoreAuthorized` changes), you will need to wrap your component with the `observer` HoC.

See <https://mobx.js.org/react-integration.html#react-integration> for more details.

## Deployment

`yarn build` will create build artifacts (static files for the SPA). After building, use `yarn preview` to test the build before deploying.

Once it has been tested to your satisfaction, use `yarn deploy` to deploy the static files to Firebase Hosting.
