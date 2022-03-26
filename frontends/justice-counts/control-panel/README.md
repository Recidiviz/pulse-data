# Getting Started

Welcome to the Justice Counts Control Panel - a tool that allows agencies to report Justice Counts metrics. This application was bootstrapped with [Create React App](https://github.com/facebook/create-react-app), written in [TypeScript](https://www.typescriptlang.org/docs), and authenticated via [Auth0](https://auth0.com/).

## Installation

1. Install dependencies

   ##### For all Yarn installation options, see [Yarn Installation](https://yarnpkg.com/en/docs/install).

   ```sh
   yarn install
   ```

2. Set up your Auth0 environment variables

   Expected variables:

   ```js
   REACT_APP_AUTH0_DOMAIN = ``;
   REACT_APP_AUTH0_CLIENT_ID = ``;
   REACT_APP_AUTH0_AUDIENCE = ``;
   ```

   Optional variables (currently for testing calls to public and protected API endpoints):

   ```js
   REACT_APP_SERVER_URL = ``;
   ```

3. Run the local development server

   ```sh
   yarn run dev
   ```

4. Test your development environment

   ```sh
   yarn test
   yarn lint
   ```
