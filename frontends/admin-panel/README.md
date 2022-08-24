# Set up your environment

Install `yarn`:

```bash
brew install yarn
brew install node
```

# Running the app

### Spin up the backend

1. Navigate to the root of the repo.
2. Make sure you have launched the `Docker` application.
3. If you haven't yet, run `gcloud auth configure-docker`.
4. Run `docker pull us.gcr.io/recidiviz-staging/appengine/default:latest` to pull the
latest Docker image for `main` if you have not done so recently.
5. Run `./recidiviz/tools/admin_panel/initialize_development_environment.sh` (if you 
have not done so recently or the script has changed). 
6. Run `docker-compose -f docker-compose.yaml -f docker-compose.admin-panel.yaml up`.

As needed:
6. Follow the instructions in `recidiviz/tools/admin_panel/load_operations_db_fixtures.py`
   to load fixture data into the local operations DB.
7. Follow the instructions in `recidiviz/tools/case_triage/load_fixtures.py`
   to load fixture data into the local case triage DB.
8. Follow the instructions in `recidiviz/tools/justice_counts/control_panel/load_fixtures.py`
   to load fixture data into the local justice counts DB.

If your local development environment is failing to build properly on main, try
running `docker build . -t us.gcr.io/recidiviz-staging/appengine/default:latest`
to ensure that you have the latest dependencies in your docker image. If it’s still not
working, ping `#eng`.

### Spin up the frontend

1. Navigate to this directory (`frontends/admin-panel`).
2. Run `yarn` to install all dependencies.
3. Run `yarn dev`.

The admin panel will now be accessible at `http://localhost:3030`.

# Frontend Info

This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

## Available Scripts

In the project directory, you can run:

### `yarn dev`

Runs the app in the development mode.<br>
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.<br>
You will also see any lint errors in the console.

### `yarn test`

Launches the test runner in the interactive watch mode.<br>
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

### `yarn run build`

Builds the app for production to the `build` folder.<br>
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.<br>
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

### `yarn run eject`

**Note: this is a one-way operation. Once you `eject`, you can’t go back!**

If you aren’t satisfied with the build tool and configuration choices, you can `eject` at any time. This command will remove the single build dependency from your project.

Instead, it will copy all the configuration files and the transitive dependencies (Webpack, Babel, ESLint, etc) right into your project so you have full control over them. All of the commands except `eject` will still work, but they will point to the copied scripts so you can tweak them. At this point you’re on your own.

You don’t have to ever use `eject`. The curated feature set is suitable for small and middle deployments, and you shouldn’t feel obligated to use this feature. However we understand that this tool wouldn’t be useful if you couldn’t customize it when you are ready for it.

## Learn More

You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).
