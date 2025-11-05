# Asset generation

A Node.JS application for data-driven asset generation with server-side React.

## Development

### Prerequisites

Run `yarn` for package dependencies.

#### Yarn Security Configuration

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

Running the server or tests locally requires the "Public Sans" font, which can be installed by opening the
[font file](./src/fonts/PublicSans-Medium.ttf) locally and clicking Install.

### Commands

See `package.json`, but some highlights:

- `yarn dev`: run the local dev server
- `yarn storybook`: inspect the available components in Storybook.
- `yarn test`: run tests! Includes visual snapshot tests of the generated images.

### Docker

The service is built for production using the Dockerfile.asset-generation dockerfile in the pulse-data root.
To build + run it locally, run (from pulse-data/):

- `docker build . -f Dockerfile.asset-generation -t asset-gen`
- `docker run -it -p 5174:5174 -v $(pwd)/nodejs/asset-generation:/app/ -v /app/node_modules --init asset-gen`
