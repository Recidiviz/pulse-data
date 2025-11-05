# Yarn Security Migration Guide

This repository has been configured with npm package security gates to reduce the likelihood of installing compromised packages.

## Configuration

The following `.yarnrc.yml` files have been added:

- Root: `/.yarnrc.yml`
- Admin Panel: `/frontends/admin-panel/.yarnrc.yml`
- Prototypes Functions: `/frontends/prototypes/functions/.yarnrc.yml`
- Prototypes Admin: `/frontends/prototypes/admin/.yarnrc.yml`
- Prototype App: `/frontends/prototypes/prototype-app/.yarnrc.yml`
- Asset Generation: `/nodejs/asset-generation/.yarnrc.yml`

Each configuration includes:
```yaml
npmMinimalAgeGate: 4320  # 72 hours in minutes
```

## Current Status

The projects are currently using Yarn 1.x, which does not support the `npmMinimalAgeGate` feature. To enable the security gate, projects need to be upgraded to Yarn 2+ (Berry).

## Migration to Yarn 2+

To enable the security feature in a project:

1. Navigate to the project directory
2. Run: `corepack enable`
3. Run: `corepack prepare yarn@stable --activate`
4. The `.yarnrc.yml` file is already configured and will take effect

## Verification

After migration, verify the security gate is active:
```bash
yarn config get npmMinimalAgeGate
# Should return: 4320
```

## Security Benefit

With this configuration, Yarn will reject any npm package that was published less than 72 hours ago, helping to protect against supply chain attacks where malicious packages are quickly published and distributed.