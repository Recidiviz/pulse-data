const path = require("path");

module.exports = {
  devServer: (devServerConfig, { paths }) => {
    return {
      ...devServerConfig,
      watchOptions: {
        ...devServerConfig.watchOptions,

        // Ignore all node_modules except the `@recidiviz/design-system` library
        ignored: `^(?!${escape(
          path.normalize(`${paths.appPath}/`).replace(/[\\]+/g, "/")
        )}).+/node_modules/(?!@recidiviz/design-system)`,
      },
    };
  },
};
