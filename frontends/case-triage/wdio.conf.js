exports.config = {
  runner: "local",
  automationProtocol: "devtools",
  specs: ["e2e/features/**.feature"],
  maxInstances: 1,
  capabilities: [
    {
      maxInstances: 1,
      browserName: "chrome",
      "goog:chromeOptions":
        process.env.RUN_TESTS_HEADLESS === "true"
          ? {
              args: [
                "--headless",
                "--no-sandbox",
                "--window-size=1280,800",
                "--disable-gpu",
              ],
            }
          : {},
      acceptInsecureCerts: true,
    },
  ],
  logLevel: "warn",
  bail: 0,
  baseUrl: "http://localhost:3000",
  waitforTimeout: 10000,
  connectionRetryTimeout: 120000,
  connectionRetryCount: 3,
  framework: "cucumber",
  reporters: ["spec"],
  cucumberOpts: {
    require: ["e2e/steps/*.ts"],
    backtrace: false,
    dryRun: false,
    failFast: false,
    format: ["pretty"],
    snippets: true,
    source: true,
    strict: false,
    tagExpression: "",
    timeout: 60000,
    ignoreUndefinedDefinitions: false,
  },
  autoCompileOpts: {
    autoCompile: true,
    tsNodeOpts: {
      transpileOnly: true,
      project: "e2e/tsconfig.json",
    },
  },
  // ---------------------
  // Below this line: arbitrary custom configuration used in tests,
  // not actual WDIO settings
  // ---------------------
  credentials: {
    admin: {
      username: process.env.TEST_AUTH_USER,
      password: process.env.TEST_AUTH_PASSWORD,
    },
  },
};
