export const gcpEnvironment = {
  isProduction: window.RUNTIME_GCP_ENVIRONMENT === "production",
  isStaging: window.RUNTIME_GCP_ENVIRONMENT === "staging",
};
