// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================

import { ServicesClient } from "@google-cloud/run";
import { SecretManagerServiceClient } from "@google-cloud/secret-manager";
import { Storage } from "@google-cloud/storage";

import { isDevMode, isTestMode } from "../utils";

const secretmanagerClient = new SecretManagerServiceClient();
export const gcsClient = new Storage();

export async function getCloudRunUrl(
  localPort: string | number = 5174
): Promise<string> {
  if (isDevMode() || isTestMode()) {
    return `https://localhost:${localPort}`;
  }
  const runClient = new ServicesClient();
  const projectId = await runClient.getProjectId();
  const [service] = await runClient.getService({
    name: runClient.servicePath(projectId, "us-central1", "asset-generation"),
  });
  const url = service.uri;
  if (!url) {
    throw new Error("Could not read Cloud Run URL");
  }
  return url;
}

export async function getSecret(secretName: string) {
  const projectId = await secretmanagerClient.getProjectId();
  const path = secretmanagerClient.secretVersionPath(
    projectId,
    secretName,
    "latest"
  );
  const [secret] = await secretmanagerClient.accessSecretVersion({
    name: path,
  });
  const data = secret.payload?.data?.toString();
  if (!data) {
    throw new Error(`secret ${secretName} is empty`);
  }
  return data;
}
