// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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

import { message } from "antd";
import assert from "assert";
import {
  getCurrentIngestInstanceStatus,
  getRawDataSourceInstance,
} from "../../AdminPanelAPI";
import { isIngestInDataflowEnabled } from "../../AdminPanelAPI/IngestOperations";
import { DirectIngestInstance } from "../IngestOperationsView/constants";

export const fetchCurrentIngestInstanceStatus = async (
  stateCode: string,
  instance: DirectIngestInstance
): Promise<string> => {
  const response = await getCurrentIngestInstanceStatus(stateCode, instance);
  return response.text();
};

export const fetchCurrentRawDataSourceInstance = async (
  stateCode: string,
  instance: DirectIngestInstance
): Promise<DirectIngestInstance | null> => {
  try {
    const response = await getRawDataSourceInstance(stateCode, instance);
    const json = await response.json();
    if (response.status === 500) {
      message.error(
        `An error occurred when attempting to retrieve raw data source instance of secondary rerun: ${json}`,
        8
      );
      return null;
    }
    return json.instance;
  } catch (err) {
    message.error(
      `An error occurred when attempting to retrieve raw data source instance of secondary rerun: ${err}`,
      8
    );
    return null;
  }
};

export const fetchDataflowEnabled = async (
  stateCode: string
): Promise<boolean> => {
  const primaryResponse = await isIngestInDataflowEnabled(
    stateCode,
    DirectIngestInstance.PRIMARY
  );
  const isEnabledPrimary = await primaryResponse.json();
  const secondaryResponse = await isIngestInDataflowEnabled(
    stateCode,
    DirectIngestInstance.SECONDARY
  );
  const isEnabledSecondary = await secondaryResponse.json();

  assert(isEnabledPrimary === true || isEnabledPrimary === false);
  assert(isEnabledSecondary === true || isEnabledSecondary === false);
  return isEnabledPrimary && isEnabledSecondary;
};
