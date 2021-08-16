// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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

import { StateCodeInfo } from "../IngestOperationsView/constants";

export const layout = {
  labelCol: { span: 8 },
  wrapperCol: { span: 14 },
};

export const tailLayout = {
  wrapperCol: { offset: 8, span: 14 },
};

export interface BatchInfoDict {
  sentDate: string;
  totalDelivered: string;
  redirectAddress: string;
}

export type BatchInfoType = {
  batchId: string;
  sendResults: BatchInfoDict[];
};

export interface POEmailsFormProps {
  stateInfo: StateCodeInfo;
}
