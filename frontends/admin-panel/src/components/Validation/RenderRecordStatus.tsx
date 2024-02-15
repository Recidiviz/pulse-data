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

import classNames from "classnames";
import { FC } from "react";

import { ValidationStatusRecord } from "../../recidiviz/admin_panel/models/validation_pb";
import { RecordStatus } from "./constants";
import {
  formatStatusAmount,
  getClassNameForRecordStatus,
  getRecordStatus,
  getTextForRecordStatus,
} from "./utils";

interface RenderRecordStatusProps {
  record: ValidationStatusRecord | undefined;
}

const RenderRecordStatus: FC<RenderRecordStatusProps> = ({ record }) => {
  const status = getRecordStatus(record);
  const statusClassName = getClassNameForRecordStatus(status);
  const text = getTextForRecordStatus(status);
  // If we don't have data so the validation didn't run, we don't need to add the dev
  // mode treatment.
  const devMode = record?.getDevMode() && status > RecordStatus.NEED_DATA;
  const body =
    text +
    (status > RecordStatus.NEED_DATA
      ? ` (${formatStatusAmount(
          record?.getErrorAmount(),
          record?.getIsPercentage()
        )})`
      : "");
  return (
    <div className={classNames(statusClassName, { "dev-mode": devMode })}>
      {body}
      {devMode && (
        <div>
          <em>Dev Mode</em>
        </div>
      )}
    </div>
  );
};

export default RenderRecordStatus;
