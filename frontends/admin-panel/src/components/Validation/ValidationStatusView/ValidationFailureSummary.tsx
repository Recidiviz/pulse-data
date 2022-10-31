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

import Title from "antd/lib/typography/Title";
import { FC } from "react";
import { ValidationStatusRecord } from "../../../recidiviz/admin_panel/models/validation_pb";
import {
  ANCHOR_VALIDATION_FAILURE_SUMMARY,
  ANCHOR_VALIDATION_HARD_FAILURES,
  RecordStatus,
  ANCHOR_VALIDATION_SOFT_FAILURES,
} from "../constants";
import ValidationFailureTable from "../ValidationFailureTable";

interface ValidationFailureSummaryProps {
  categoryIds: string[];
  stateCode: string | null;
  allStates: string[];
  records: ValidationStatusRecord[];
  loading?: boolean;
}

const ValidationFailureSummary: FC<ValidationFailureSummaryProps> = ({
  categoryIds,
  stateCode,
  allStates,
  records,
  loading,
}) => {
  return (
    <>
      <Title id={ANCHOR_VALIDATION_FAILURE_SUMMARY} level={1}>
        Failure Summary
      </Title>
      <Title id={ANCHOR_VALIDATION_HARD_FAILURES} level={2}>
        Hard Failures
      </Title>
      <ValidationFailureTable
        statuses={[RecordStatus.FAIL_HARD, RecordStatus.BROKEN]}
        records={records}
        allStates={allStates}
        selectedState={stateCode}
        categoryIds={categoryIds}
        loading={loading}
      />
      <Title
        id={ANCHOR_VALIDATION_SOFT_FAILURES}
        level={2}
        style={{ marginTop: "1.2em" }}
      >
        Soft Failures
      </Title>
      <ValidationFailureTable
        statuses={[RecordStatus.FAIL_SOFT]}
        records={records}
        allStates={allStates}
        selectedState={stateCode}
        categoryIds={categoryIds}
        loading={loading}
      />
    </>
  );
};

export default ValidationFailureSummary;
