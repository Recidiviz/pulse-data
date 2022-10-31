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

import { Alert } from "antd";
import Title from "antd/lib/typography/Title";
import { FC } from "react";
import { ValidationStatusRecord } from "../../../recidiviz/admin_panel/models/validation_pb";
import { ANCHOR_VALIDATION_FULL_RESULTS } from "../constants";
import { readableNameForCategoryId } from "../utils";
import ValidationCategoryFullResultsTable from "../ValidationCategoryFullResultsTable";

interface ValidationFullResultsProps {
  categoryIds: string[];
  stateCode: string | null;
  allStates: string[];
  dictOfCategoryIdsToRecords: {
    [category: string]: MetadataRecord<ValidationStatusRecord>[];
  };
  loading?: boolean;
}

const ValidationFullResults: FC<ValidationFullResultsProps> = ({
  categoryIds,
  stateCode,
  allStates,
  dictOfCategoryIdsToRecords,
  loading,
}) => {
  return (
    <>
      <Title id={ANCHOR_VALIDATION_FULL_RESULTS} level={1}>
        Full Results
      </Title>
      {stateCode ? (
        categoryIds.sort().map((categoryId) => {
          return (
            <>
              <Title id={categoryId} level={2} style={{ marginTop: "1.2em" }}>
                {readableNameForCategoryId(categoryId)}
              </Title>
              <ValidationCategoryFullResultsTable
                selectedStates={stateCode ? [stateCode] : allStates}
                categoryId={categoryId}
                dictOfCategoryIdsToRecords={dictOfCategoryIdsToRecords}
                loading={loading}
              />
            </>
          );
        })
      ) : (
        <Alert
          message="Select a region to view region-specific validation category details"
          type="warning"
          showIcon
        />
      )}
    </>
  );
};

export default ValidationFullResults;
