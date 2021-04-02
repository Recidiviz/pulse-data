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
import * as React from "react";
import { PageHeader, Select, Spin } from "antd";
import { fetchIngestRegionCodes } from "../AdminPanelAPI";
import useFetchedData from "../hooks";

const ActionsView = (): JSX.Element => {
  const { loading, data } = useFetchedData<string[]>(fetchIngestRegionCodes);
  const [regionCode, setRegionCode] = React.useState<string | undefined>(
    undefined
  );
  const title = regionCode
    ? `Key Actions for ${regionCode.toUpperCase()}`
    : "Key Actions";

  const handleChange = (value: string) => {
    setRegionCode(value);
  };

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  return (
    <>
      <PageHeader
        title={title}
        extra={[
          <Select
            style={{ width: 200 }}
            placeholder="Select a region"
            onChange={handleChange}
          >
            {data?.sort().map((code: string) => {
              return (
                <Select.Option key={code} value={code}>
                  {code.toUpperCase()}
                </Select.Option>
              );
            })}
          </Select>,
        ]}
      />
    </>
  );
};

export default ActionsView;
