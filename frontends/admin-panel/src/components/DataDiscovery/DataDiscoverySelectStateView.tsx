/*
Recidiviz - a data platform for criminal justice reform
Copyright (C) 2021 Recidiviz, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
=============================================================================
*/
import { Form, Select } from "antd";
import * as React from "react";
import useFetchedData from "../../hooks";
import { fetchIngestRegionCodes } from "../../AdminPanelAPI";

const DataDiscoverySelectStateView = (): JSX.Element => {
  const { loading, data: regionCodes } = useFetchedData<string[]>(
    fetchIngestRegionCodes
  );

  return (
    <Form.Item label="State" name="region_code" rules={[{ required: true }]}>
      <Select>
        {!loading && regionCodes
          ? regionCodes.map((code) => (
              <Select.Option key={code} value={code}>
                {code}
              </Select.Option>
            ))
          : null}
      </Select>
    </Form.Item>
  );
};

export default DataDiscoverySelectStateView;
