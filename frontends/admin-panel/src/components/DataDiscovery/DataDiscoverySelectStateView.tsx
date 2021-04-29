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
import { Form, FormInstance } from "antd";
import * as React from "react";
import IngestStateSelector from "../IngestStateSelector";

interface DataDiscoverySelectStateViewProps {
  form: FormInstance;
}

const DataDiscoverySelectStateView: React.FC<DataDiscoverySelectStateViewProps> = ({
  form,
}) => {
  const handleStateCodeChange = (value: string) => {
    form.setFieldsValue({ region_code: value });
  };

  return (
    <Form.Item label="State" name="region_code" rules={[{ required: true }]}>
      <IngestStateSelector handleStateCodeChange={handleStateCodeChange} />
    </Form.Item>
  );
};

export default DataDiscoverySelectStateView;
