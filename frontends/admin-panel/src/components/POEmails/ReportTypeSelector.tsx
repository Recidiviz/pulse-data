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
import { Select } from "antd";
import * as React from "react";
import { fetchReportTypes } from "../../AdminPanelAPI/LineStaffTools";
import { useFetchedDataJSON } from "../../hooks";

interface ReportTypeSelectorProps {
  onChange: (reportType: string) => void;
}

const ReportTypeSelector: React.FC<ReportTypeSelectorProps> = ({
  onChange,
}) => {
  const { loading, data } = useFetchedDataJSON<string[]>(fetchReportTypes);

  return (
    <Select
      style={{ width: 200 }}
      placeholder="Select a report type"
      loading={loading}
      optionFilterProp="children"
      onChange={onChange}
      filterOption
      showSearch
    >
      {data?.map((reportType) => {
        return (
          <Select.Option key={reportType} value={reportType}>
            {reportType}
          </Select.Option>
        );
      })}
    </Select>
  );
};

export default ReportTypeSelector;
