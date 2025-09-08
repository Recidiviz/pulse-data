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

import { PageHeader } from "antd";

import { fetchIngestStateCodes } from "../../AdminPanelAPI";
import StateSelector from "../Utilities/StateSelector";
import { StateCodeInfo } from "./constants";

interface StateSelectorPageHeaderProps {
  title: string;
  subTitle?: string;
  stateCode: string | null;
  onChange: (stateCode: StateCodeInfo) => void;
  onBack?: (e?: React.MouseEvent<HTMLDivElement, MouseEvent>) => void;
}

const StateSelectorPageHeader: React.FC<StateSelectorPageHeaderProps> = ({
  title,
  subTitle,
  stateCode,
  onChange,
  onBack,
}) => {
  return (
    <PageHeader
      title={title}
      subTitle={subTitle}
      extra={[
        <StateSelector
          fetchStateList={fetchIngestStateCodes}
          onChange={onChange}
          initialValue={stateCode}
          key="state-selector"
        />,
      ]}
      onBack={onBack}
    />
  );
};

export default StateSelectorPageHeader;
