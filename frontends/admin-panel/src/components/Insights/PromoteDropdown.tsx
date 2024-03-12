// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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
import { DownOutlined } from "@ant-design/icons";
import { Dropdown, MenuProps, Popconfirm, Space, Tooltip } from "antd";
import { observer } from "mobx-react-lite";
import styled from "styled-components/macro";

import ConfigurationPresenter from "../../InsightsStore/presenters/ConfigurationPresenter";

const PromoteButtonContainer = styled.div`
  border: 1px solid #d3d3d3;
  background-color: white;
  border-radius: 2px;
  padding: 3px;

  &:hover {
    cursor: pointer;
    color: #72c6ed;
    border: 1px solid #72c6ed;
  }

  .anticon-down {
    width: 12px;
    color: #d3d3d3;
  }
`;

const PromoteDropdown = ({
  presenter,
  selectedConfigId,
}: {
  presenter: ConfigurationPresenter;
  selectedConfigId?: number;
}): JSX.Element => {
  const {
    promoteSelectedVersionToProduction,
    promoteSelectedVersionToDefault,
    envIsStaging,
  } = presenter;

  const items: MenuProps["items"] = [
    {
      label: (
        <Popconfirm
          title="Are you sure you want to promote this config to default?"
          onConfirm={() => promoteSelectedVersionToDefault(selectedConfigId)}
          disabled={!selectedConfigId}
        >
          Promote to default
        </Popconfirm>
      ),
      key: "default",
      disabled: !selectedConfigId,
    },
    {
      label: (
        <Popconfirm
          title="Are you sure you want to promote this config to production?"
          onConfirm={() => promoteSelectedVersionToProduction(selectedConfigId)}
          disabled={!selectedConfigId || !envIsStaging}
        >
          Promote to production
        </Popconfirm>
      ),
      key: "production",
      disabled: !selectedConfigId || !envIsStaging,
    },
  ];

  return (
    <PromoteButtonContainer>
      <Dropdown trigger={["click"]} menu={{ items }}>
        <Tooltip
          title="Select a configuration to promote to default, or promote to production (from staging only)"
          key="leftButton"
          placement="top"
        >
          <Space>
            Promote
            <DownOutlined />
          </Space>
        </Tooltip>
      </Dropdown>
    </PromoteButtonContainer>
  );
};

export default observer(PromoteDropdown);
