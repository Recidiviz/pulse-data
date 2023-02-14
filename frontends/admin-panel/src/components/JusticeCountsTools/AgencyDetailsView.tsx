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
import { SearchOutlined } from "@ant-design/icons";
import { Button, Input, message, PageHeader, Select, Space, Table } from "antd";
import { FilterDropdownProps } from "antd/lib/table/interface";
import { useMemo } from "react";
import { useParams } from "react-router-dom";
import {
  getAgency,
  updateAgencyUserRole,
} from "../../AdminPanelAPI/JusticeCountsTools";
import { useFetchedDataJSON } from "../../hooks";
import {
  AgencyResponse,
  AgencyTeamMember,
  AgencyTeamMemberRole,
  ErrorResponse,
} from "./constants";

const AgencyProvisioningView = (): JSX.Element => {
  const { agencyId } = useParams() as { agencyId: string };
  const getAgencyWithId = useMemo(() => getAgency(agencyId), [agencyId]);
  const { data } = useFetchedDataJSON<AgencyResponse>(getAgencyWithId);

  const onAgencyUserRoleChange = async (email: string, role: string) => {
    try {
      const response = await updateAgencyUserRole(agencyId, email, role);
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occured: ${error}`);
        return;
      }
      message.success(`Successfully changed user ${email}'s role to ${role}`);
    } catch (err) {
      message.error(`An error occured: ${err}`);
    }
  };

  const getColumnSearchProps = (dataIndex: keyof AgencyTeamMember) => ({
    filterDropdown: ({
      setSelectedKeys,
      selectedKeys,
      confirm,
      clearFilters,
    }: FilterDropdownProps) => (
      <div style={{ padding: 8 }}>
        <Input
          placeholder={`Search ${dataIndex}`}
          value={selectedKeys[0]}
          onChange={(e) =>
            setSelectedKeys(e.target.value ? [e.target.value] : [])
          }
          onPressEnter={() => confirm()}
          style={{ marginBottom: 8, display: "block" }}
        />
        <Space>
          <Button
            type="primary"
            onClick={() => confirm()}
            icon={<SearchOutlined />}
            size="small"
            style={{ width: 90 }}
          >
            Search
          </Button>
          <Button onClick={clearFilters} size="small" style={{ width: 90 }}>
            Reset
          </Button>
        </Space>
      </div>
    ),
    filterIcon: (filtered: boolean) => (
      <SearchOutlined style={{ color: filtered ? "#1890ff" : undefined }} />
    ),
    onFilter: (value: string | number | boolean, record: AgencyTeamMember) => {
      const result = record[dataIndex];
      return result
        ? result
            .toString()
            .toLowerCase()
            .includes(value.toString().toLowerCase())
        : false;
    },
  });

  const columns = [
    {
      title: "Auth0 User ID",
      dataIndex: "auth0_user_id",
      key: "auth0_user_id",
      ...getColumnSearchProps("auth0_user_id"),
    },
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      ...getColumnSearchProps("name"),
    },
    {
      title: "Email",
      dataIndex: "email",
      key: "email",
      ...getColumnSearchProps("email"),
    },
    {
      title: "Invitation Status",
      dataIndex: "invitation_status",
      key: "invitation_status",
      ...getColumnSearchProps("invitation_status"),
    },
    {
      title: "Role",
      dataIndex: "role",
      key: "role",
      render: (role: AgencyTeamMemberRole, user: AgencyTeamMember) => {
        return (
          <Select
            defaultValue={role}
            onChange={(selectedRole: string) => {
              onAgencyUserRoleChange(user.email, selectedRole);
            }}
            style={{ minWidth: 250 }}
          >
            {Object.values(AgencyTeamMemberRole).map((teamMemberRole) => (
              <Select.Option key={teamMemberRole} value={teamMemberRole}>
                {teamMemberRole}
              </Select.Option>
            ))}
          </Select>
        );
      },
      ...getColumnSearchProps("role"),
    },
  ];

  return (
    <>
      <PageHeader title="Agency Team Members" />
      <Table
        columns={columns}
        dataSource={data?.agency ? data?.agency.team : []}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          size: "small",
        }}
        rowKey={(agency) => agency.auth0_user_id}
      />
    </>
  );
};

export default AgencyProvisioningView;
