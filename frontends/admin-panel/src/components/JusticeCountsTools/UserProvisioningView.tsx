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
import {
  Button,
  Form,
  Input,
  message,
  PageHeader,
  Select,
  Space,
  Table,
} from "antd";
import { FilterDropdownProps } from "antd/lib/table/interface";
import { useState } from "react";
import { getAgencies, getUsers } from "../../AdminPanelAPI";
import { updateUser } from "../../AdminPanelAPI/JusticeCountsTools";
import { useFetchedDataJSON } from "../../hooks";
import {
  AgenciesResponse,
  Agency,
  ErrorResponse,
  User,
  UsersResponse,
} from "./constants";

const UserProvisioningView = (): JSX.Element => {
  const [showSpinner, setShowSpinner] = useState(false);
  const { data: usersData, setData: setUsersData } =
    useFetchedDataJSON<UsersResponse>(getUsers);
  const { data: agenciesData } =
    useFetchedDataJSON<AgenciesResponse>(getAgencies);
  const [form] = Form.useForm();

  const onAgencyChange = async (user: User, agencyIds: number[]) => {
    try {
      const response = await updateUser(user, null, agencyIds);
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occured: ${error}`);
        return;
      }
      message.success(
        `${user.auth0_email} moved to ${agenciesData?.agencies
          .filter((agency) => agencyIds.includes(agency.id))
          ?.map((agency) => agency.name)}!`
      );
    } catch (err) {
      message.error(`An error occured: ${err}`);
    }
  };

  const onNameChange = async (user: User, name: string) => {
    try {
      const response = await updateUser(user, name, null);
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occured: ${error}`);
        return;
      }
      message.success(`${user.auth0_email}'s name changed to ${name}!`);
    } catch (err) {
      message.error(`An error occured: ${err}`);
    }
  };

  const getColumnSearchProps = (dataIndex: keyof User) => ({
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
    onFilter: (value: string | number | boolean, record: User) => {
      const cell = record[dataIndex];
      return cell
        ? cell.toString().toLowerCase().includes(value.toString().toLowerCase())
        : false;
    },
  });

  const columns = [
    {
      title: "Auth0 ID",
      dataIndex: "auth0_user_id",
      key: "auth0_user_id",
      ...getColumnSearchProps("auth0_user_id"),
      width: "10%",
      ellipsis: true,
    },
    {
      title: "Email",
      dataIndex: "auth0_email",
      key: "auth0_email",
      ...getColumnSearchProps("auth0_email"),
      width: "15%",
      ellipsis: true,
    },
    {
      title: "Auth0 Name",
      dataIndex: "auth0_name",
      key: "auth0_name",
      ...getColumnSearchProps("auth0_name"),
      width: "15%",
      ellipsis: true,
    },
    {
      title: "DB Name",
      dataIndex: "db_name",
      key: "db_name",
      ...getColumnSearchProps("db_name"),
      width: "15%",
      render: (_: string, user: User) => {
        return (
          <Input
            defaultValue={user.db_name}
            onBlur={(e) => onNameChange(user, e.target.value)}
          />
        );
      },
    },
    {
      title: "Agencies",
      dataIndex: "agencies",
      key: "agency",
      render: (agencies: Agency[], user: User) => {
        const currentAgencies = agencies;
        return (
          <Select
            mode="multiple"
            allowClear
            defaultValue={currentAgencies.map((agency) => agency.id)}
            showSearch
            optionFilterProp="children"
            disabled={showSpinner}
            filterOption={(input, option) =>
              (option?.children as unknown as string)
                .toLowerCase()
                .indexOf(input.toLowerCase()) >= 0
            }
            onChange={(agencyIds: number[]) => onAgencyChange(user, agencyIds)}
            style={{ minWidth: 250 }}
          >
            {/* #TODO(#12091): Replace with debounced search bar */}
            {agenciesData?.agencies.map((agency) => (
              <Select.Option key={agency.id} value={agency.id}>
                {agency.name}
              </Select.Option>
            ))}
          </Select>
        );
      },
    },
  ];

  return (
    <>
      <PageHeader title="User Provisioning" />
      <Table
        columns={columns}
        dataSource={usersData?.users}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          size: "small",
        }}
        rowKey={(user) => user.auth0_user_id}
      />
    </>
  );
};

export default UserProvisioningView;
