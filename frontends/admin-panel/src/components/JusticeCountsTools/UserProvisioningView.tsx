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
  Spin,
  Table,
  Typography,
} from "antd";
import { FilterDropdownProps } from "antd/lib/table/interface";
import { useState } from "react";

import { getAgencies, getUsers } from "../../AdminPanelAPI";
import {
  createUser,
  deleteUsersAccounts,
  updateUser,
  updateUsersAccounts,
} from "../../AdminPanelAPI/JusticeCountsTools";
import { useFetchedDataJSON } from "../../hooks";
import { formLayout, formTailLayout } from "../constants";
import {
  AgenciesResponse,
  Agency,
  CreateUserRequest,
  ErrorResponse,
  User,
  UserResponse,
  UsersResponse,
} from "./constants";

const UserProvisioningView = (): JSX.Element => {
  const [showSpinner, setShowSpinner] = useState(false);
  const [form] = Form.useForm();
  const { data: usersData, setData: setUsersData } =
    useFetchedDataJSON<UsersResponse>(getUsers);
  const { data: agenciesData } =
    useFetchedDataJSON<AgenciesResponse>(getAgencies);

  const onFinish = async ({ email, name }: CreateUserRequest) => {
    const emailTrimmed = email.trim();
    const nameTrimmed = name.trim();
    setShowSpinner(true);
    try {
      const response = await createUser(emailTrimmed, nameTrimmed);
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        throw error;
      }
      const { user } = (await response.json()) as UserResponse;
      setUsersData({
        users: [...(usersData?.users || []), user],
      });
      form.resetFields();
      setShowSpinner(false);
      message.success(`"${name}" added!`);
    } catch (err) {
      setShowSpinner(false);
      message.error(`An error occured: ${err}`);
    }
  };

  const onAgencyChange = async (
    updatedUser: User,
    newAgencyIds: number[],
    currentAgencyIds: number[]
  ) => {
    try {
      const response = await Promise.resolve(
        newAgencyIds.length > currentAgencyIds.length
          ? updateUsersAccounts(
              /** user */ updatedUser,
              /** agency_ids */ newAgencyIds.filter(
                (x) => !currentAgencyIds.includes(x)
              )
            )
          : deleteUsersAccounts(
              /** user */ updatedUser,
              /** agency_ids */ currentAgencyIds.filter(
                (x) => !newAgencyIds.includes(x)
              )
            )
      );

      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occurred: ${error}`);
        return;
      }
      const { user } = (await response.json()) as UserResponse;
      const newUsersData = {
        users:
          usersData?.users.map((currUser) =>
            currUser.auth0_user_id === user.auth0_user_id ? user : currUser
          ) || [],
      };
      setUsersData(newUsersData);
      message.success(
        `${user.email} moved to ${user?.agencies?.map(
          (agency) => agency.name
        )}!`
      );
    } catch (err) {
      message.error(`An error occurred: ${err}`);
    }
  };

  const onNameChange = async (user: User, name: string) => {
    try {
      const response = await updateUser(user.auth0_user_id, name);
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        message.error(`An error occured: ${error}`);
        return;
      }
      message.success(`${user.email}'s name changed to ${name}!`);
    } catch (err) {
      message.error(`An error occured: ${err}`);
    }
  };

  const getColumnSearchProps = (dataIndex: keyof User) => ({
    // eslint-disable-next-line react/no-unstable-nested-components
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
    // eslint-disable-next-line react/no-unstable-nested-components
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
      dataIndex: "email",
      key: "email",
      ...getColumnSearchProps("email"),
      width: "15%",
      ellipsis: true,
    },
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      ...getColumnSearchProps("name"),
      width: "15%",
      render: (_: string, user: User) => {
        return (
          <Input
            defaultValue={user.name}
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
        const currentAgencyIds = currentAgencies.map((agency) => agency.id);
        return (
          <Select
            mode="multiple"
            allowClear
            defaultValue={currentAgencyIds}
            showSearch
            optionFilterProp="children"
            disabled={showSpinner}
            onChange={(newAgencyIds: number[]) =>
              onAgencyChange(user, newAgencyIds, currentAgencyIds)
            }
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
        rowKey={(user) => JSON.stringify(user.auth0_user_id + user.agencies)}
        // If the user is added to a Superagency, the table will re-render so that we can
        // can see the cells of the child agencies with the name of the user we added as well.
      />
      <Form
        {...formLayout}
        form={form}
        onFinish={onFinish}
        requiredMark={false}
      >
        <Typography.Title
          level={4}
          style={{ paddingTop: 16, paddingBottom: 8 }}
        >
          Add User
        </Typography.Title>
        <Typography.Paragraph style={{ marginBottom: 16 }}>
          This will create a new user in both Auth0 and our database.
        </Typography.Paragraph>
        <Form.Item label="Name" name="name" rules={[{ required: true }]}>
          <Input disabled={showSpinner} />
        </Form.Item>
        <Form.Item label="Email" name="email" rules={[{ required: true }]}>
          <Input disabled={showSpinner} />
        </Form.Item>
        <Form.Item {...formTailLayout}>
          <Button type="primary" htmlType="submit" disabled={showSpinner}>
            Submit
          </Button>
          {showSpinner && <Spin style={{ marginLeft: 16 }} />}
        </Form.Item>
      </Form>
    </>
  );
};

export default UserProvisioningView;
