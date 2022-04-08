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
import {
  PageHeader,
  Table,
  Form,
  Input,
  Button,
  Spin,
  Typography,
  message,
  Select,
} from "antd";
import * as React from "react";
import { getUsers, createUser, getAgencies } from "../../AdminPanelAPI";
import {
  AgenciesResponse,
  Agency,
  CreateUserRequest,
  CreateUserResponse,
  UsersResponse,
  ErrorResponse,
} from "./constants";
import { useFetchedDataJSON } from "../../hooks";
import { formLayout, formTailLayout } from "../constants";

const UserProvisioningView = (): JSX.Element => {
  const [showSpinner, setShowSpinner] = React.useState(false);
  const { data: usersData, setData: setUsersData } =
    useFetchedDataJSON<UsersResponse>(getUsers);
  const { data: agenciesData } =
    useFetchedDataJSON<AgenciesResponse>(getAgencies);
  const [form] = Form.useForm();
  const columns = [
    {
      title: "Email",
      dataIndex: "email_address",
      key: "email_address",
    },
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
    },
    {
      title: "Agency",
      dataIndex: "agencies",
      key: "agency",
      render: (agencies: Agency[]) => agencies.map((agency) => agency.name),
    },
  ];
  const onFinish = async ({ email, agencyId, name }: CreateUserRequest) => {
    const emailTrimmed = email.trim();
    const nameTrimmed = name?.trim();
    setShowSpinner(true);
    try {
      const response = await createUser(emailTrimmed, [agencyId], nameTrimmed);
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        setShowSpinner(false);
        message.error(`An error occured: ${error}`);
        return;
      }
      const { user } = (await response.json()) as CreateUserResponse;
      setUsersData({
        users: usersData?.users ? [...usersData.users, user] : [user],
      });
      form.resetFields();
      setShowSpinner(false);
      message.success(`"${emailTrimmed}" added!`);
    } catch (err) {
      setShowSpinner(false);
      message.error(`An error occured: ${err}`);
    }
  };

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
        rowKey={(user) => user.id}
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
        <Form.Item label="Email" name="email" rules={[{ required: true }]}>
          <Input disabled={showSpinner} />
        </Form.Item>
        <Form.Item label="Name" name="name" rules={[{ required: false }]}>
          <Input disabled={showSpinner} />
        </Form.Item>
        <Form.Item label="Agency" name="agencyId" rules={[{ required: true }]}>
          <Select
            showSearch
            optionFilterProp="children"
            disabled={showSpinner}
            filterOption={(input, option) =>
              option?.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
            }
          >
            {/* #TODO(#12091): Replace with debounced search bar */}
            {agenciesData?.agencies.map((agency) => (
              <Select.Option key={agency.id} value={agency.id}>
                {agency.name}
              </Select.Option>
            ))}
          </Select>
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
