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
import {
  PageHeader,
  Table,
  Button,
  Form,
  Input,
  message,
  Modal,
  Space,
  Spin,
  Alert,
} from "antd";
import { FilterDropdownProps } from "antd/lib/table/interface";
import { SearchOutlined } from "@ant-design/icons";
import Draggable from "react-draggable";
import * as React from "react";
import { useRef, useState } from "react";
import { getStateUserPermissions, createNewUser } from "../AdminPanelAPI";
import { useFetchedDataJSON } from "../hooks";

const CreateAddUserForm = ({
  visible,
  onCreate,
  onCancel,
}: {
  visible: boolean;
  onCreate: (arg0: StateUserPermissionsResponse) => Promise<void>;
  onCancel: () => void;
}) => {
  const [form] = Form.useForm();

  // make modal draggable
  const [disabled, setDisabled] = useState(false);
  const [bounds, setBounds] = useState({
    left: 0,
    top: 0,
    bottom: 0,
    right: 0,
  });
  const dragRef = useRef<HTMLInputElement>(null);
  const onStart = (_event: unknown, uiData: { x: number; y: number }) => {
    const { clientWidth, clientHeight } = window.document.documentElement;
    const targetRect = dragRef.current?.getBoundingClientRect();
    if (!targetRect) {
      return;
    }
    setBounds({
      left: -targetRect.left + uiData.x,
      right: clientWidth - (targetRect.right - uiData.x),
      top: -targetRect.top + uiData.y,
      bottom: clientHeight - (targetRect.bottom - uiData.y),
    });
  };

  return (
    <Modal
      visible={visible}
      title={
        <div
          style={{
            width: "100%",
            cursor: "move",
          }}
          onMouseOver={() => {
            if (disabled) {
              setDisabled(false);
            }
          }}
          onMouseOut={() => {
            setDisabled(true);
          }}
          onFocus={() => undefined}
          onBlur={() => undefined}
        >
          Add a New User
        </div>
      }
      okText="Add"
      cancelText="Cancel"
      onCancel={onCancel}
      onOk={() => {
        form.validateFields().then((values) => {
          form.resetFields();
          onCreate(values);
        });
      }}
      modalRender={(modal) => (
        <Draggable
          disabled={disabled}
          bounds={bounds}
          onStart={(event, uiData) => onStart(event, uiData)}
        >
          <div ref={dragRef}>{modal}</div>
        </Draggable>
      )}
    >
      <Alert
        message="Caution!"
        description="This form should only be used by members of the Polaris team."
        type="warning"
        showIcon
      />
      <br />
      <Form
        form={form}
        layout="horizontal"
        onFinish={onCreate}
        labelCol={{ span: 6 }}
      >
        <Form.Item
          name="emailAddress"
          label="Email Address"
          rules={[
            {
              required: true,
              message: "Please input the user's email address.",
            },
            { type: "email", message: "Please enter a valid email." },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="stateCode"
          label="State"
          rules={[
            {
              required: true,
              message: "Please input the user's state code.",
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item
          name="role"
          label="Role"
          rules={[
            {
              required: true,
              message: "Please input the user's role.",
            },
          ]}
        >
          <Input />
        </Form.Item>
        <Form.Item name="externalId" label="External ID">
          <Input />
        </Form.Item>
        <Form.Item name="district" label="District">
          <Input />
        </Form.Item>
        <Form.Item name="firstName" label="First Name">
          <Input />
        </Form.Item>
        <Form.Item name="lastName" label="Last Name">
          <Input />
        </Form.Item>
      </Form>
    </Modal>
  );
};

const StateUserPermissionsView = (): JSX.Element => {
  const { loading, data, setData } = useFetchedDataJSON<
    StateUserPermissionsResponse[]
  >(getStateUserPermissions);
  const [visible, setVisible] = useState(false);

  if (loading || !data) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const onAdd = async ({
    emailAddress,
    stateCode,
    externalId,
    role,
    district,
    firstName,
    lastName,
  }: StateUserPermissionsResponse) => {
    try {
      const response = await createNewUser(
        emailAddress,
        stateCode,
        externalId,
        role,
        district,
        firstName,
        lastName
      );
      if (!response.ok) {
        const { error } = await response.json();
        message.error(`An error occurred: ${error}`);
        return;
      }
      const user = await response.json();
      setData([...data, user]);
      setVisible(false);
      message.success(`"${emailAddress}" added!`);
    } catch (err) {
      message.error(`An error occurred: ${err}`);
    }
  };

  const getColumnSearchProps = (dataIndex: string) => ({
    filterDropdown: ({
      setSelectedKeys,
      selectedKeys,
      confirm,
      clearFilters,
    }: FilterDropdownProps) => (
      <div style={{ padding: 8 }}>
        <Input
          placeholder="Search..."
          value={selectedKeys[0]}
          onChange={(e) =>
            setSelectedKeys(e.target.value ? [e.target.value] : [])
          }
          onPressEnter={() => confirm()}
          style={{
            marginBottom: 8,
            display: "block",
          }}
        />
        <Space>
          <Button
            type="primary"
            onClick={() => confirm()}
            icon={<SearchOutlined />}
            size="small"
            style={{
              width: 90,
            }}
          >
            Search
          </Button>
          <Button
            onClick={clearFilters}
            size="small"
            style={{
              width: 90,
            }}
          >
            Reset
          </Button>
        </Space>
      </div>
    ),
    filterIcon: (filtered: boolean) => (
      <SearchOutlined style={{ color: filtered ? "#1890ff" : undefined }} />
    ),
    onFilter: (
      value: string | number | boolean,
      record: StateUserPermissionsResponse
    ) =>
      record[dataIndex as keyof StateUserPermissionsResponse]
        .toString()
        .toLowerCase()
        .includes(value.toString().toLowerCase()),
  });

  const columns = [
    {
      title: "Email",
      dataIndex: "emailAddress",
      width: 250,
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.emailAddress.localeCompare(b.emailAddress),
      ...getColumnSearchProps("emailAddress"),
    },
    {
      title: "First Name",
      dataIndex: "firstName",
    },
    {
      title: "Last Name",
      dataIndex: "lastName",
    },
    {
      title: "State",
      dataIndex: "stateCode",
      filters: [
        {
          text: "CO",
          value: "US_CO",
        },
        {
          text: "ID",
          value: "US_ID",
        },
        {
          text: "ME",
          value: "US_ME",
        },
        {
          text: "MI",
          value: "US_MI",
        },
        {
          text: "MO",
          value: "US_MO",
        },
        {
          text: "ND",
          value: "US_ND",
        },
        {
          text: "TN",
          value: "US_TN",
        },
      ],
      onFilter: (
        value: string | number | boolean,
        record: StateUserPermissionsResponse
      ) =>
        record.stateCode.indexOf(
          value as keyof StateUserPermissionsResponse
        ) === 0,
      sorter: (
        a: StateUserPermissionsResponse,
        b: StateUserPermissionsResponse
      ) => a.stateCode.localeCompare(b.stateCode),
    },
    {
      title: "Can Access Leadership Dashboard",
      dataIndex: "canAccessLeadershipDashboard",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.canAccessLeadershipDashboard != null) {
          return record.canAccessLeadershipDashboard.toString();
        }
      },
    },
    {
      title: "Can Access Case Triage",
      dataIndex: "canAccessCaseTriage",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.canAccessCaseTriage != null) {
          return record.canAccessCaseTriage.toString();
        }
      },
    },
    {
      title: "Should See Beta Charts",
      dataIndex: "shouldSeeBetaCharts",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.shouldSeeBetaCharts != null) {
          return record.shouldSeeBetaCharts.toString();
        }
      },
    },
    {
      title: "Routes",
      dataIndex: "routes",
      width: 300,
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.routes) {
          return JSON.stringify(record.routes, null, "\t").slice(2, -2);
        }
      },
    },
    {
      title: "Allowed Supervision Location IDs",
      dataIndex: "allowedSupervisionLocationIds",
    },
    {
      title: "Allowed Supervision Location Level",
      dataIndex: "allowedSupervisionLocationLevel",
      width: 220,
    },
    {
      title: "Blocked",
      dataIndex: "blocked",
      render: (_: string, record: StateUserPermissionsResponse) => {
        if (record.blocked != null) {
          return record.blocked.toString();
        }
      },
    },
  ];

  return (
    <>
      <PageHeader title="State User Permissions" />
      <Button
        onClick={() => {
          setVisible(true);
        }}
      >
        Add User
      </Button>
      <CreateAddUserForm
        visible={visible}
        onCreate={onAdd}
        onCancel={() => {
          setVisible(false);
        }}
      />
      <br /> <br />
      <Table dataSource={data} columns={columns} scroll={{ x: 1700, y: 700 }} />
    </>
  );
};

export default StateUserPermissionsView;
