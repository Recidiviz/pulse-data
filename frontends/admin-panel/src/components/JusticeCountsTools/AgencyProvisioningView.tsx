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
  Space,
} from "antd";
import * as React from "react";
import { SearchOutlined } from "@ant-design/icons";
import { FilterDropdownProps } from "antd/lib/table/interface";
import { getAgencies, createAgency } from "../../AdminPanelAPI";
import {
  AgenciesResponse,
  Agency,
  CreateAgencyRequest,
  CreateAgencyResponse,
  ErrorResponse,
} from "./constants";
import { useFetchedDataJSON } from "../../hooks";
import { formLayout, formTailLayout } from "../constants";

const AgencyProvisioningView = (): JSX.Element => {
  const [showSpinner, setShowSpinner] = React.useState(false);
  const { data, setData } = useFetchedDataJSON<AgenciesResponse>(getAgencies);
  const [form] = Form.useForm();

  const onFinish = async ({ name }: CreateAgencyRequest) => {
    const nameTrimmed = name.trim();
    setShowSpinner(true);
    try {
      const response = await createAgency(nameTrimmed);
      if (!response.ok) {
        const { error } = (await response.json()) as ErrorResponse;
        setShowSpinner(false);
        message.error(`An error occured: ${error}`);
        return;
      }
      const { agency } = (await response.json()) as CreateAgencyResponse;
      setData({
        agencies: data?.agencies ? [...data.agencies, agency] : [agency],
      });
      form.resetFields();
      setShowSpinner(false);
      message.success(`"${nameTrimmed}" added!`);
    } catch (err) {
      setShowSpinner(false);
      message.error(`An error occured: ${err}`);
    }
  };

  const getColumnSearchProps = (dataIndex: keyof Agency) => ({
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
    onFilter: (value: string | number | boolean, record: Agency) => {
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
      title: "Name",
      dataIndex: "name",
      key: "name",
      ...getColumnSearchProps("name"),
    },
  ];

  return (
    <>
      <PageHeader title="Agency Provisioning" />
      <Table
        columns={columns}
        dataSource={data?.agencies}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          size: "small",
        }}
        rowKey={(agency) => agency.id}
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
          Add Agency
        </Typography.Title>
        <Form.Item label="Name" name="name" rules={[{ required: true }]}>
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

export default AgencyProvisioningView;
