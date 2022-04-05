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
} from "antd";
import * as React from "react";
import { getAgencies, createAgency } from "../AdminPanelAPI";
import { useFetchedDataJSON } from "../hooks";
import { formLayout, formTailLayout } from "./constants";

type Agency = {
  id: number;
  name: string;
};

export type AgencyRequest = {
  name: string;
};

type AgenciesResponse = {
  agencies: Agency[];
};

type CreateAgencyResponse = {
  agency: Agency;
};

const AgencyProvisioningView = (): JSX.Element => {
  const [showSpinner, setShowSpinner] = React.useState(false);
  const { data, setData } = useFetchedDataJSON<AgenciesResponse>(getAgencies);
  const [form] = Form.useForm();
  const columns = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
    },
  ];
  const onFinish = async ({ name }: AgencyRequest) => {
    const nameTrimmed = name.trim();
    setShowSpinner(true);
    try {
      const response = await createAgency(nameTrimmed);
      const text = await response.text();
      try {
        const { agency } = JSON.parse(text) as CreateAgencyResponse; // Try to parse it as JSON
        if (data?.agencies) {
          setData({ agencies: [...data.agencies, agency] });
        } else {
          setData({ agencies: [agency] });
        }
        form.resetFields();
        setShowSpinner(false);
        message.success(`"${nameTrimmed}" added!`);
      } catch (err) {
        setShowSpinner(false);
        message.error(`An error occured: ${text}`);
      }
    } catch (err) {
      setShowSpinner(false);
      message.error(`An error occured: ${err}`);
    }
  };

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
