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
import * as React from "react";
import {
  Alert,
  Badge,
  Button,
  Card,
  Col,
  Form,
  DatePicker,
  Row,
  Select,
  Space,
} from "antd";
import { SearchOutlined } from "@ant-design/icons";
import moment from "moment";
import {
  createDiscovery,
  fetchIngestRegionCodes,
  Message,
  pollDiscoveryStatus,
} from "../AdminPanelAPI";
import useFetchedData from "../hooks";

interface FormValues {
  // eslint-disable-next-line camelcase
  region_code: string;
  dates: moment.Moment[];
  columns: string[];
  values: string[];
}

interface ReportCardProps {
  message: Message;
}

const ReportCard: React.FC<ReportCardProps> = ({
  message,
}: ReportCardProps) => {
  return (
    <Card title="Report">
      {message && message.kind !== "close" ? (
        <Badge status="processing" text={message.data} />
      ) : (
        <div
          id="report"
          dangerouslySetInnerHTML={{
            __html: message ? message.data : "",
          }}
        />
      )}
    </Card>
  );
};

const DirectionsCard: React.FC = () => (
  <Card title="How to use this tool" type="inner">
    <p>
      The Data Discovery tool is useful for investigating instance-specific
      ingest data quality issues.
    </p>
    <ol>
      <li>Identify a relevant time range to search</li>
      <li>
        Input a set of columns / values to filter for
        <br />
        Returns rows matching any permutation of <strong>column = value</strong>
      </li>
    </ol>
  </Card>
);

const Component: React.FC = () => {
  const [message, setMessage] = React.useState<Message | null>();
  const { loading, data: regionCodes } = useFetchedData<string[]>(
    fetchIngestRegionCodes
  );

  const submit = React.useCallback(async (values: FormValues) => {
    const body = { ...values };
    setMessage({ cursor: -1, data: "Creating search task", kind: "open" });

    const [start, end] = body.dates;

    try {
      const response = await createDiscovery({
        region_code: body.region_code,
        columns: body.columns,
        values: body.values,
        start_date: start.format("Y-MM-DD"),
        end_date: end.format("Y-MM-DD"),
      });
      const { id } = await response.json();

      await pollDiscoveryStatus(id, null, (receivedMessage: Message) => {
        setMessage(receivedMessage);
      });
    } catch (e) {
      setMessage(null);
      throw e;
    }
  }, []);

  const isLoading = !!message && message.kind !== "close";

  return (
    <>
      <Alert
        type="error"
        message="Not yet available"
        description="The Data discovery tool is not yet available, it will be soon, though!"
      />
      <br />
      <Row gutter={18}>
        <Col span={12}>
          <Card title="Data Discovery">
            <Form onFinish={submit} layout="vertical">
              <Form.Item
                label="State"
                name="region_code"
                rules={[{ required: true }]}
              >
                <Select>
                  {!loading && regionCodes
                    ? regionCodes.map((code) => (
                        <Select.Option key={code} value={code}>
                          {code.toUpperCase()}
                        </Select.Option>
                      ))
                    : null}
                </Select>
              </Form.Item>
              <Form.Item
                label="Date Range"
                help="This specifies the date range of when the ingest files were received / processed"
                name="dates"
                rules={[{ required: true }]}
              >
                <DatePicker.RangePicker
                  disabledDate={(current) =>
                    current && current > moment().endOf("day")
                  }
                />
              </Form.Item>
              <Form.Item
                name="columns"
                label="Columns"
                help="Specifies the columns of raw data files / ingest views to search for"
                rules={[{ required: true }]}
              >
                <Select mode="tags" />
              </Form.Item>

              <Form.Item
                name="values"
                label="Values"
                rules={[{ required: true }]}
              >
                <Select mode="tags" />
              </Form.Item>

              <Form.Item>
                <Button
                  type="primary"
                  htmlType="submit"
                  icon={<SearchOutlined />}
                  loading={isLoading}
                  disabled
                >
                  Search
                </Button>
              </Form.Item>
            </Form>
          </Card>
        </Col>
        <Col span={12}>
          <DirectionsCard />
        </Col>
      </Row>

      <Space size={18}>
        {message ? <ReportCard message={message} /> : null}
      </Space>
    </>
  );
};

export default Component;
