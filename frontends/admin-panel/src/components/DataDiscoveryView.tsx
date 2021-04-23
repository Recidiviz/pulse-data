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
import { Badge, Button, Card, Form, Space, Steps } from "antd";
import { LoadingOutlined, SearchOutlined } from "@ant-design/icons";
import { FormInstance } from "antd/es/form";
import {
  createDiscovery,
  Message,
  pollDiscoveryStatus,
} from "../AdminPanelAPI";
import DataDiscoverySelectStateView from "./DataDiscovery/DataDiscoverySelectStateView";
import DataDiscoveryFiltersView from "./DataDiscovery/DataDiscoveryFiltersView";
import DataDiscoverySelectFilesView from "./DataDiscovery/DataDiscoverySelectFilesView";

const { Step } = Steps;

interface ReportCardProps {
  message: Message;
}

const ReportCard: React.FC<ReportCardProps> = ({
  message,
}: ReportCardProps) => {
  return (
    <>
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
    </>
  );
};
interface FormContextType {
  form: FormInstance;
}

export type { FormContextType };
export const FormContext = React.createContext({} as FormContextType);

const Component: React.FC = () => {
  const [message, setMessage] = React.useState<Message | null>();
  const [form] = Form.useForm();

  const submit = React.useCallback(async () => {
    const body = { ...form.getFieldsValue(true) };
    setMessage({ cursor: -1, data: "Creating search task", kind: "open" });
    const [start, end] = body.dates;
    delete body.dates;

    try {
      const response = await createDiscovery({
        ...body,
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
  }, [form]);

  const isLoading = !!message && message.kind !== "close";

  const [current, setCurrent] = React.useState(0);

  const next = () => {
    setCurrent(current + 1);
  };

  const prev = () => {
    setCurrent(current - 1);
  };

  const steps = [
    {
      title: "Choose State",
      description: "Select the state you want to search ingest data for",
      content: <DataDiscoverySelectStateView />,
    },
    {
      title: "Choose Files",
      description: "Select raw data files / ingest views you'd like to search",
      content: (
        <DataDiscoverySelectFilesView
          regionCode={form.getFieldValue("region_code")}
        />
      ),
    },
    {
      title: "Filter Conditions",
      description: "Define your filters",
      content: <DataDiscoveryFiltersView />,
    },
    {
      title: "View Result",
      description: "Report",
      content: message ? <ReportCard message={message} /> : null,
      icon: message && message.kind !== "close" ? <LoadingOutlined /> : null,
    },
  ];

  return (
    <FormContext.Provider value={{ form }}>
      <Card title="Data Discovery" className="data-discovery__card">
        <Form onFinish={submit} layout="vertical" form={form}>
          <Space direction="vertical" size={16}>
            <Steps current={current} style={{ width: "100%" }}>
              {steps.map((item) => (
                <Step key={item.title} title={item.title} icon={item.icon} />
              ))}
            </Steps>

            <Card
              title={steps[current].description || ""}
              type="inner"
              key={steps[current].title}
            >
              {steps[current].content}
            </Card>

            <Space>
              {current < steps.length - 2 && (
                <Button type="primary" onClick={() => next()} size="large">
                  Next
                </Button>
              )}
              {current === steps.length - 2 && (
                <Button
                  type="primary"
                  htmlType="submit"
                  onClick={async () => {
                    await form.validateFields();
                    await form.submit();
                    next();
                  }}
                  icon={<SearchOutlined />}
                  loading={isLoading}
                  size="large"
                >
                  Search
                </Button>
              )}
              {current > 0 && (
                <Button onClick={() => prev()} size="large">
                  Previous
                </Button>
              )}
            </Space>
          </Space>
        </Form>
      </Card>
    </FormContext.Provider>
  );
};

export default Component;
