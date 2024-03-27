// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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
  AlignLeftOutlined,
  BookOutlined,
  CheckCircleOutlined,
  FileAddOutlined,
  FileSearchOutlined,
  MinusCircleOutlined,
} from "@ant-design/icons";
import { Loading } from "@recidiviz/design-system";
import {
  Button,
  Card,
  Checkbox,
  Collapse,
  Descriptions,
  Divider,
  Form,
  Radio,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import { CheckboxChangeEvent } from "antd/es/checkbox";
import { CheckboxValueType } from "antd/es/checkbox/Group";
import moment from "moment-timezone";
import newGithubIssueUrl from "new-github-issue-url";
import * as React from "react";
import styled from "styled-components/macro";

import { fetchOncallLogs } from "../../AdminPanelAPI/OnCall";
import { ErrorInstance, RequestTrace } from "./types";
import {
  buildLogsExplorerUrl,
  formatTimestamp,
  getIssueTitle,
  getRequestsQuery,
  getStatusCodeTitle,
  getTraceQuery,
} from "./utils";

const { Text, Title } = Typography;

const CodeBlock = styled.pre`
  font-size: smaller;
  padding: 0.4em 0.6em;
  white-space: pre-wrap;
  word-wrap: break-word;
  background: rgba(150, 150, 150, 0.1);
  border: 1px solid rgba(100, 100, 100, 0.2);
  border-radius: 3px;
`;

const CollapsePanelWithFullWidthHeader = styled(Collapse.Panel)`
  & .ant-collapse-header-text {
    width: 100%;
  }
`;

type ErrorInstanceProps = {
  instance: ErrorInstance;
};
const ErrorInstanceHeader = ({ instance }: ErrorInstanceProps) => {
  const tag = instance.sinceSucceededTimestamp ? (
    <Tag icon={<CheckCircleOutlined />} color="success">
      200 seen: {formatTimestamp(instance.sinceSucceededTimestamp)}
    </Tag>
  ) : null;

  return (
    <>
      <code>
        <Text strong>{instance.method}</Text>{" "}
        <Text ellipsis style={{ maxWidth: "65vw" }}>
          {instance.url}
        </Text>
      </code>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <code>
          <Text strong>{instance.status} </Text>
          <Text type="secondary">
            {getStatusCodeTitle(parseInt(instance.status))}
          </Text>{" "}
        </code>
        <aside>
          {tag}
          <Tag icon={<MinusCircleOutlined />} color="error">
            Last seen: {formatTimestamp(instance.latestResponse)}
          </Tag>
        </aside>
      </div>
    </>
  );
};
const TraceTableColumns = [
  {
    title: "Timestamp",
    dataIndex: "timestamp",
    key: "timestamp",
  },
  {
    title: "Trace",
    dataIndex: "trace",
    key: "trace",
  },
  {
    title: "Logs Explorer Link",
    dataIndex: "link",
    key: "link",
  },
];

const TraceLink = ({ trace }: { trace: RequestTrace }) => {
  return (
    <Button target="_blank" href={buildLogsExplorerUrl(getTraceQuery(trace))}>
      View Logs
    </Button>
  );
};

type ErrorInstanceTab = "summary" | "traces";

const ERROR_INSTANCE_TAB_LIST = [
  { key: "summary", tab: "Summary" },
  { key: "traces", tab: "Traces" },
];

const ErrorInstanceInfo = ({ instance }: ErrorInstanceProps) => {
  const url = new URL(instance.url, window.location.href);
  return (
    <>
      <Title level={5}>
        <samp>{url.pathname}</samp>{" "}
      </Title>{" "}
      <Descriptions bordered column={3}>
        {Array.from(url.searchParams.entries()).map(
          ([key, value]: [string, string]) => (
            <Descriptions.Item label={<Text strong>{key}</Text>} span={3}>
              <samp>{value}</samp>
            </Descriptions.Item>
          )
        )}
      </Descriptions>
      <br />
      <Space>
        {instance.resource.type === "gae_app" ? (
          <Button
            href={`https://app.gitbook.com/o/-MS0FZPVqDyJ1aem018G/s/-MRvK9sMirb5JcYHAkjo-887967055/endpoint-catalog${url.pathname}`}
            target="_blank"
          >
            <BookOutlined /> Endpoint Documentation
          </Button>
        ) : null}
        <Button
          href={buildLogsExplorerUrl(getRequestsQuery(instance))}
          target="_blank"
        >
          <AlignLeftOutlined /> Recent Logs
        </Button>
        <Button
          href={`https://github.com/recidiviz/pulse-data/issues?q=${encodeURIComponent(
            getIssueTitle(instance)
          )}`}
          target="_blank"
        >
          <FileSearchOutlined /> Search Issues
        </Button>
        <Button
          href={newGithubIssueUrl({
            user: "Recidiviz",
            repo: "pulse-data",
            title: getIssueTitle(instance),
            template: "bug_report.md",
          })}
          target="_blank"
        >
          <FileAddOutlined /> Open Issue
        </Button>
      </Space>
    </>
  );
};

const ErrorInstanceComponent = ({ instance }: ErrorInstanceProps) => {
  const [activeTabKey, setActiveTabKey] =
    React.useState<ErrorInstanceTab>("summary");

  const traces = instance.traces.map((trace: RequestTrace) => {
    return {
      ...trace,
      link: <TraceLink trace={trace} />,
    };
  });

  const contentList = {
    summary: (
      <>
        <ErrorInstanceInfo instance={instance} />
        <Divider />
        <Title level={5}>Error logs</Title>
        {instance.errorLogs.map((message: string) => {
          return <CodeBlock>{message}</CodeBlock>;
        })}
      </>
    ),
    traces: <Table dataSource={traces} columns={TraceTableColumns} />,
  };

  return (
    <Card
      style={{ width: "100%" }}
      tabList={ERROR_INSTANCE_TAB_LIST}
      activeTabKey={activeTabKey}
      onTabChange={(key) => {
        setActiveTabKey(key as ErrorInstanceTab);
      }}
    >
      {contentList[activeTabKey]}
    </Card>
  );
};

type ErrorInstancesByDate = Record<string, ErrorInstance[]>;

const NOISY_STATUSES = [400, 403, 404, 405, 409, 429, 431, 499, 502].map(
  (code) => ({
    label: (
      <>
        <Text strong>{code}</Text>{" "}
        <Text type="secondary">
          <small>{getStatusCodeTitle(code)}</small>
        </Text>
      </>
    ) as React.ReactNode,
    value: code,
  })
);

const Component: React.FC = () => {
  const [isLoading, setIsLoading] = React.useState(false);
  const [results, setResults] = React.useState<ErrorInstancesByDate>({});

  function requestOnCallLogs(values: Record<string, string>) {
    setIsLoading(true);
    fetchOncallLogs(values)
      .then((response) => response.json())
      .then((data: ErrorInstance[]) => {
        const errorsByLastResponseDate: Record<string, ErrorInstance[]> =
          data.reduce(
            (memo: Record<string, ErrorInstance[]>, result: ErrorInstance) => {
              const date = moment(result.latestResponse)
                .tz(moment.tz.guess())
                .format("LL");
              return {
                ...memo,
                [date]: memo[date] ? [...memo[date], result] : [result],
              };
            },
            {}
          );
        setIsLoading(false);
        setResults(errorsByLastResponseDate);
      });
  }
  const [form] = Form.useForm();
  const view = Form.useWatch("view", form);
  const statuses = Form.useWatch("ignored_statuses", form);

  React.useEffect(() => {
    form.submit();
  }, [form]);

  const [checkAll, setCheckAll] = React.useState(false);
  const [indeterminate, setIndeterminate] = React.useState(true);

  const onChange = (list: CheckboxValueType[]) => {
    form.setFieldsValue({
      statuses: list,
    });
    setIndeterminate(!!list.length && list.length < NOISY_STATUSES.length);
    setCheckAll(list.length === NOISY_STATUSES.length);
  };

  const onCheckAllChange = (e: CheckboxChangeEvent) => {
    form.setFieldsValue({
      statuses: e.target.checked
        ? NOISY_STATUSES.map((option) => option.value)
        : [],
    });
    setIndeterminate(false);
    setCheckAll(e.target.checked);
  };

  return (
    <>
      <Typography.Title level={2}>On-Call Error Logs</Typography.Title>

      <Card>
        <Form
          layout="horizontal"
          labelCol={{ span: 4 }}
          wrapperCol={{ span: 18 }}
          initialValues={{
            view: "direct_ingest",
            ignored_statuses: NOISY_STATUSES.map((option) => option.value),
            cloud_run_services: [
              "application-data-import",
              "asset-generation",
              "case-triage-web",
            ],
          }}
          onFinish={(values) => requestOnCallLogs(values)}
          form={form}
        >
          <Form.Item name="view" label="Services">
            <Radio.Group
              options={[
                { label: "Direct Ingest", value: "direct_ingest" },
                { label: "App Engine", value: "app_engine" },
                { label: "Cloud Run", value: "cloud_run" },
              ]}
            />
          </Form.Item>

          {view === "cloud_run" ? (
            <Form.Item name="cloud_run_services" wrapperCol={{ offset: 4 }}>
              <Checkbox.Group
                options={[
                  {
                    label: "Application Data Import",
                    value: "application-data-import",
                  },
                  { label: "Asset Generation", value: "asset-generation" },
                  { label: "Case Triage", value: "case-triage-web" },
                  { label: "Justice Counts", value: "justice-counts-web" },
                  { label: "Admin Panel", value: "admin-panel" },
                ]}
              />
            </Form.Item>
          ) : null}

          <Form.Item name="ignored_statuses" label="Ignored statuses">
            <Checkbox
              indeterminate={indeterminate}
              onChange={onCheckAllChange}
              checked={checkAll}
            >
              Check all
            </Checkbox>
            <Checkbox.Group
              options={NOISY_STATUSES}
              onChange={onChange}
              value={statuses}
              style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr" }}
            />
          </Form.Item>

          <Form.Item
            name="show_resolved"
            valuePropName="checked"
            wrapperCol={{ offset: 4 }}
          >
            <Checkbox>Show Resolved Issues</Checkbox>
          </Form.Item>
          <Form.Item wrapperCol={{ offset: 4 }}>
            <Button type="primary" htmlType="submit">
              Search
            </Button>
          </Form.Item>
        </Form>
      </Card>
      <Divider />

      {isLoading ? (
        <Loading />
      ) : (
        Object.entries(results).map(
          ([date, instances]: [string, ErrorInstance[]]) => {
            return (
              <>
                <Typography.Title level={4}>{date}</Typography.Title>
                <Collapse>
                  {instances.map((result: ErrorInstance) => (
                    <CollapsePanelWithFullWidthHeader
                      header={<ErrorInstanceHeader instance={result} />}
                      key={`${result.status} ${result.url}`}
                    >
                      <ErrorInstanceComponent instance={result} />
                    </CollapsePanelWithFullWidthHeader>
                  ))}
                </Collapse>
                <Divider />
              </>
            );
          }
        )
      )}
    </>
  );
};

export default Component;
