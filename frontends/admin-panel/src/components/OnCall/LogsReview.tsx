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
import * as React from "react";
import {
  Button,
  Card,
  Collapse,
  Divider,
  Form,
  Radio,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import {
  CheckCircleOutlined,
  FileAddOutlined,
  FileSearchOutlined,
  MinusCircleOutlined,
} from "@ant-design/icons";
import styled from "styled-components/macro";
import moment from "moment";
import { Loading } from "@recidiviz/design-system";
import newGithubIssueUrl from "new-github-issue-url";
import { fetchOncallLogs } from "../../AdminPanelAPI/OnCall";
import { gcpEnvironment } from "../Utilities/EnvironmentUtilities";

const { Text } = Typography;

const CodeBlock = styled.pre`
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

type RequestTrace = {
  timestamp: string;
  trace: string;
};

type ErrorInstance = {
  method: string;
  status: string;
  url: string;
  errorLogs: string[];
  latestResponse: string;
  earliestResponse: string;
  sinceSucceededTimestamp: string;
  traces: RequestTrace[];
};

type ErrorInstanceProps = {
  instance: ErrorInstance;
};

function getIssueTitle(instance: ErrorInstance): string {
  return `${instance.status} ${instance.method} \`${instance.url}\``;
}

const ErrorInstanceHeader = ({ instance }: ErrorInstanceProps) => {
  const tag = instance.sinceSucceededTimestamp ? (
    <Tag icon={<CheckCircleOutlined />} color="success">
      A 200 response was recorded on {instance.sinceSucceededTimestamp}
    </Tag>
  ) : (
    <Tag icon={<MinusCircleOutlined />} color="error">
      Last seen: {instance.latestResponse}
    </Tag>
  );

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
      }}
    >
      <div>
        <code>
          <Text strong>
            {instance.status} {instance.method}
          </Text>{" "}
          <Text>{instance.url}</Text>
        </code>
        <br />
        {tag}
      </div>
      <Space size="small">
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
    </div>
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

const getTraceQuery = (trace: RequestTrace): string => {
  return `resource.type = "gae_app" trace = "${trace.trace}"`;
};

const TraceLink = ({ trace }: { trace: RequestTrace }) => {
  const projectId = gcpEnvironment.isProduction
    ? "recidiviz-123"
    : "recidiviz-staging";

  return (
    <Button
      target="_blank"
      href={`https://console.cloud.google.com/logs/query;query=${encodeURIComponent(
        getTraceQuery(trace)
      )}?project=${projectId}`}
    >
      View Logs
    </Button>
  );
};

type ErrorInstanceTab = "summary" | "traces";

const ERROR_INSTANCE_TAB_LIST = [
  { key: "summary", tab: "Summary" },
  { key: "traces", tab: "Traces" },
];

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
    summary: instance.errorLogs.map((message: string) => {
      return <CodeBlock>{message}</CodeBlock>;
    }),
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
              const date = moment(result.latestResponse).format("LL");
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

  React.useEffect(() => {
    requestOnCallLogs({ view: "direct_ingest" });
  }, []);

  return (
    <>
      <Typography.Title level={2}>On-Call Error Logs</Typography.Title>

      <Card>
        <Form
          layout="vertical"
          initialValues={{ view: "direct_ingest" }}
          onFinish={requestOnCallLogs}
        >
          <Form.Item name="view" label="Logs To View">
            <Radio.Group
              options={[
                { label: "Direct Ingest", value: "direct_ingest" },
                { label: "App Engine", value: "app_engine" },
                { label: "Cloud Run", value: "cloud_run" },
              ]}
            />
          </Form.Item>
          <Form.Item>
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
