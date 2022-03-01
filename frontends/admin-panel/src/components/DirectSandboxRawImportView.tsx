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
  Button,
  Card,
  Divider,
  Empty,
  Form,
  Input,
  message,
  Modal,
  List,
  PageHeader,
  Select,
  Spin,
} from "antd";
import * as React from "react";

import {
  importRawDataToSandbox,
  listSandboxBuckets,
} from "../AdminPanelAPI/IngestOperations";
import ImportSandboxFileStatusTable from "./IngestOperationsView/ImportSandboxFileStatusTable";
import { fetchIngestStateCodes } from "../AdminPanelAPI";
import { StateCodeInfo, FileStatus } from "./IngestOperationsView/constants";
import StateSelector from "./Utilities/StateSelector";
import { layout, tailLayout } from "./POEmails/constants";

interface SandboxFormData {
  sandboxDatasetPrefix: string;
  sourceBucket: string;
  fileTagFilterRegex: string;
}

const DirectSandboxRawImport = (): JSX.Element => {
  const [form] = Form.useForm();
  const [formData, setFormData] =
    React.useState<SandboxFormData | undefined>(undefined);
  const [stateInfo, setStateInfo] =
    React.useState<StateCodeInfo | undefined>(undefined);
  const [sourceBucketList, setSourceBucketList] =
    React.useState<string[] | undefined>(undefined);
  const [fileStatusList, setFileStatusList] =
    React.useState<FileStatus[] | undefined>(undefined);
  const [statusTableSpinner, setStatusTableSpinner] = React.useState(false);
  const [errorList, setErrorList] =
    React.useState<FileStatus[] | undefined>(undefined);
  const [visible, setVisible] = React.useState(false);

  const onFinish = (values?: SandboxFormData | undefined) => {
    setFormData(values);
    setVisible(true);
  };

  const onImportActionConfirmation = async () => {
    setVisible(false);
    setStatusTableSpinner(true);
    setFileStatusList(undefined);
    if (stateInfo && formData?.sandboxDatasetPrefix && formData?.sourceBucket) {
      const r = await importRawDataToSandbox(
        stateInfo.code,
        formData.sandboxDatasetPrefix,
        formData.sourceBucket,
        formData?.fileTagFilterRegex
      );
      const json = await r.json();
      if (r.status >= 400) {
        const errorMessage = `Import Data... failed: Either invalid parameters:\n 
        stateCode: ${stateInfo.code} \n sandboxDatasetPrefix: ${formData.sandboxDatasetPrefix} \n
        sourceBucket: ${formData.sourceBucket} \n fileTagFilterRegex: ${formData.fileTagFilterRegex}\n
        OR raw files are not already uploaded to \n [gs://${formData.sourceBucket}]. 
        `;
        message.error(
          <>
            <p>
              {errorMessage?.split("\n")?.map((error) => (
                <p>{error}</p>
              ))}
              {json.errorMessage ? (
                <p>{json.errorMessage}</p>
              ) : (
                <p>{json.message}</p>
              )}
            </p>
          </>
        );
      } else {
        message.success(`Importing Raw Files to BQ Sandbox... succeeded!`);
        setFileStatusList(json.fileStatusList);
        getErrorList(json.fileStatusList);
      }
    }
    setStatusTableSpinner(false);
  };

  const getErrorList = (statusList: FileStatus[]) => {
    const errors: FileStatus[] = [];
    statusList.forEach((file) => {
      if (file.errorMessage) {
        errors.push(file);
      }
    });
    setErrorList(errors);
  };

  const handleOk = () => {
    setVisible(false);
    onImportActionConfirmation();
  };

  const handleCancel = () => {
    setVisible(false);
    setFormData(undefined);
  };

  const getBucketNames = React.useCallback(() => {
    if (!stateInfo?.code) return;

    const getBucketList = async () => {
      form.setFieldsValue({
        sourceBucket: undefined,
      });
      const r = await listSandboxBuckets();
      const json = await r.json();
      setSourceBucketList(json.bucketNames);
    };
    getBucketList();
  }, [stateInfo?.code, form]);

  React.useEffect(() => {
    getBucketNames();
  }, [getBucketNames]);

  return (
    <>
      <PageHeader title="Direct Sandbox Raw Data Import" />
      <div style={{ width: "100%", marginTop: "8px", display: "inline-flex" }}>
        <span style={{ width: "35%" }}>
          <Card
            title="Parameters"
            style={{ width: "100%" }}
            extra={
              <StateSelector
                fetchStateList={fetchIngestStateCodes}
                onChange={setStateInfo}
              />
            }
          >
            {stateInfo ? (
              <Form
                form={form}
                {...layout}
                className="buffer"
                onFinish={onFinish}
              >
                <Form.Item
                  label="Dataset Prefix"
                  name="sandboxDatasetPrefix"
                  rules={[
                    {
                      required: true,
                      message: "Sandbox Dataset Prefix is required",
                    },
                  ]}
                >
                  <Input />
                </Form.Item>
                {sourceBucketList ? (
                  <Form.Item
                    label="Source Bucket"
                    name="sourceBucket"
                    rules={[
                      {
                        required: true,
                      },
                    ]}
                  >
                    <Select showSearch>
                      {sourceBucketList?.map((bucket) => (
                        <Select.Option
                          key={bucket}
                          label={bucket}
                          value={bucket}
                        >
                          {bucket}
                        </Select.Option>
                      ))}
                    </Select>
                  </Form.Item>
                ) : (
                  <Form.Item
                    label="Source Bucket"
                    name="sourceBucket"
                    rules={[
                      {
                        required: true,
                      },
                    ]}
                  >
                    <Spin style={{ marginLeft: "40%" }} />
                  </Form.Item>
                )}

                <Form.Item label="File Tag Regex" name="fileTagFilterRegex">
                  <Input />
                </Form.Item>
                <Form.Item {...tailLayout}>
                  <Button type="primary" htmlType="submit">
                    Import Data
                  </Button>
                </Form.Item>
              </Form>
            ) : (
              <Empty />
            )}
          </Card>
        </span>
        <span style={{ width: "35%", marginLeft: "25%", height: "25%" }}>
          <Card title="File Import Status">
            {fileStatusList ? (
              <ImportSandboxFileStatusTable fileStatusList={fileStatusList} />
            ) : (
              <>
                {statusTableSpinner ? (
                  <Spin
                    size="large"
                    style={{ marginLeft: "50%", marginRight: "50%" }}
                  />
                ) : (
                  <Empty />
                )}
              </>
            )}
          </Card>
        </span>
      </div>
      {fileStatusList ? (
        <div
          style={{
            width: "80%",
            marginTop: "10px",
            display: "block",
            marginLeft: "auto",
            marginRight: "auto",
          }}
        >
          <Divider orientation="left">Error Log</Divider>
          <List
            pagination={{ pageSize: 1 }}
            dataSource={errorList}
            renderItem={(item) => (
              <>
                <List.Item>
                  <Card title={item.fileTag} style={{ width: "100%" }}>
                    {item.errorMessage?.split("\n")?.map((fail) => (
                      <>
                        <p>{fail}</p>
                      </>
                    ))}
                  </Card>
                </List.Item>
              </>
            )}
          />
        </div>
      ) : null}

      {stateInfo && formData?.sourceBucket ? (
        <Modal
          visible={visible}
          title="Have files been uploaded?"
          onOk={handleOk}
          onCancel={handleCancel}
          footer={[
            <Button key="no" onClick={handleCancel}>
              No
            </Button>,
            <Button key="yes" type="primary" onClick={handleOk}>
              Yes
            </Button>,
          ]}
        >
          <p>
            Have you already uploaded raw files to [gs://{formData.sourceBucket}
            ] using script
          </p>
          <p>
            `recidiviz.tools.ingest.operations.upload_raw_state_files_to_ingest_bucket_with_date`
            with arg
          </p>
          <p>`--destination-bucket {formData.sourceBucket}`?</p>
        </Modal>
      ) : null}
    </>
  );
};

export default DirectSandboxRawImport;
