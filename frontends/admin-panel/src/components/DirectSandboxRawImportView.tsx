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
  listRawFilesInSandboxBucket,
} from "../AdminPanelAPI/IngestOperations";
import ImportSandboxFileStatusTable from "./DirectSandboxImport/ImportSandboxFileStatusTable";
import FileUploadDatesTable from "./DirectSandboxImport/FileUploadDatesTable";
import { fetchIngestStateCodes } from "../AdminPanelAPI";
import { FileStatus, FileUploadInfo } from "./DirectSandboxImport/constants";
import { StateCodeInfo } from "./IngestOperationsView/constants";
import StateSelector from "./Utilities/StateSelector";
import { layout, tailLayout } from "./POEmails/constants";

interface SandboxFormData {
  sandboxDatasetPrefix: string;
  sourceBucket: string;
  fileTagFilters: string[];
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
  const [rawFilesSpinner, setRawFilesSpinner] = React.useState(false);
  const [errorList, setErrorList] =
    React.useState<FileStatus[] | undefined>(undefined);
  const [visible, setVisible] = React.useState(false);
  const [rawFileUploadList, setRawFileUploadList] =
    React.useState<FileUploadInfo[] | undefined>(undefined);
  const [tempSourceBucket, setTempSourceBucket] =
    React.useState<string | undefined>(undefined);

  const onFinish = (values?: SandboxFormData | undefined) => {
    setFormData(values);
    setVisible(true);
  };

  const onFieldsChange = (values: SandboxFormData | undefined) => {
    setTempSourceBucket(values?.sourceBucket);
  };

  const getRawFilesList = React.useCallback(() => {
    if (!stateInfo?.code || !tempSourceBucket) return;

    setRawFileUploadList(undefined);
    setRawFilesSpinner(true);
    const rawFileList = async () => {
      const r = await listRawFilesInSandboxBucket(
        stateInfo.code,
        tempSourceBucket
      );
      const json = await r.json();
      if (r.status >= 400) {
        message.error(
          <>
            <p>Get Raw Files List... failed: Potentially invalid parameters:</p>
            <p>stateCode: ${stateInfo.code}</p>
            <p>sourceBucket: ${tempSourceBucket}</p>
          </>
        );
      } else {
        message.success(`Getting Raw Files From Bucket... succeeded!`);
        setRawFileUploadList(json.rawFilesList);
      }
      setRawFilesSpinner(false);
    };
    rawFileList();
  }, [stateInfo?.code, tempSourceBucket]);

  const onImportActionConfirmation = async () => {
    setVisible(false);
    setStatusTableSpinner(true);
    setFileStatusList(undefined);
    if (stateInfo && formData?.sandboxDatasetPrefix && formData?.sourceBucket) {
      const r = await importRawDataToSandbox(
        stateInfo.code,
        formData.sandboxDatasetPrefix,
        formData.sourceBucket,
        formData?.fileTagFilters
      );
      if (r.status >= 400) {
        const text = await r.text();
        message.error(
          <>
            <p>Import Data... failed: Either invalid parameters:</p>
            <p>stateCode: {stateInfo.code}</p>
            <p>sandboxDatasetPrefix: {formData.sandboxDatasetPrefix}</p>
            <p>sourceBucket: {formData.sourceBucket}</p>
            <p>fileTagFilters: {formData.fileTagFilters}</p>
            <p>OR {text}</p>
          </>
        );
      } else {
        const json = await r.json();
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
  };

  React.useEffect(() => {
    const getBucketList = async () => {
      form.setFieldsValue({
        sourceBucket: undefined,
      });
      const r = await listSandboxBuckets();
      const json = await r.json();
      setSourceBucketList(json.bucketNames);
    };
    getBucketList();
  }, [form]);

  React.useEffect(() => {
    getRawFilesList();
  }, [getRawFilesList]);

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
                onValuesChange={(_, values) => onFieldsChange(values)}
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

                <Form.Item
                  label="File Tag Filters"
                  name="fileTagFilters"
                  extra={<>{rawFilesSpinner ? <Spin size="small" /> : null}</>}
                >
                  <Select
                    mode="tags"
                    tokenSeparators={[",", " "]}
                    placeholder="CIS_100_CLIENT, CIS_125_CURRENT_STATUS_HIST"
                    allowClear
                  >
                    {rawFileUploadList?.map((file) => (
                      <Select.Option
                        key={file.fileTag}
                        label={file.fileTag}
                        value={file.fileTag}
                      >
                        {file.fileTag}
                      </Select.Option>
                    ))}
                  </Select>
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
      <div
        style={{
          width: "100%",
          marginTop: "10px",
          display: "inline-flex",
        }}
      >
        <span style={{ width: "35%", marginRight: "10px" }}>
          {tempSourceBucket ? (
            <Card title={`Raw files in ${tempSourceBucket}`}>
              {rawFileUploadList ? (
                <FileUploadDatesTable fileUploadList={rawFileUploadList} />
              ) : (
                <>{rawFilesSpinner ? <Spin size="large" /> : <Empty />}</>
              )}
            </Card>
          ) : null}
        </span>
        <span style={{ width: "60%" }}>
          {fileStatusList ? (
            <>
              <Card title="Error Log">
                <List
                  pagination={{ pageSize: 1 }}
                  dataSource={errorList}
                  style={{ width: "inherit" }}
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
              </Card>
            </>
          ) : null}
        </span>
      </div>

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
