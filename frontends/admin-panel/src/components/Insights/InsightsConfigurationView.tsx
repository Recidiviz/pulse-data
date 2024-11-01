// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { Button, Form, Space } from "antd";
import { observer } from "mobx-react-lite";
import { useEffect } from "react";
import Markdown from "react-markdown";
import { useHistory, useParams } from "react-router-dom";
import styled from "styled-components/macro";

import { InsightsConfiguration } from "../../InsightsStore/models/InsightsConfiguration";
import { useInsightsStore } from "../StoreProvider";

const PreservedFormItem = styled.pre`
  margin-top: 5px;
  font-family: inherit;
  max-width: 50rem;
  white-space: pre-wrap;
`;

const Title = styled.h3`
  padding-top: 20px;
`;

const Heading = styled.h4``;

const MarkdownPreview = styled.div`
  padding: 5px 10px;
  border: 1px solid #d9d9d9;
  border-radius: 4px;
  background-color: #f9f9f9;
  overflow: auto;
`;

const MarkdownWrapper = styled.div`
  display: flex;
  flex-direction: row;
  gap: 10px;
`;

const Column = styled.div`
  flex: 1 1 0;
`;

const InsightsConfigurationView = (): JSX.Element => {
  const store = useInsightsStore();
  const { stateCode, configId: configIdString } = useParams<{
    stateCode: string;
    configId: string;
  }>();
  const configId = Number(configIdString);
  const history = useHistory();

  useEffect(() => {
    if (stateCode) store.setStateCode(stateCode);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stateCode]);

  useEffect(() => {
    if (store.hydrationState.status === "needs hydration") store.hydrate();
  }, [store]);
  const { configurationPresenter } = store;
  const config = configurationPresenter?.getConfigForId(configId);

  if (!configurationPresenter || !config) return <div />;

  const metadataColumns: Record<string, string>[] = [
    {
      title: "ID",
      dataIndex: "id",
    },
    {
      title: "Status",
      dataIndex: "status",
    },
    {
      title: "Feature Variant",
      dataIndex: "featureVariant",
    },
    {
      title: "Updated By",
      dataIndex: "updatedBy",
    },
    {
      title: "Updated At",
      dataIndex: "updatedAt",
    },
  ];

  // Create copy columns which have standard attributes
  const copyColumnNames = Object.keys(config).filter(
    (d) =>
      !metadataColumns
        .map((c) => {
          return c.dataIndex;
        })
        .includes(d) && d !== "actionStrategyCopy"
  );
  const copyColumns = copyColumnNames.map((c) => {
    return {
      title: c,
      dataIndex: c,
    };
  });

  const handleClick = async (callbackFn: (id: number) => Promise<boolean>) => {
    await callbackFn(config.id);
    history.push(`/admin/line_staff_tools/insights_configurations`);
  };

  return (
    <>
      <div>
        <h3>Managing configurations:</h3>
        Only ACTIVE configurations are available for use by the staging and
        production frontends. Use these actions to update which configurations
        are ACTIVE for specified feature variants, or the default configuration
        (no feature variant). Configurations cannot be created in production,
        only promoted from staging.
        <ul>
          <li>
            Reactivate: Turns an INACTIVE configuration ACTIVE. If there is
            another ACTIVE configuration with the same feature variant, it will
            become INACTIVE.
          </li>
          <li>
            Deactivate: Turns an ACTIVE configuration INACTIVE. This means it is
            no longer able to be promoted or used by the FE.
          </li>
          <li>
            Promote to default: Available on ACTIVE configurations with a
            feature variant set. Promoting to default will remove the feature
            variant and set it to the ACTIVE configuration for the default view.
          </li>
          <li>
            Promote to production: When promoted, this configuration will become
            the ACTIVE configuration used in the production frontend. Only
            available on staging configurations.
          </li>
        </ul>
      </div>
      <Space>
        {config.status === "ACTIVE" && (
          <>
            <Button
              onClick={() =>
                // eslint-disable-next-line no-alert
                window.confirm(
                  "Are you sure you want to deactivate this config? Only configs that are not the ACTIVE, default config can be deactivated."
                ) &&
                handleClick(configurationPresenter.deactivateSelectedConfig)
              }
            >
              Deactivate
            </Button>
            <Button
              onClick={() =>
                // eslint-disable-next-line no-alert
                window.confirm(
                  "Are you sure you want to promote this config to default?"
                ) &&
                handleClick(
                  configurationPresenter.promoteSelectedConfigToDefault
                )
              }
              disabled={!config.featureVariant}
            >
              Promote to default
            </Button>
            <Button
              onClick={() =>
                // eslint-disable-next-line no-alert
                window.confirm(
                  "Are you sure you want to promote this config to production?"
                ) &&
                configurationPresenter.promoteSelectedConfigToProduction(
                  config.id
                )
              }
              disabled={!configurationPresenter.envIsStaging}
            >
              Promote to Production (from staging)
            </Button>
          </>
        )}
        {config.status === "INACTIVE" && (
          <Button
            onClick={() =>
              // eslint-disable-next-line no-alert
              window.confirm(
                "Are you sure you want to reactivate this config? If there is another ACTIVE config with the same feature variant it will be deactivated."
              ) && handleClick(configurationPresenter.reactivateSelectedConfig)
            }
          >
            Reactivate
          </Button>
        )}
      </Space>
      <Title>Configuration details:</Title>
      <Form>
        {metadataColumns.concat(copyColumns).map((c) => (
          <Form.Item
            label={c.title}
            labelCol={{ span: 6 }}
            labelAlign="left"
            wrapperCol={{ span: 16 }}
          >
            <span className="ant-form-text">
              <PreservedFormItem>
                {config[c.dataIndex as keyof InsightsConfiguration]}
              </PreservedFormItem>
            </span>
          </Form.Item>
        ))}
        <Heading>Action Strategy Copy:</Heading>
        {Object.entries(config.actionStrategyCopy).map(([name, copyObject]) => (
          <>
            <div>{name}</div>
            <MarkdownWrapper>
              <Column>
                <div>Prompt</div>
                <MarkdownPreview>
                  <Markdown>{copyObject.prompt}</Markdown>
                </MarkdownPreview>
              </Column>
              <Column>
                <div>Body</div>
                <MarkdownPreview>
                  <Markdown>{copyObject.body}</Markdown>
                </MarkdownPreview>
              </Column>
            </MarkdownWrapper>
          </>
        ))}
      </Form>
    </>
  );
};

export default observer(InsightsConfigurationView);
