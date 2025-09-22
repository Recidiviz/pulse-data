// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2020 Recidiviz, Inc.
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
import "../style/App.css";

import { Avatar, Layout, Menu, MenuProps, Segmented, Typography } from "antd";
import { SegmentedLabeledOption, SegmentedValue } from "antd/lib/segmented";
import classNames from "classnames";
import qs from "querystringify";
import { useEffect } from "react";
import {
  Redirect,
  Route,
  Switch,
  useHistory,
  useLocation,
} from "react-router-dom";

import Nelly from "../favicon-nelly.png";
import MetadataDataset from "../models/MetadataDatasets";
import * as DatasetMetadata from "../navigation/DatasetMetadata";
import * as IngestOperations from "../navigation/IngestOperations";
import * as Lineage from "../navigation/Lineage";
import * as LineStaffTools from "../navigation/LineStaffTools";
import * as OnCall from "../navigation/OnCall";
import DatasetView from "./Datasets/DatasetView";
import DemoAppManagementView from "./DemoAppManagement/DemoAppManagementView";
import IngestStatusView from "./IngestStatus";
import FlashDatabaseChecklist from "./IngestStatus/FlashChecklist/FlashDatabaseChecklist";
import InsightsConfigurationsView from "./Insights/InsightsConfigurationsView";
import InsightsConfigurationView from "./Insights/InsightsConfigurationView";
import LineageDisplay from "./Lineage/Lineage";
import OnCallLogsReview from "./OnCall/LogsReview";
import POEmailsView from "./POEmailsView";
import StateRoleDefaultPermissionsView from "./StateUserPermissions/StateRolePermissionsView";
import StateUserPermissionsView from "./StateUserPermissions/StateUserPermissionsView";
import StoreProvider from "./StoreProvider";
import { EnvironmentType } from "./types";
import UploadRawFilesView from "./UploadRawFilesView";
import ValidationStatusOverview from "./Validation/ValidationStatusOverview";
import OpportunitiesHeader from "./Workflows/OpportunitiesHeader";
import OpportunitiesView from "./Workflows/OpportunitiesView";
import OpportunityConfigurationForm from "./Workflows/OpportunityConfiguration/OpportunityConfigurationForm";
import OpportunityConfigurationView from "./Workflows/OpportunityConfiguration/OpportunityConfigurationView";
import OpportunityView from "./Workflows/OpportunityView";

type MenuItem = Required<MenuProps>["items"][number];
type QueryString = {
  stateCode?: string;
};

function getItem(
  label: React.ReactNode,
  key: React.Key,
  icon?: React.ReactNode,
  children?: MenuItem[],
  type?: "group"
): MenuItem {
  return {
    key,
    icon,
    children,
    label,
    type,
  } as MenuItem;
}

interface EnvironmentOption {
  baseUrl: string;
  title: string;
  backgroundColor: string;
}

const ENVIRONMENT_OPTIONS: Map<EnvironmentType, EnvironmentOption> = new Map([
  [
    "development",
    {
      title: "Development",
      baseUrl: "http://localhost:3030",
      backgroundColor: "#bf7474",
    },
  ],
  [
    "staging",
    {
      title: "Staging",
      baseUrl: "https://admin-panel-staging.recidiviz.org",
      backgroundColor: "#bf7474",
    },
  ],
  [
    "production",
    {
      title: "Production",
      baseUrl: "https://admin-panel-prod.recidiviz.org",
      backgroundColor: "#90aeb5",
    },
  ],
]);

const items: MenuProps["items"] = [
  getItem("Ingest", "ingest_group", null, [
    getItem("Ingest Status", IngestOperations.INGEST_DATAFLOW_ROUTE),
    getItem("Flash Databases", IngestOperations.FLASH_DB_CHECKLIST_ROUTE),
    getItem(
      "State Dataset",
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.INGEST)
    ),
  ]),
  getItem("Validation", "validation_group", null, [
    getItem("Validation Status", DatasetMetadata.VALIDATION_STATUS_ROUTE),
    getItem(
      "External Accuracy Dataset",
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.VALIDATION)
    ),
  ]),
  getItem("Line Staff Tools", "line_staff_tools_group", null, [
    getItem("GCS & Cloud SQL", LineStaffTools.GCS_CSV_TO_CLOUD_SQL_ROUTE),
    getItem("Cloud SQL & GCS", LineStaffTools.CLOUD_SQL_TO_GCS_CSV_ROUTE),
    getItem("PO Feedback", LineStaffTools.PO_FEEDBACK_ROUTE),
    getItem("Email Reports", LineStaffTools.EMAIL_REPORTS_ROUTE),
    getItem("Upload Raw Files", LineStaffTools.UPLOAD_RAW_FILES_ROUTE),
    getItem(
      "State User Permissions",
      LineStaffTools.STATE_USER_PERMISSIONS_ROUTE
    ),
    getItem(
      "State Role Default Permissions",
      LineStaffTools.STATE_ROLE_DEFAULT_PERMISSIONS_ROUTE
    ),
    getItem("Demo App Management", LineStaffTools.DEMO_APP_MANAGEMENT_ROUTE),
    getItem(
      "Insights Configuration",
      LineStaffTools.INSIGHTS_CONFIGURATION_ROUTE
    ),
    getItem(
      "Workflows Configuration",
      LineStaffTools.WORKFLOWS_OPPORTUNITIES_ROUTE
    ),
  ]),
  getItem("Lineage", Lineage.LINEAGE_BASE),
  getItem("On-Call", OnCall.ON_CALL_BASE_ROUTE),
];

const formatPageName = (page: string) => {
  return page
    .replace(/_/g, " ") // convert underscores to spaces
    .replace(/(^|\s)\S/g, function (t) {
      return t.toUpperCase();
    }) // title case
    .replace(/Sql/g, "SQL") // Correctly case acronyms
    .replace(/Gcs/g, "GCS")
    .replace(/Po/g, "PO")
    .replace(/POp/g, "Pop")
    .replace(/Csv/g, "CSV")
    .replace(/Pfi/g, "PFI");
};

const App = (): JSX.Element => {
  const location = useLocation();
  const title = "Admin Panel";
  const history = useHistory();
  const env = (window.RUNTIME_GCP_ENVIRONMENT ||
    "production") as EnvironmentType;

  const onClick: MenuProps["onClick"] = (e) => {
    history.push(e.key);
  };

  useEffect(() => {
    // Update the document title (tab name) based on the page and state
    const page = location.pathname.split("/").slice(-1)[0];
    const { stateCode } = qs.parse(location.search) as QueryString;
    const stateCodePrefix =
      stateCode && typeof stateCode === "string"
        ? `${stateCode?.split("_")[1]} `
        : "";
    document.title = ` ${stateCodePrefix}${formatPageName(page)}`;
  }, [location]);

  const routeClass = classNames({
    "main-content": true,
    "main-content-padding": ![
      DatasetMetadata.VALIDATION_STATUS_ROUTE,
      IngestOperations.INGEST_DATAFLOW_ROUTE,
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.VALIDATION),
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.INGEST),
      Lineage.LINEAGE_DETAIL,
      Lineage.LINEAGE_BASE,
    ].filter((x) => history.location.pathname.includes(x)).length,
  });

  const onEnvironmentChange = (value: SegmentedValue) => {
    window.open(value.toString().concat(window.location.search), "_blank");
  };

  return (
    <Layout style={{ height: "100%" }}>
      <Layout.Sider width={256}>
        <div
          className="title-header"
          style={{
            backgroundColor: `${ENVIRONMENT_OPTIONS.get(env)?.backgroundColor}`,
          }}
        >
          <Typography.Title level={3}>{title}</Typography.Title>
          <Avatar
            shape="square"
            style={{ backgroundColor: "inherit", padding: "2px" }}
            icon={<img src={Nelly} id="adminPanelNelly" alt="Nelly" />}
          />
        </div>

        <Segmented
          block
          options={getEnvLinkOptions(location.pathname)}
          onChange={onEnvironmentChange}
          value={window.location.origin.concat(location.pathname)}
        />
        <Menu
          onClick={onClick}
          mode="inline"
          selectedKeys={selectedMenuKeys(location.pathname)}
          items={items}
          defaultOpenKeys={selectedMenuKeys(location.pathname)}
        />
      </Layout.Sider>
      <div className={routeClass}>
        <Switch>
          <Redirect
            exact
            from="/"
            to={IngestOperations.INGEST_DATAFLOW_ROUTE}
          />
          <Redirect
            exact
            from={IngestOperations.ADMIN_BASE}
            to={IngestOperations.INGEST_DATAFLOW_ROUTE}
          />
          <Route
            path={DatasetMetadata.METADATA_DATASET_ROUTE_TEMPLATE}
            component={DatasetView}
          />
          <Route
            path={DatasetMetadata.VALIDATION_STATUS_ROUTE}
            component={ValidationStatusOverview}
          />
          <Route
            path={IngestOperations.INGEST_DATAFLOW_ROUTE}
            component={IngestStatusView}
          />
          <Route
            exact
            path={IngestOperations.FLASH_DB_CHECKLIST_ROUTE}
            component={FlashDatabaseChecklist}
          />
          <Route
            path={LineStaffTools.EMAIL_REPORTS_ROUTE}
            component={POEmailsView}
          />
          <Route
            path={LineStaffTools.UPLOAD_RAW_FILES_ROUTE}
            component={UploadRawFilesView}
          />
          <Route
            path={LineStaffTools.STATE_USER_PERMISSIONS_ROUTE}
            component={StateUserPermissionsView}
          />
          <Route
            path={LineStaffTools.STATE_ROLE_DEFAULT_PERMISSIONS_ROUTE}
            component={StateRoleDefaultPermissionsView}
          />
          <Route
            path={LineStaffTools.DEMO_APP_MANAGEMENT_ROUTE}
            component={DemoAppManagementView}
          />
          <Route
            path={[Lineage.LINEAGE_DETAIL, Lineage.LINEAGE_BASE]}
            component={LineageDisplay}
          />
          <Route
            path={OnCall.ON_CALL_BASE_ROUTE}
            component={OnCallLogsReview}
          />
          <StoreProvider>
            <Switch>
              <Route
                path={`${LineStaffTools.INSIGHTS_CONFIGURATION_ROUTE}/:stateCode/configurations/:configId`}
                component={InsightsConfigurationView}
              />
              <Route
                path={`${LineStaffTools.INSIGHTS_CONFIGURATION_ROUTE}/:stateCode?`}
                component={InsightsConfigurationsView}
              />
            </Switch>
            <Route
              path={[
                `${LineStaffTools.WORKFLOWS_OPPORTUNITIES_ROUTE}/:opportunityType?/configurations/:configId?`,
                `${LineStaffTools.WORKFLOWS_OPPORTUNITIES_ROUTE}/:opportunityType?`,
              ]}
              component={OpportunitiesHeader}
            />
            <Switch>
              <Route
                path={`${LineStaffTools.WORKFLOWS_OPPORTUNITIES_ROUTE}/:opportunityType/configurations/new`}
                component={OpportunityConfigurationForm}
              />
              <Route
                path={`${LineStaffTools.WORKFLOWS_OPPORTUNITIES_ROUTE}/:opportunityType/configurations/:configId`}
                component={OpportunityConfigurationView}
              />
              <Route
                path={`${LineStaffTools.WORKFLOWS_OPPORTUNITIES_ROUTE}/:opportunityType`}
                component={OpportunityView}
              />
              <Route
                path={LineStaffTools.WORKFLOWS_OPPORTUNITIES_ROUTE}
                component={OpportunitiesView}
              />
            </Switch>
          </StoreProvider>
        </Switch>
      </div>
    </Layout>
  );
};

function selectedMenuKeys(pathname: string): string[] {
  if (
    pathname.startsWith(
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.INGEST)
    )
  ) {
    return [DatasetMetadata.routeForMetadataDataset(MetadataDataset.INGEST)];
  }
  if (
    pathname.startsWith(
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.VALIDATION)
    )
  ) {
    return [
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.VALIDATION),
    ];
  }
  if (pathname.startsWith(DatasetMetadata.VALIDATION_STATUS_ROUTE)) {
    return [DatasetMetadata.VALIDATION_STATUS_ROUTE];
  }
  if (pathname.startsWith(LineStaffTools.GCS_CSV_TO_CLOUD_SQL_ROUTE)) {
    return [LineStaffTools.GCS_CSV_TO_CLOUD_SQL_ROUTE];
  }
  if (pathname.startsWith(LineStaffTools.CLOUD_SQL_TO_GCS_CSV_ROUTE)) {
    return [LineStaffTools.CLOUD_SQL_TO_GCS_CSV_ROUTE];
  }
  if (pathname.startsWith(LineStaffTools.PO_FEEDBACK_ROUTE)) {
    return [LineStaffTools.PO_FEEDBACK_ROUTE];
  }
  if (pathname.startsWith(LineStaffTools.EMAIL_REPORTS_ROUTE)) {
    return [LineStaffTools.EMAIL_REPORTS_ROUTE];
  }
  return [];
}

export default App;

function getEnvironmentSegmentedLabelOption(
  env: EnvironmentType,
  pathname: string
): SegmentedLabeledOption {
  const environmentOption = ENVIRONMENT_OPTIONS.get(env);
  return {
    value: environmentOption?.baseUrl.concat(pathname) || "",
    label: environmentOption?.title,
  };
}

function getEnvLinkOptions(pathname: string): SegmentedLabeledOption[] {
  const env = window.RUNTIME_GCP_ENVIRONMENT; // production, staging, development
  if (env === "staging" || env === "production") {
    return [
      getEnvironmentSegmentedLabelOption("staging", pathname),
      getEnvironmentSegmentedLabelOption("production", pathname),
    ];
  }

  return [
    getEnvironmentSegmentedLabelOption("development", pathname),
    getEnvironmentSegmentedLabelOption("staging", pathname),
  ];
}
