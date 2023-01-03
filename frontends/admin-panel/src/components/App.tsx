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
import { Avatar, Layout, Menu, MenuProps, Segmented, Typography } from "antd";
import { SegmentedLabeledOption, SegmentedValue } from "antd/lib/segmented";
import classNames from "classnames";
import {
  Redirect,
  Route,
  Switch,
  useHistory,
  useLocation,
} from "react-router-dom";
import Nelly from "../favicon-32x32.png";
import MetadataDataset from "../models/MetadataDatasets";
import * as DatasetMetadata from "../navigation/DatasetMetadata";
import * as IngestOperations from "../navigation/IngestOperations";
import * as JusticeCountsTools from "../navigation/JusticeCountsTools";
import * as LineStaffTools from "../navigation/LineStaffTools";
import "../style/App.css";
import CloudSQLExportView from "./CloudSQLExportView";
import CloudSQLImportView from "./CloudSQLImportView";
import DataFreshnessView from "./DataFreshnessView";
import DatasetView from "./Datasets/DatasetView";
import DirectSandboxRawImport from "./DirectSandboxRawImportView";
import FlashDatabaseChecklist from "./FlashDatabaseChecklist";
import IngestOperationsView from "./IngestOperationsView";
import AgencyProvisioningView from "./JusticeCountsTools/AgencyProvisioningView";
import UserProvisioningView from "./JusticeCountsTools/UserProvisioningView";
import POEmailsView from "./POEmailsView";
import POFeedbackView from "./POFeedbackView";
import StateRoleDefaultPermissionsView from "./StateUserPermissions/StateRolePermissionsView";
import StateUserPermissionsView from "./StateUserPermissions/StateUserPermissionsView";
import UploadLineStaffRostersView from "./UploadRostersView";
import UploadRawFilesView from "./UploadRawFilesView";
import ValidationStatusOverview from "./Validation/ValidationStatusOverview";

type MenuItem = Required<MenuProps>["items"][number];

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
}

const ENVIRONMENT_OPTIONS: Map<
  "development" | "staging" | "production",
  EnvironmentOption
> = new Map([
  [
    "development",
    {
      title: "Development",
      baseUrl: "http://localhost:3030",
    },
  ],
  [
    "staging",
    {
      title: "Staging",
      baseUrl: "https://recidiviz-staging.ue.r.appspot.com",
    },
  ],
  [
    "production",
    {
      title: "Production",
      baseUrl: "https://recidiviz-123.ue.r.appspot.com",
    },
  ],
]);

const items: MenuProps["items"] = [
  getItem("Ingest", "ingest_group", null, [
    getItem("Ingest Status", IngestOperations.INGEST_ACTIONS_ROUTE),
    getItem("Flash Databases", IngestOperations.FLASH_DB_CHECKLIST_ROUTE),
    getItem(
      "Sandbox Raw Data Import",
      IngestOperations.DIRECT_SANDBOX_RAW_IMPORT
    ),
    getItem("Data Freshness", DatasetMetadata.DATA_FRESHNESS_ROUTE),
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
    getItem("Upload Rosters", LineStaffTools.UPLOAD_ROSTERS_ROUTE),
    getItem("Upload Raw Files", LineStaffTools.UPLOAD_RAW_FILES_ROUTE),
    getItem(
      "State User Permissions",
      LineStaffTools.STATE_USER_PERMISSIONS_ROUTE
    ),
    getItem(
      "State Role Default Permissions",
      LineStaffTools.STATE_ROLE_DEFAULT_PERMISSIONS_ROUTE
    ),
  ]),
  getItem("Justice Counts", "justice_counts_group", null, [
    getItem(
      "Agency Provisioning",
      JusticeCountsTools.AGENCY_PROVISIONING_ROUTE
    ),
    getItem("User Provisioning", JusticeCountsTools.USER_PROVISIONING_ROUTE),
    getItem("Bulk Upload", JusticeCountsTools.BULK_UPLOAD_ROUTE),
  ]),
];

const App = (): JSX.Element => {
  const location = useLocation();
  const title = "Admin Panel";
  const history = useHistory();
  const onClick: MenuProps["onClick"] = (e) => {
    history.push(e.key);
  };

  const routeClass = classNames({
    "main-content": true,
    "main-content-padding": ![
      DatasetMetadata.VALIDATION_STATUS_ROUTE,
      IngestOperations.INGEST_ACTIONS_ROUTE,
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.VALIDATION),
      DatasetMetadata.routeForMetadataDataset(MetadataDataset.INGEST),
    ].filter((x) => history.location.pathname.includes(x)).length,
  });

  const onEnvironmentChange = (value: SegmentedValue) => {
    window.open(value.toString().concat(window.location.search), "_blank");
  };

  return (
    <Layout style={{ height: "100%" }}>
      <Layout.Sider width={256}>
        <div className="title-header">
          <Typography.Title level={3}>{title}</Typography.Title>
          <Avatar
            shape="square"
            style={{ backgroundColor: "bisque", padding: "2px" }}
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
          <Route
            path={DatasetMetadata.METADATA_DATASET_ROUTE_TEMPLATE}
            component={DatasetView}
          />
          <Route
            path={DatasetMetadata.DATA_FRESHNESS_ROUTE}
            component={DataFreshnessView}
          />
          <Route
            path={DatasetMetadata.VALIDATION_STATUS_ROUTE}
            component={ValidationStatusOverview}
          />
          <Route
            path={IngestOperations.INGEST_ACTIONS_ROUTE}
            component={IngestOperationsView}
          />
          <Route
            exact
            path={IngestOperations.DIRECT_SANDBOX_RAW_IMPORT}
            component={DirectSandboxRawImport}
          />
          <Route
            exact
            path={IngestOperations.FLASH_DB_CHECKLIST_ROUTE}
            component={FlashDatabaseChecklist}
          />
          <Route
            path={LineStaffTools.GCS_CSV_TO_CLOUD_SQL_ROUTE}
            component={CloudSQLImportView}
          />
          <Route
            path={LineStaffTools.CLOUD_SQL_TO_GCS_CSV_ROUTE}
            component={CloudSQLExportView}
          />
          <Route
            path={LineStaffTools.PO_FEEDBACK_ROUTE}
            component={POFeedbackView}
          />
          <Route
            path={LineStaffTools.EMAIL_REPORTS_ROUTE}
            component={POEmailsView}
          />
          <Route
            path={LineStaffTools.UPLOAD_ROSTERS_ROUTE}
            component={UploadLineStaffRostersView}
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
            path={JusticeCountsTools.AGENCY_PROVISIONING_ROUTE}
            component={AgencyProvisioningView}
          />
          <Route
            path={JusticeCountsTools.USER_PROVISIONING_ROUTE}
            component={UserProvisioningView}
          />
          <Redirect from="/" to={IngestOperations.INGEST_ACTIONS_ROUTE} />
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
  if (pathname.startsWith(DatasetMetadata.DATA_FRESHNESS_ROUTE)) {
    return [DatasetMetadata.DATA_FRESHNESS_ROUTE];
  }
  if (pathname.startsWith(DatasetMetadata.VALIDATION_STATUS_ROUTE)) {
    return [DatasetMetadata.VALIDATION_STATUS_ROUTE];
  }
  if (pathname.startsWith(IngestOperations.INGEST_ACTIONS_ROUTE)) {
    return [IngestOperations.INGEST_ACTIONS_ROUTE];
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
  env: "production" | "staging" | "development",
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
