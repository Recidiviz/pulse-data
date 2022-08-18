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
import { Layout, Menu, MenuProps, Typography } from "antd";
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
import ColumnView from "./ColumnView";
import DataFreshnessView from "./DataFreshnessView";
import DatasetView from "./DatasetView";
import DirectSandboxRawImport from "./DirectSandboxRawImportView";
import FlashDatabaseChecklist from "./FlashDatabaseChecklist";
import IngestOperationsView from "./IngestOperationsView";
import AgencyProvisioningView from "./JusticeCountsTools/AgencyProvisioningView";
import JusticeCountsBulkUploadView from "./JusticeCountsTools/BulkUpload";
import UserProvisioningView from "./JusticeCountsTools/UserProvisioningView";
import POEmailsView from "./POEmailsView";
import POFeedbackView from "./POFeedbackView";
import StateUserPermissionsView from "./StateUserPermissionsView";
import TableView from "./TableView";
import UploadRawFilesView from "./UploadRawFilesView";
import UploadRostersView from "./UploadRostersView";
import ValidationDetailView from "./Validation/ValidationDetailView";
import ValidationStatusView from "./Validation/ValidationStatusView";

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
  const title = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";
  const history = useHistory();

  const onClick: MenuProps["onClick"] = (e) => {
    history.push(e.key);
  };

  return (
    <Layout style={{ height: "100%" }}>
      <Layout.Sider width={256}>
        <Typography.Title level={4} style={{ margin: 23 }}>
          {title.toUpperCase()}
          <img src={Nelly} id="adminPanelNelly" alt="Nelly" />
        </Typography.Title>
        <Menu
          onClick={onClick}
          mode="inline"
          selectedKeys={selectedMenuKeys(location.pathname)}
          items={items}
        />
      </Layout.Sider>
      <div className="main-content">
        <Switch>
          <Route
            path={DatasetMetadata.METADATA_COLUMN_ROUTE_TEMPLATE}
            component={ColumnView}
          />
          <Route
            path={DatasetMetadata.METADATA_TABLE_ROUTE_TEMPLATE}
            component={TableView}
          />
          <Route
            path={DatasetMetadata.METADATA_DATASET_ROUTE_TEMPLATE}
            component={DatasetView}
          />
          <Route
            path={DatasetMetadata.DATA_FRESHNESS_ROUTE}
            component={DataFreshnessView}
          />
          <Route
            path={DatasetMetadata.VALIDATION_DETAIL_ROUTE_TEMPLATE}
            component={ValidationDetailView}
          />
          <Route
            path={DatasetMetadata.VALIDATION_STATUS_ROUTE}
            component={ValidationStatusView}
          />
          <Route
            exact
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
            component={UploadRostersView}
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
            path={JusticeCountsTools.AGENCY_PROVISIONING_ROUTE}
            component={AgencyProvisioningView}
          />
          <Route
            path={JusticeCountsTools.USER_PROVISIONING_ROUTE}
            component={UserProvisioningView}
          />
          <Route
            path={JusticeCountsTools.BULK_UPLOAD_ROUTE}
            component={JusticeCountsBulkUploadView}
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
