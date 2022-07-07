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
import { Layout, Menu, Typography } from "antd";
import { Link, Redirect, Route, Switch, useLocation } from "react-router-dom";
import MetadataDataset from "../models/MetadataDatasets";
import * as DatasetMetadata from "../navigation/DatasetMetadata";
import * as IngestOperations from "../navigation/IngestOperations";
import * as JusticeCountsTools from "../navigation/JusticeCountsTools";
import * as LineStaffTools from "../navigation/LineStaffTools";
import "../style/App.css";
import AgencyProvisioningView from "./JusticeCountsTools/AgencyProvisioningView";
import UserProvisioningView from "./JusticeCountsTools/UserProvisioningView";
import CloudSQLExportView from "./CloudSQLExportView";
import CloudSQLImportView from "./CloudSQLImportView";
import ColumnView from "./ColumnView";
import DataFreshnessView from "./DataFreshnessView";
import DatasetView from "./DatasetView";
import DirectSandboxRawImport from "./DirectSandboxRawImportView";
import FlashDatabaseChecklist from "./FlashDatabaseChecklist";
import IngestOperationsView from "./IngestOperationsView";
import Nelly from "../favicon-32x32.png";
import POEmailsView from "./POEmailsView";
import POFeedbackView from "./POFeedbackView";
import TableView from "./TableView";
import UploadRawFilesView from "./UploadRawFilesView";
import UploadRostersView from "./UploadRostersView";
import ValidationDetailView from "./Validation/ValidationDetailView";
import ValidationStatusView from "./Validation/ValidationStatusView";
import StateUserPermissionsView from "./StateUserPermissionsView";

const App = (): JSX.Element => {
  const location = useLocation();
  const title = window.RUNTIME_GCP_ENVIRONMENT || "unknown env";

  return (
    <Layout style={{ height: "100%" }}>
      <Layout.Sider>
        <Typography.Title level={4} style={{ margin: 23 }}>
          {title.toUpperCase()}
          <img src={Nelly} id="adminPanelNelly" alt="Nelly" />
        </Typography.Title>
        <Menu mode="inline" selectedKeys={selectedMenuKeys(location.pathname)}>
          <Menu.ItemGroup title="Ingest Metadata">
            <Menu.Item
              key={DatasetMetadata.routeForMetadataDataset(
                MetadataDataset.INGEST
              )}
            >
              <Link
                to={DatasetMetadata.routeForMetadataDataset(
                  MetadataDataset.INGEST
                )}
              >
                State Dataset
              </Link>
            </Menu.Item>
            <Menu.Item key={DatasetMetadata.DATA_FRESHNESS_ROUTE}>
              <Link to={DatasetMetadata.DATA_FRESHNESS_ROUTE}>
                Data Freshness
              </Link>
            </Menu.Item>
          </Menu.ItemGroup>
          <Menu.ItemGroup title="Validation Metadata">
            <Menu.Item key={DatasetMetadata.VALIDATION_STATUS_ROUTE}>
              <Link to={DatasetMetadata.VALIDATION_STATUS_ROUTE}>
                Validation Status
              </Link>
            </Menu.Item>
            <Menu.Item
              key={DatasetMetadata.routeForMetadataDataset(
                MetadataDataset.VALIDATION
              )}
            >
              <Link
                to={DatasetMetadata.routeForMetadataDataset(
                  MetadataDataset.VALIDATION
                )}
              >
                External Accuracy Dataset
              </Link>
            </Menu.Item>
          </Menu.ItemGroup>
          <Menu.ItemGroup title="Ingest Operations">
            <Menu.Item key={IngestOperations.INGEST_ACTIONS_ROUTE}>
              <Link to={IngestOperations.INGEST_ACTIONS_ROUTE}>
                Actions &amp; Summaries
              </Link>
            </Menu.Item>
            <Menu.Item key={IngestOperations.DIRECT_SANDBOX_RAW_IMPORT}>
              <Link to={IngestOperations.DIRECT_SANDBOX_RAW_IMPORT}>
                Sandbox Raw Data Import
              </Link>
            </Menu.Item>
            <Menu.Item key={IngestOperations.FLASH_DB_CHECKLIST_ROUTE}>
              <Link to={IngestOperations.FLASH_DB_CHECKLIST_ROUTE}>
                Flash Database
              </Link>
            </Menu.Item>
          </Menu.ItemGroup>
          <Menu.ItemGroup title="Line Staff Tools">
            <Menu.Item key={LineStaffTools.GCS_CSV_TO_CLOUD_SQL_ROUTE}>
              <Link to={LineStaffTools.GCS_CSV_TO_CLOUD_SQL_ROUTE}>
                GCS &rarr; Cloud SQL
              </Link>
            </Menu.Item>
            <Menu.Item key={LineStaffTools.CLOUD_SQL_TO_GCS_CSV_ROUTE}>
              <Link to={LineStaffTools.CLOUD_SQL_TO_GCS_CSV_ROUTE}>
                Cloud SQL &rarr; GCS
              </Link>
            </Menu.Item>
            <Menu.Item key={LineStaffTools.PO_FEEDBACK_ROUTE}>
              <Link to={LineStaffTools.PO_FEEDBACK_ROUTE}>PO Feedback</Link>
            </Menu.Item>
            <Menu.Item key={LineStaffTools.EMAIL_REPORTS_ROUTE}>
              <Link to={LineStaffTools.EMAIL_REPORTS_ROUTE}>Email Reports</Link>
            </Menu.Item>
            <Menu.Item key={LineStaffTools.UPLOAD_ROSTERS_ROUTE}>
              <Link to={LineStaffTools.UPLOAD_ROSTERS_ROUTE}>
                Upload Rosters
              </Link>
            </Menu.Item>
            <Menu.Item key={LineStaffTools.UPLOAD_RAW_FILES_ROUTE}>
              <Link to={LineStaffTools.UPLOAD_RAW_FILES_ROUTE}>
                Upload Raw Files
              </Link>
            </Menu.Item>
            <Menu.Item key={LineStaffTools.STATE_USER_PERMISSIONS_ROUTE}>
              <Link to={LineStaffTools.STATE_USER_PERMISSIONS_ROUTE}>
                State User Permissions
              </Link>
            </Menu.Item>
          </Menu.ItemGroup>
          <Menu.ItemGroup title="Justice Counts Tools">
            <Menu.Item key={JusticeCountsTools.AGENCY_PROVISIONING_ROUTE}>
              <Link to={JusticeCountsTools.AGENCY_PROVISIONING_ROUTE}>
                Agency Provisioning
              </Link>
            </Menu.Item>
            <Menu.Item key={JusticeCountsTools.USER_PROVISIONING_ROUTE}>
              <Link to={JusticeCountsTools.USER_PROVISIONING_ROUTE}>
                User Provisioning
              </Link>
            </Menu.Item>
          </Menu.ItemGroup>
        </Menu>
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
          <Redirect
            from="/"
            to={DatasetMetadata.routeForMetadataDataset(MetadataDataset.INGEST)}
          />
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
