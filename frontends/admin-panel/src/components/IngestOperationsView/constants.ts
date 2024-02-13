export const ANCHOR_INGEST_VIEWS = "ingest_views";
export type IngestInstanceDataflowEnabledStatusResponse = {
  [stateCode: string]: {
    primary: boolean;
    secondary: boolean;
  };
};

export type IngestViewSummaries = {
  ingestViewMaterializationSummaries: IngestViewMaterializationSummary[];
  ingestViewContentsSummaries: IngestViewContentsSummary[];
};

export type IngestViewMaterializationSummary = {
  ingestViewName: string;
  numPendingJobs: number;
  numCompletedJobs: number;
  completedJobsMaxDatetime: string | null;
  pendingJobsMinDatetime: string | null;
};

export type IngestViewContentsSummary = {
  ingestViewName: string;
  numUnprocessedRows: number;
  numProcessedRows: number;
  unprocessedRowsMinDatetime: string | null;
  processedRowsMaxDatetime: string | null;
};

export type IngestInstanceStatusTableInfo = {
  stateCode: string;
  primary: string;
  secondary: string;
  queueInfo: string | undefined;
  dataflowEnabledPrimary: boolean | undefined;
  dataflowEnabledSecondary: boolean | undefined;
  timestampPrimary: string;
  timestampSecondary: string;
};
