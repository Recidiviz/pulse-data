// package: recidiviz.admin_panel.models
// file: recidiviz/admin_panel/models/validation.proto

import * as jspb from "google-protobuf";
import * as google_protobuf_timestamp_pb from "google-protobuf/google/protobuf/timestamp_pb";
import * as google_protobuf_any_pb from "google-protobuf/google/protobuf/any_pb";

export class ComparisonValue extends jspb.Message {
  hasValue(): boolean;
  clearValue(): void;
  getValue(): number | undefined;
  setValue(value: number): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ComparisonValue.AsObject;
  static toObject(includeInstance: boolean, msg: ComparisonValue): ComparisonValue.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ComparisonValue, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ComparisonValue;
  static deserializeBinaryFromReader(message: ComparisonValue, reader: jspb.BinaryReader): ComparisonValue;
}

export namespace ComparisonValue {
  export type AsObject = {
    value?: number,
  }
}

export class ResultRow extends jspb.Message {
  clearLabelValuesList(): void;
  getLabelValuesList(): Array<string>;
  setLabelValuesList(value: Array<string>): void;
  addLabelValues(value: string, index?: number): string;

  clearComparisonValuesList(): void;
  getComparisonValuesList(): Array<ComparisonValue>;
  setComparisonValuesList(value: Array<ComparisonValue>): void;
  addComparisonValues(value?: ComparisonValue, index?: number): ComparisonValue;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ResultRow.AsObject;
  static toObject(includeInstance: boolean, msg: ResultRow): ResultRow.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ResultRow, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ResultRow;
  static deserializeBinaryFromReader(message: ResultRow, reader: jspb.BinaryReader): ResultRow;
}

export namespace ResultRow {
  export type AsObject = {
    labelValuesList: Array<string>,
    comparisonValuesList: Array<ComparisonValue.AsObject>,
  }
}

export class ExistenceValidationResultDetails extends jspb.Message {
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ExistenceValidationResultDetails.AsObject;
  static toObject(includeInstance: boolean, msg: ExistenceValidationResultDetails): ExistenceValidationResultDetails.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ExistenceValidationResultDetails, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ExistenceValidationResultDetails;
  static deserializeBinaryFromReader(message: ExistenceValidationResultDetails, reader: jspb.BinaryReader): ExistenceValidationResultDetails;
}

export namespace ExistenceValidationResultDetails {
  export type AsObject = {
  }
}

export class SamenessPerRowValidationResultDetails extends jspb.Message {
  clearFailedRowsList(): void;
  getFailedRowsList(): Array<SamenessPerRowValidationResultDetails.RowWithError>;
  setFailedRowsList(value: Array<SamenessPerRowValidationResultDetails.RowWithError>): void;
  addFailedRows(value?: SamenessPerRowValidationResultDetails.RowWithError, index?: number): SamenessPerRowValidationResultDetails.RowWithError;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): SamenessPerRowValidationResultDetails.AsObject;
  static toObject(includeInstance: boolean, msg: SamenessPerRowValidationResultDetails): SamenessPerRowValidationResultDetails.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: SamenessPerRowValidationResultDetails, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): SamenessPerRowValidationResultDetails;
  static deserializeBinaryFromReader(message: SamenessPerRowValidationResultDetails, reader: jspb.BinaryReader): SamenessPerRowValidationResultDetails;
}

export namespace SamenessPerRowValidationResultDetails {
  export type AsObject = {
    failedRowsList: Array<SamenessPerRowValidationResultDetails.RowWithError.AsObject>,
  }

  export class RowWithError extends jspb.Message {
    hasRow(): boolean;
    clearRow(): void;
    getRow(): ResultRow | undefined;
    setRow(value?: ResultRow): void;

    hasError(): boolean;
    clearError(): void;
    getError(): number | undefined;
    setError(value: number): void;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): RowWithError.AsObject;
    static toObject(includeInstance: boolean, msg: RowWithError): RowWithError.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: RowWithError, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): RowWithError;
    static deserializeBinaryFromReader(message: RowWithError, reader: jspb.BinaryReader): RowWithError;
  }

  export namespace RowWithError {
    export type AsObject = {
      row?: ResultRow.AsObject,
      error?: number,
    }
  }
}

export class SamenessPerViewValidationResultDetails extends jspb.Message {
  hasNumErrorRows(): boolean;
  clearNumErrorRows(): void;
  getNumErrorRows(): number | undefined;
  setNumErrorRows(value: number): void;

  hasTotalNumRows(): boolean;
  clearTotalNumRows(): void;
  getTotalNumRows(): number | undefined;
  setTotalNumRows(value: number): void;

  clearNonNullCountsPerColumnPerPartitionList(): void;
  getNonNullCountsPerColumnPerPartitionList(): Array<SamenessPerViewValidationResultDetails.PartitionCounts>;
  setNonNullCountsPerColumnPerPartitionList(value: Array<SamenessPerViewValidationResultDetails.PartitionCounts>): void;
  addNonNullCountsPerColumnPerPartition(value?: SamenessPerViewValidationResultDetails.PartitionCounts, index?: number): SamenessPerViewValidationResultDetails.PartitionCounts;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): SamenessPerViewValidationResultDetails.AsObject;
  static toObject(includeInstance: boolean, msg: SamenessPerViewValidationResultDetails): SamenessPerViewValidationResultDetails.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: SamenessPerViewValidationResultDetails, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): SamenessPerViewValidationResultDetails;
  static deserializeBinaryFromReader(message: SamenessPerViewValidationResultDetails, reader: jspb.BinaryReader): SamenessPerViewValidationResultDetails;
}

export namespace SamenessPerViewValidationResultDetails {
  export type AsObject = {
    numErrorRows?: number,
    totalNumRows?: number,
    nonNullCountsPerColumnPerPartitionList: Array<SamenessPerViewValidationResultDetails.PartitionCounts.AsObject>,
  }

  export class PartitionCounts extends jspb.Message {
    clearPartitionLabelsList(): void;
    getPartitionLabelsList(): Array<string>;
    setPartitionLabelsList(value: Array<string>): void;
    addPartitionLabels(value: string, index?: number): string;

    getColumnCountsMap(): jspb.Map<string, number>;
    clearColumnCountsMap(): void;
    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): PartitionCounts.AsObject;
    static toObject(includeInstance: boolean, msg: PartitionCounts): PartitionCounts.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: PartitionCounts, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): PartitionCounts;
    static deserializeBinaryFromReader(message: PartitionCounts, reader: jspb.BinaryReader): PartitionCounts;
  }

  export namespace PartitionCounts {
    export type AsObject = {
      partitionLabelsList: Array<string>,
      columnCountsMap: Array<[string, number]>,
    }
  }
}

export class ValidationStatusRecord extends jspb.Message {
  hasRunId(): boolean;
  clearRunId(): void;
  getRunId(): string | undefined;
  setRunId(value: string): void;

  hasRunDatetime(): boolean;
  clearRunDatetime(): void;
  getRunDatetime(): google_protobuf_timestamp_pb.Timestamp | undefined;
  setRunDatetime(value?: google_protobuf_timestamp_pb.Timestamp): void;

  hasSystemVersion(): boolean;
  clearSystemVersion(): void;
  getSystemVersion(): string | undefined;
  setSystemVersion(value: string): void;

  hasName(): boolean;
  clearName(): void;
  getName(): string | undefined;
  setName(value: string): void;

  hasCategory(): boolean;
  clearCategory(): void;
  getCategory(): ValidationStatusRecord.ValidationCategoryMap[keyof ValidationStatusRecord.ValidationCategoryMap] | undefined;
  setCategory(value: ValidationStatusRecord.ValidationCategoryMap[keyof ValidationStatusRecord.ValidationCategoryMap]): void;

  hasIsPercentage(): boolean;
  clearIsPercentage(): void;
  getIsPercentage(): boolean | undefined;
  setIsPercentage(value: boolean): void;

  hasStateCode(): boolean;
  clearStateCode(): void;
  getStateCode(): string | undefined;
  setStateCode(value: string): void;

  hasDidRun(): boolean;
  clearDidRun(): void;
  getDidRun(): boolean | undefined;
  setDidRun(value: boolean): void;

  hasHasData(): boolean;
  clearHasData(): void;
  getHasData(): boolean | undefined;
  setHasData(value: boolean): void;

  hasDevMode(): boolean;
  clearDevMode(): void;
  getDevMode(): boolean | undefined;
  setDevMode(value: boolean): void;

  hasHardFailureAmount(): boolean;
  clearHardFailureAmount(): void;
  getHardFailureAmount(): number | undefined;
  setHardFailureAmount(value: number): void;

  hasSoftFailureAmount(): boolean;
  clearSoftFailureAmount(): void;
  getSoftFailureAmount(): number | undefined;
  setSoftFailureAmount(value: number): void;

  hasResultStatus(): boolean;
  clearResultStatus(): void;
  getResultStatus(): ValidationStatusRecord.ValidationResultStatusMap[keyof ValidationStatusRecord.ValidationResultStatusMap] | undefined;
  setResultStatus(value: ValidationStatusRecord.ValidationResultStatusMap[keyof ValidationStatusRecord.ValidationResultStatusMap]): void;

  hasErrorAmount(): boolean;
  clearErrorAmount(): void;
  getErrorAmount(): number | undefined;
  setErrorAmount(value: number): void;

  hasFailureDescription(): boolean;
  clearFailureDescription(): void;
  getFailureDescription(): string | undefined;
  setFailureDescription(value: string): void;

  hasExistence(): boolean;
  clearExistence(): void;
  getExistence(): ExistenceValidationResultDetails | undefined;
  setExistence(value?: ExistenceValidationResultDetails): void;

  hasSamenessPerRow(): boolean;
  clearSamenessPerRow(): void;
  getSamenessPerRow(): SamenessPerRowValidationResultDetails | undefined;
  setSamenessPerRow(value?: SamenessPerRowValidationResultDetails): void;

  hasSamenessPerView(): boolean;
  clearSamenessPerView(): void;
  getSamenessPerView(): SamenessPerViewValidationResultDetails | undefined;
  setSamenessPerView(value?: SamenessPerViewValidationResultDetails): void;

  hasLastBetterStatusRunId(): boolean;
  clearLastBetterStatusRunId(): void;
  getLastBetterStatusRunId(): string | undefined;
  setLastBetterStatusRunId(value: string): void;

  hasLastBetterStatusRunDatetime(): boolean;
  clearLastBetterStatusRunDatetime(): void;
  getLastBetterStatusRunDatetime(): google_protobuf_timestamp_pb.Timestamp | undefined;
  setLastBetterStatusRunDatetime(value?: google_protobuf_timestamp_pb.Timestamp): void;

  hasLastBetterStatusRunResultStatus(): boolean;
  clearLastBetterStatusRunResultStatus(): void;
  getLastBetterStatusRunResultStatus(): ValidationStatusRecord.ValidationResultStatusMap[keyof ValidationStatusRecord.ValidationResultStatusMap] | undefined;
  setLastBetterStatusRunResultStatus(value: ValidationStatusRecord.ValidationResultStatusMap[keyof ValidationStatusRecord.ValidationResultStatusMap]): void;

  getResultDetailsCase(): ValidationStatusRecord.ResultDetailsCase;
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ValidationStatusRecord.AsObject;
  static toObject(includeInstance: boolean, msg: ValidationStatusRecord): ValidationStatusRecord.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ValidationStatusRecord, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ValidationStatusRecord;
  static deserializeBinaryFromReader(message: ValidationStatusRecord, reader: jspb.BinaryReader): ValidationStatusRecord;
}

export namespace ValidationStatusRecord {
  export type AsObject = {
    runId?: string,
    runDatetime?: google_protobuf_timestamp_pb.Timestamp.AsObject,
    systemVersion?: string,
    name?: string,
    category?: ValidationStatusRecord.ValidationCategoryMap[keyof ValidationStatusRecord.ValidationCategoryMap],
    isPercentage?: boolean,
    stateCode?: string,
    didRun?: boolean,
    hasData?: boolean,
    devMode?: boolean,
    hardFailureAmount?: number,
    softFailureAmount?: number,
    resultStatus?: ValidationStatusRecord.ValidationResultStatusMap[keyof ValidationStatusRecord.ValidationResultStatusMap],
    errorAmount?: number,
    failureDescription?: string,
    existence?: ExistenceValidationResultDetails.AsObject,
    samenessPerRow?: SamenessPerRowValidationResultDetails.AsObject,
    samenessPerView?: SamenessPerViewValidationResultDetails.AsObject,
    lastBetterStatusRunId?: string,
    lastBetterStatusRunDatetime?: google_protobuf_timestamp_pb.Timestamp.AsObject,
    lastBetterStatusRunResultStatus?: ValidationStatusRecord.ValidationResultStatusMap[keyof ValidationStatusRecord.ValidationResultStatusMap],
  }

  export interface ValidationCategoryMap {
    EXTERNAL_AGGREGATE: 0;
    EXTERNAL_INDIVIDUAL: 1;
    CONSISTENCY: 2;
    INVARIANT: 3;
    FRESHNESS: 4;
  }

  export const ValidationCategory: ValidationCategoryMap;

  export interface ValidationResultStatusMap {
    SUCCESS: 0;
    FAIL_SOFT: 1;
    FAIL_HARD: 2;
  }

  export const ValidationResultStatus: ValidationResultStatusMap;

  export enum ResultDetailsCase {
    RESULT_DETAILS_NOT_SET = 0,
    EXISTENCE = 15,
    SAMENESS_PER_ROW = 16,
    SAMENESS_PER_VIEW = 17,
  }
}

export class ValidationStatusRecords extends jspb.Message {
  clearRecordsList(): void;
  getRecordsList(): Array<ValidationStatusRecord>;
  setRecordsList(value: Array<ValidationStatusRecord>): void;
  addRecords(value?: ValidationStatusRecord, index?: number): ValidationStatusRecord;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ValidationStatusRecords.AsObject;
  static toObject(includeInstance: boolean, msg: ValidationStatusRecords): ValidationStatusRecords.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ValidationStatusRecords, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ValidationStatusRecords;
  static deserializeBinaryFromReader(message: ValidationStatusRecords, reader: jspb.BinaryReader): ValidationStatusRecords;
}

export namespace ValidationStatusRecords {
  export type AsObject = {
    recordsList: Array<ValidationStatusRecord.AsObject>,
  }
}

