import * as React from "react";
import { Checkbox, Empty, Table } from "antd";
import { CheckboxChangeEvent } from "antd/es/checkbox";
import { ColumnsType } from "antd/es/table";
import { Link } from "react-router-dom";

// const emptyCell = <div className="center">N/A</div>;

const stateColumnsForBreakdown = (
  states: string[],
  nonplaceholdersOnly: boolean
): ColumnsType<MetadataRecord> => {
  return states.map((s) => {
    return {
      title: s,
      key: s,
      render: (_: string, record: MetadataRecord) => {
        const countedRecord = record.resultsByState[s];
        if (countedRecord === undefined) {
          return <div className="center">N/A</div>;
        }
        const count = nonplaceholdersOnly
          ? countedRecord.totalCount - countedRecord.placeholderCount
          : countedRecord.totalCount;
        if (count === 0) {
          return <div className="center">N/A</div>;
        }
        return <div className="success">{count.toLocaleString()}</div>;
      },
    };
  });
};

interface MetadataTableProps {
  initialColumnTitle: string;
  initialColumnLink?: (name: string) => string;
  data: MetadataAPIResult | undefined;
}

const MetadataTable = (props: MetadataTableProps): JSX.Element => {
  const { data, initialColumnLink, initialColumnTitle } = props;
  const [nonplaceholdersOnly, setNonplaceholdersOnly] = React.useState<boolean>(
    true
  );

  if (data === undefined) {
    return <Empty className="buffer" />;
  }
  const metadataRecords = Object.keys(data)
    .sort()
    .map(
      (name: string): MetadataRecord => {
        return {
          name,
          resultsByState: data[name],
        };
      }
    );
  if (metadataRecords.length === 0) {
    return <Empty className="buffer" />;
  }

  const statesMap: { [state: string]: true } = {};
  metadataRecords.forEach((record) => {
    Object.keys(record.resultsByState).forEach((stateCode) => {
      statesMap[stateCode] = true;
    });
  });
  const states = Object.keys(statesMap).sort();

  const initialColumns: ColumnsType<MetadataRecord> = [
    {
      title: initialColumnTitle,
      key: "name",
      fixed: "left",
      render: (_: string, record: MetadataRecord) => {
        if (initialColumnLink === undefined) {
          return record.name;
        }
        return (
          <div>
            <Link to={initialColumnLink(record.name)}>{record.name}</Link>
          </div>
        );
      },
    },
  ];
  const columns = initialColumns.concat(
    stateColumnsForBreakdown(states, nonplaceholdersOnly)
  );

  const onChange = (e: CheckboxChangeEvent) => {
    setNonplaceholdersOnly(e.target.checked);
  };

  return (
    <>
      <Checkbox
        className="buffer"
        onChange={onChange}
        checked={nonplaceholdersOnly}
      >
        Non-placeholders only
      </Checkbox>
      <div>
        <Table
          className="metadata-table"
          dataSource={metadataRecords}
          columns={columns}
          pagination={{
            hideOnSinglePage: true,
            showSizeChanger: true,
            pageSize: 50,
            size: "small",
          }}
          rowClassName="metadata-table-row"
          rowKey="name"
        />
      </div>
    </>
  );
};

export default MetadataTable;
