/*
Recidiviz - a data platform for criminal justice reform
Copyright (C) 2021 Recidiviz, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
=============================================================================
*/
import {
  Button,
  Col,
  DatePicker,
  Divider,
  Form,
  Row,
  Select,
  Space,
} from "antd";
import moment from "moment";
import { MinusCircleOutlined, PlusOutlined } from "@ant-design/icons";
import * as React from "react";
import { useContext } from "react";
import { FormListFieldData, FormListOperation } from "antd/es/form/FormList";
import { observer } from "mobx-react-lite";
import { useRootStore } from "../../stores";
import { FormContext } from "../DataDiscoveryView";

const { Option } = Select;

const getDefaultCondition = () => ({
  column: null,
  operator: "in",
  values: [],
});
const getDefaultGroup = () => ({ conditions: [getDefaultCondition()] });

interface ConditionProps {
  field: FormListFieldData;
  operation: FormListOperation;
}

const Condition = observer(
  ({ field, operation }: ConditionProps): JSX.Element => {
    const { dataDiscoveryStore: store } = useRootStore();
    const { form } = useContext(FormContext);
    const values = form.getFieldsValue(true);

    return (
      <Row
        gutter={8}
        align="middle"
        className="data-discovery-filters__condition"
      >
        <Col span={10}>
          <Form.Item
            {...field}
            name={[field.name, "column"]}
            fieldKey={[field.fieldKey, "column"]}
            label="Columns"
            rules={[{ required: true }]}
            noStyle
          >
            <Select showSearch>
              {store
                .columns(values.raw_files, values.ingest_view)
                .map((value) => (
                  <Option value={value} key={value}>
                    {value}
                  </Option>
                ))}
            </Select>
          </Form.Item>
        </Col>
        <Col span={3}>
          <Form.Item
            {...field}
            label="Operator"
            name={[field.name, "operator"]}
            fieldKey={[field.fieldKey, "operator"]}
            noStyle
          >
            <Select>
              {["in", "not in"].map((operator) => (
                <Option value={operator} key={operator}>
                  {operator}
                </Option>
              ))}
            </Select>
          </Form.Item>
        </Col>
        <Col span={10}>
          <Form.Item
            {...field}
            name={[field.name, "values"]}
            fieldKey={[field.fieldKey, "values"]}
            label="Values"
            rules={[{ required: true }]}
            noStyle
          >
            <Select mode="tags" />
          </Form.Item>
        </Col>
        <Col span={1}>
          <MinusCircleOutlined onClick={() => operation.remove(field.name)} />
        </Col>
      </Row>
    );
  }
);

interface OrConditionGroupProps {
  conditions: FormListFieldData[];
  operation: FormListOperation;
}

const OrConditionGroup = ({ conditions, operation }: OrConditionGroupProps) => {
  const split = <Divider style={{ margin: "4px" }}>AND</Divider>;
  return (
    <>
      <Space split={split} direction="vertical">
        {conditions.map((condition) => (
          <Condition
            field={condition}
            operation={operation}
            key={condition.name}
          />
        ))}
      </Space>

      <Form.Item noStyle style={{ marginTop: "8px" }}>
        <Button
          onClick={() => operation.add(getDefaultCondition())}
          block
          style={{ marginTop: "16px" }}
        >
          <PlusOutlined /> Add AND condition
        </Button>
      </Form.Item>
    </>
  );
};

const DataDiscoveryFiltersView = (): JSX.Element => {
  return (
    <Space direction="vertical" split={<Divider />}>
      <Form.Item
        label="Date Range"
        help="This specifies the date range of when the ingest files were received / processed"
        name="dates"
        rules={[{ required: true }]}
      >
        <DatePicker.RangePicker
          disabledDate={(current) => current && current > moment().endOf("day")}
        />
      </Form.Item>
      <Form.Item label="Conditions" noStyle style={{ width: "100%" }}>
        <Form.List name="condition_groups">
          {(fields, { add, remove }) => {
            return (
              <>
                <Space
                  direction="vertical"
                  split={<Divider style={{ margin: "4px" }}>OR</Divider>}
                >
                  {fields.map((field) => (
                    <div
                      className="data-discovery-filters__condition-group"
                      key={field.name}
                    >
                      <Form.List name={[field.name, "conditions"]}>
                        {(conditions, operation) => (
                          <OrConditionGroup
                            conditions={conditions}
                            operation={operation}
                          />
                        )}
                      </Form.List>
                    </div>
                  ))}
                </Space>

                <Form.Item style={{ marginTop: "16px" }}>
                  <Button
                    block
                    onClick={() => add(getDefaultGroup())}
                    type="dashed"
                  >
                    <PlusOutlined /> Add OR condition group
                  </Button>
                </Form.Item>
              </>
            );
          }}
        </Form.List>
      </Form.Item>
    </Space>
  );
};

export default DataDiscoveryFiltersView;
