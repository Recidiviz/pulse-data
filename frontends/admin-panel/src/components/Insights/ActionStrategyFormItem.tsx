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

import { Form, Input } from "antd";
import { observer } from "mobx-react-lite";
import { useState } from "react";
import styled from "styled-components/macro";

const MarkdownContainer = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
`;

const MarkdownInput = styled.div`
  width: 48%;
`;

const Title = styled.h4`
  padding-top: 10px;
`;

type ActionStrategyCopy = [string, { prompt: string; body: string }];

const ActionStrategyFormItem = ({
  data,
}: {
  data: ActionStrategyCopy;
}): JSX.Element => {
  const [field, values] = data;
  const [prompt, setPrompt] = useState(values.prompt);
  const [body, setBody] = useState(values.body);

  const onInputChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    const { name, value } = event.target;
    if (name === "prompt") {
      setPrompt(value);
    } else if (name === "body") {
      setBody(value);
    }
  };

  return (
    <>
      <Title>{field}</Title>
      <Form.Item
        name={["actionStrategyCopy", field, "prompt"]}
        label="Prompt"
        rules={[
          {
            required: true,
            message: `Please input the string for how to display the ${field} prompt copy`,
          },
        ]}
        labelCol={{ span: 24 }}
      >
        <MarkdownContainer>
          <MarkdownInput>
            <Input.TextArea
              autoSize={{ minRows: 1, maxRows: 5 }}
              name="prompt"
              value={prompt}
              onChange={onInputChange}
              // so that user can select text within the input, otherwise it drags the modal
              style={{ userSelect: "text" }}
            />
          </MarkdownInput>
        </MarkdownContainer>
      </Form.Item>
      <Form.Item
        name={["actionStrategyCopy", field, "body"]}
        label="Body"
        rules={[
          {
            required: true,
            message: `Please input the string for how to display the ${field} body copy`,
          },
        ]}
        labelCol={{ span: 24 }}
      >
        <MarkdownContainer>
          <MarkdownInput>
            <Input.TextArea
              autoSize={{ minRows: 10, maxRows: 30 }}
              name="body"
              value={body}
              onChange={onInputChange}
              // so that user can select text within the input, otherwise it drags the modal
              style={{ userSelect: "text" }}
            />
          </MarkdownInput>
        </MarkdownContainer>
      </Form.Item>
    </>
  );
};

export default observer(ActionStrategyFormItem);
