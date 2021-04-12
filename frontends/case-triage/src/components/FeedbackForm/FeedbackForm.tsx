// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import styled from "styled-components/macro";
import {
  Button,
  fonts,
  H3,
  palette,
  spacing,
} from "@recidiviz/case-triage-components";
import { rem } from "polished";
import * as React from "react";
import * as Yup from "yup";
import { Form, FormikBag, FormikProps, withFormik } from "formik";
import Checkbox from "./Checkbox";
import CaseUpdatesStore, {
  CaseUpdateActionType,
} from "../../stores/CaseUpdatesStore";
import { DecoratedClient } from "../../stores/ClientsStore";

interface FeedbackFormProps {
  // eslint-disable-next-line react/no-unused-prop-types
  caseUpdatesStore: CaseUpdatesStore;
  // eslint-disable-next-line react/no-unused-prop-types
  client: DecoratedClient;
  onCancel: (event?: React.MouseEvent<Element, MouseEvent>) => void;
}

const Input = styled.textarea`
  background: ${palette.marble3};
  border: 1px solid ${palette.slate20};
  border-radius: 4px;
  color: ${palette.text.normal};
  font-size: ${rem("16px")};
  font-family ${fonts.body};
  padding: 16px;
  width: 100%;
  height: 117px;

  &::placeholder {
    color: ${palette.text.caption};
    font-family: ${fonts.body};
  }
`;

const Description = styled.div`
  font-size: ${rem("17px")};
  margin: ${rem(spacing.xl)} 0;
`;

const ReasonsContainer = styled.div`
  margin-top: ${rem(spacing.xl)};
  margin-bottom: ${rem(spacing.lg)};
`;

const Reason = styled.span`
  margin-bottom: ${rem(spacing.sm)};
  font-size: ${rem("16px")};
  color: ${palette.text.links};
`;

const SubmitContainer = styled.div`
  margin-top: ${rem(spacing.xl)};
  text-align: center;
`;

const SubmitButton = styled.button`
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  padding: 16px 32px;
  background: ${palette.signal.links};
  border: none;
  color: white;

  width: 262px;
  height: 48px;

  border-radius: 4px;

  margin: 0 auto;
  margin-bottom: 16px;

  font-size: ${rem("17px")};

  &:disabled {
    background: #e2e9e9;
    border: 1px solid #e2e9e9;
    color: ${palette.text.caption};
  }
`;

const CheckboxComponent = styled(Checkbox)`
  margin-bottom: ${rem(spacing.sm)};
`;

const CancelButton = styled(Button).attrs({
  kind: "link",
})`
  font-size: ${rem("17px")};
`;

const FeedbackFormSchema = Yup.object().shape({
  actions: Yup.array(Yup.string().oneOf(Object.keys(CaseUpdateActionType)))
    .required()
    .min(1),
  otherText: Yup.string(),
});

interface FeedbackFormValues {
  actions: CaseUpdateActionType[];
  otherText: string;
}

const InnerForm = ({
  dirty,
  isValid,
  handleChange,
  onCancel,
  values,
}: FeedbackFormProps & FormikProps<FeedbackFormValues>) => {
  return (
    <Form>
      <H3>Give Us Feedback</H3>
      <Description>
        After you click submit, we will move this item to the bottom of the
        list.
      </Description>

      <ReasonsContainer>
        <CheckboxComponent
          name="actions"
          value={CaseUpdateActionType.INFORMATION_DOESNT_MATCH_OMS}
        >
          <Reason>This information does not match CIS</Reason>
        </CheckboxComponent>
        <CheckboxComponent
          name="actions"
          value={CaseUpdateActionType.NOT_ON_CASELOAD}
        >
          <Reason>This person is not on my caseload</Reason>
        </CheckboxComponent>
        <CheckboxComponent
          name="actions"
          value={CaseUpdateActionType.FILED_REVOCATION_OR_VIOLATION}
        >
          <Reason>I filed a revocation or violation for this person</Reason>
        </CheckboxComponent>
        <CheckboxComponent
          name="actions"
          value={CaseUpdateActionType.OTHER_DISMISSAL}
        >
          <Reason>Other reason</Reason>
        </CheckboxComponent>
      </ReasonsContainer>

      <Input
        name="otherText"
        onChange={handleChange}
        value={values.otherText}
        placeholder="Tell us more"
      />

      <SubmitContainer>
        <SubmitButton type="submit" disabled={!(isValid && dirty)}>
          Submit
        </SubmitButton>
        <CancelButton onClick={onCancel}>Cancel</CancelButton>
      </SubmitContainer>
    </Form>
  );
};

const FeedbackForm = withFormik<FeedbackFormProps, FeedbackFormValues>({
  mapPropsToValues: () => ({ actions: [], otherText: "" }),
  validationSchema: FeedbackFormSchema,
  handleSubmit: (
    values,
    {
      props: { caseUpdatesStore, client, onCancel },
    }: FormikBag<FeedbackFormProps, FeedbackFormValues>
  ) => {
    caseUpdatesStore
      .submit(client, values.actions, values.otherText)
      .then(() => onCancel());

    return false;
  },
})(InnerForm);

export default FeedbackForm;
