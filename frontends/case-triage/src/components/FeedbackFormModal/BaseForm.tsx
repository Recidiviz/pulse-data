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
import { H3, Icon, IconSVG } from "@recidiviz/design-system";
import * as React from "react";
import * as Yup from "yup";
import { FormikBag, FormikProps, withFormik } from "formik";
import CaseUpdatesStore, {
  CaseUpdateActionType,
} from "../../stores/CaseUpdatesStore";
import { DecoratedClient } from "../../stores/ClientsStore";
import {
  Description,
  Input,
  SubmitContainer,
  SubmitButton,
  CancelButton,
  ExtraPaddedForm,
  CloseButton,
  PaddedForm,
} from "./FeedbackForm.styles";

interface BaseFormProps {
  // eslint-disable-next-line react/no-unused-prop-types
  caseUpdatesStore: CaseUpdatesStore;
  // eslint-disable-next-line react/no-unused-prop-types
  client: DecoratedClient;
  // eslint-disable-next-line react/no-unused-prop-types
  actionType: CaseUpdateActionType;

  commentPlaceholder: string;
  description: string;
  title: string;
  header?: React.ReactNode;

  onCancel: (event?: React.MouseEvent<Element, MouseEvent>) => void;
}

const FeedbackFormSchema = Yup.object().shape({
  comment: Yup.string(),
});

interface FeedbackFormValues {
  comment: string;
}

const InnerForm = ({
  dirty,
  isValid,
  handleChange,
  onCancel,
  values,
  commentPlaceholder,
  description,
  title,
  header,
}: BaseFormProps & FormikProps<FeedbackFormValues>) => {
  const FormComponent = header ? PaddedForm : ExtraPaddedForm;
  return (
    <div>
      <CloseButton onClick={onCancel}>
        <Icon kind={IconSVG.Close} />
      </CloseButton>
      {header}
      <FormComponent>
        <H3>{title}</H3>
        <Description>{description}</Description>

        <Input
          name="comment"
          onChange={handleChange}
          value={values.comment}
          placeholder={commentPlaceholder}
        />

        <SubmitContainer>
          <SubmitButton type="submit" disabled={!(isValid && dirty)}>
            Submit
          </SubmitButton>
          <CancelButton onClick={onCancel}>Cancel</CancelButton>
        </SubmitContainer>
      </FormComponent>
    </div>
  );
};

const BaseFeedbackForm = withFormik<BaseFormProps, FeedbackFormValues>({
  mapPropsToValues: () => ({ comment: "" }),
  validationSchema: FeedbackFormSchema,
  handleSubmit: async (
    values,
    {
      props: { caseUpdatesStore, client, actionType, onCancel },
    }: FormikBag<BaseFormProps, FeedbackFormValues>
  ) => {
    await caseUpdatesStore.recordAction(client, actionType, values.comment);
    onCancel();
  },
})(InnerForm);

export default BaseFeedbackForm;
