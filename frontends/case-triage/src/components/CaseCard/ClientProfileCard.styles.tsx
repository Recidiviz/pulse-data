import styled from "styled-components/macro";
import { rem } from "polished";
import {
  H3,
  Icon,
  palette,
  spacing,
  TabList,
  TabPanel,
  Tabs,
} from "@recidiviz/design-system";

export const ClientName = styled(H3)`
  margin-top: 0;
  margin-right: auto;
  margin-bottom: 0;
`;

export const DetailsLineItem = styled.div`
  margin-bottom: ${rem(spacing.xs)};
`;

export const SummaryLineItem = styled(DetailsLineItem)`
  display: -ms-grid;
  display: grid;
  -ms-grid-columns: ${rem(24)} 1fr;
  grid-template-columns: ${rem(24)} 1fr;
`;

export const SummaryIcon = styled(Icon).attrs({
  color: palette.slate30,
  size: 16,
})`
  -ms-grid-column: 1;
  grid-column: 1;
  /* slight vertical offset to approximate baseline alignment */
  margin-top: ${rem(2)};
`;

export const Summary = styled.div`
  font-size: ${rem(14)};
  -ms-grid-column: 2;
  grid-column: 2;
`;

export const DetailsPanelHeading = styled.div`
  color: ${palette.pine2};
  font-size: ${rem(16)};
  font-weight: 500;
`;

export const DetailsPanelSection = styled.article`
  font-size: ${rem(14)};
  padding: ${rem(spacing.lg)} ${rem(spacing.xl)};
  border-bottom: 1px solid ${palette.slate10};

  &:last-of-type {
    border-bottom-width: 0;
  }
`;

export const DetailsLabel = styled.span`
  color: ${palette.slate80};
  margin-right: ${rem(spacing.xs)};
`;

export const ClientInfo = styled.div`
  font-size: ${rem(14)};
  width: 100%;
`;

export const ClientProfileWrapper = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
  justify-content: stretch;
  overflow: hidden;
`;

export const ClientProfileTabPanel = styled(TabPanel).attrs({})`
  &&.TabPanel--selected {
    padding: 0;
  }
`;

export const ClientProfileTabs = styled(Tabs)`
  display: -ms-grid;
  display: grid;
  flex: 1 1 auto;
  -ms-grid-columns: 1fr;
  grid-template-columns: 1fr;
  -ms-grid-rows: auto minmax(0, 1fr);
  grid-template-rows: auto minmax(0, 1fr);
  height: 100%;
  /* need a non-auto min-height to contain within parent */
  min-height: 0;
  width: 100%;

  ${TabList} {
    -ms-grid-column: 1;
    grid-column: 1;
    -ms-grid-row: 1;
    grid-row: 1;
    width: 100%;
  }

  ${ClientProfileTabPanel} {
    -ms-grid-column: 1;
    grid-column: 1;
    -ms-grid-row: 2;
    grid-row: 2;
    width: 100%;
  }
`;

export const ClientProfileHeading = styled.div`
  display: flex;
  flex-wrap: wrap;
  padding: ${rem(spacing.xl)};
  padding-bottom: ${rem(spacing.md)};
  border-bottom: none;
`;
