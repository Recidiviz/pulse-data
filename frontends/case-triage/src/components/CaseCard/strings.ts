import {
  parseContactFrequency,
  SupervisionContactFrequency,
} from "../../stores/PolicyStore/Policy";
import { Client } from "../../stores/ClientsStore";

export const getContactFrequencyText = (
  contactFrequency: SupervisionContactFrequency | undefined
): string | null => {
  if (!contactFrequency) {
    return null;
  }

  const [contacts, days] = parseContactFrequency(contactFrequency);
  return `Policy: ${contacts} every ${days}`;
};

export const getLastContactedText = (client: Client): string => {
  const { mostRecentFaceToFaceDate } = client;

  if (mostRecentFaceToFaceDate) {
    return `Last contacted on ${mostRecentFaceToFaceDate.format(
      "MMMM Do, YYYY"
    )}`;
  }
  return `No contact on file`;
};
