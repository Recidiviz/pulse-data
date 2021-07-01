import { SupervisionContactFrequency } from "../../stores/PolicyStore/Policy";
import { Client } from "../../stores/ClientsStore";

export const getContactFrequencyText = (
  contactFrequency: SupervisionContactFrequency | undefined,
  singularUnit: string
): string | null => {
  if (!contactFrequency) {
    return null;
  }

  const [contacts, days] = contactFrequency;
  const pluralized = contacts === 1 ? "" : "s";
  const daysPluralized = days === 1 ? "day" : "days";
  return `Policy: ${contacts} ${singularUnit}${pluralized} every ${days} ${daysPluralized}`;
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
