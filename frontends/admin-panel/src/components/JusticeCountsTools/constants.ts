export type Agency = {
  id: number;
  name: string;
};

export type CreateAgencyRequest = {
  name: string;
};

export type AgenciesResponse = {
  agencies: Agency[];
};

export type CreateAgencyResponse = {
  agency: Agency;
};

export type ErrorResponse = {
  error: string;
};

/* eslint-disable camelcase */
export type User = {
  id: number;
  email_address: string;
  agencies: Agency[];
  name?: string;
  auth0_user_id?: string;
};
/* eslint-enable camelcase */

export type CreateUserRequest = {
  email: string;
  agencyId: number;
  name?: string;
};

export type UsersResponse = {
  users: User[];
};

export type CreateUserResponse = {
  user: User;
};
