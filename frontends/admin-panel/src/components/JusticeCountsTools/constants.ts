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

export type User = {
  id: number;
  email: string;
  name?: string;
};

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
