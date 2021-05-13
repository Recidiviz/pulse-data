Feature: Login with Auth0
    I must log in to access client data

    Scenario: Not logged in
        When I visit the home page
        Then I should not see my client list

    Scenario: Logging in as admin
        Given I am on the login page
        When I login as "admin"
        Then I should see my client list
