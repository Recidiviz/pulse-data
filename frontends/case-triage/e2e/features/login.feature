Feature: Login with Auth0
    I must log in to access client data

    Scenario: Logging in as admin
        Given I am on the login page
        When I login as "admin"
        Then I should see my client list
