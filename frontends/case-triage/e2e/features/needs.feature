Feature: Needs
  I must log in to access client data

  Background:
    When I visit the home page

  Scenario: Helping find employment
    When I login as "admin"
    When I open "Cheri Maynard"'s case card from the "Up Next" list
    Then the "employment" need should say "Employment Needed"
    When I complete the "I helped them find employment" need action
    Then the "employment" need should have an "active" action "I helped them find employment"
    When I undo the active action inside the "employment" need
    Then the "employment" need should have an "inactive" action "I helped them find employment"

  Scenario: Scheduling a face to face contact
    When I open "Whitney Velez"'s case card from the "Up Next" list
    Then the "face to face contact" need should say "Face to Face Contact Needed"
    When I complete the "I scheduled our next face-to-face contact" need action
    Then the "face to face contact" need should have an "active" action "I scheduled our next face-to-face contact"
    When I undo the active action inside the "face to face contact" need
    Then the "face to face contact" need should have an "inactive" action "I scheduled our next face-to-face contact"

  Scenario: Completing a risk assessment
    When I open "Whitney Velez"'s case card from the "Up Next" list
    Then the "risk assessment" need should say "Risk Assessment Needed"
    When I complete the "I completed their risk assessment" need action
    Then the "risk assessment" need should have an "active" action "I completed their risk assessment"
    When I undo the active action inside the "risk assessment" need
    Then the "risk assessment" need should have an "inactive" action "I completed their risk assessment"

  Scenario: Submitting a correction on a met need
    When I open "Marie-jeanne Gray"'s case card from the "Up Next" list
    Then the "employment" need should say "Employer: Applied Materials, Inc."
    When I click to submit an "Incorrect employment status" correction to the "employment" need
    Then I should see a modal with text "Incorrect Employment Status"
    Then I fill in the "Tell us more (optional)." field with "They are currently unemployed"
    Then I click the "Report" button
    Then I should not see a modal
    Then the "employment" need should have an "active" action "Incorrect employment status"
    When I undo the active action inside the "employment" need
    Then the "employment" need should say "Submit a correction"

  Scenario: Submitting a correction on an unmet need
    When I open "Kaylyn Hoover"'s case card from the "Up Next" list
    Then the "risk assessment" need should say "Risk Assessment Needed"
    When I click to submit an "Incorrect assessment status" correction to the "risk assessment" need
    Then I should see a modal with text "Incorrect Assessment Status"
    Then I fill in the "Tell us more (optional)." field with "This does not match their last assessment score"
    Then I click the "Report" button
    Then the "risk assessment" need should say "Incorrect assessment status"
    When I undo the active action inside the "risk assessment" need
    Then the "risk assessment" need should say "I completed their risk assessment"

  Scenario: Submitting a correction on an unmet need
    When I open "Jessika Leonard"'s case card from the "Up Next" list
    Then the "face to face contact" need should say "Face to Face Contact Needed"
    When I click to submit an "Incorrect contact status" correction to the "face to face contact" need
    Then I should see a modal with text "Incorrect Contact Status"
    Then I fill in the "Tell us more (optional)." field with "I met with them on Tuesday"
    Then I click the "Report" button
    Then the "face to face contact" need should say "Incorrect contact status"
    When I undo the active action inside the "face to face contact" need
    Then the "face to face contact" need should say "I scheduled our next face-to-face contact"

  Scenario: Marking client as not on caseload
    When I open "Brandi Bradford"'s case card from the "Up Next" list
    When I click to submit an "Not on Caseload" correction to "Brandi Bradford"'s case
    Then I should see a modal with text "Not on Caseload"
    When I fill in the "After you click submit, we will move this person to the bottom of the list. Once processed, this person will be removed from your list." field with "They transferred to another officer"
    And I click the "Report" button
    Then I should not see a modal
    And I should see "Brandi Bradford" at the "top" of the "Processing Feedback" list

  Scenario: Marking client as in custody
    When I open "Jessika Leonard"'s case card from the "Up Next" list
    When I click to submit an "In Custody" correction to "Jessika Leonard"'s case
    Then I should see a modal with text "In Custody"
    When I fill in the "After you click submit, we will move this person to the bottom of the list. Once processed, this person will be removed from your list." field with "They are currently in jail"
    When I click the "Report" button
    Then I should not see a modal
    And I should see "Jessika Leonard" at the "bottom" of the "Up Next" list
    When I click the "Return to Caseload" button
    # Below the 3 top opportunity clients
    Then I should see "Jessika Leonard" at the "4th spot" of the "Up Next" list
