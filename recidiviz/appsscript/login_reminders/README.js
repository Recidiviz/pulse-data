/* 

Hello! Before you send any email reminders, please send yourself test emails 
by running the functions sendTestLinestaffEmails and sendTestSupervisorEmails
within TestEmailReminders.gs. Review the wording of the emails in your inbox.

If there are any changes, please request for an engineer to make the change
by posting in #email_reminder_rollouts. Here are the locations of text:
- State-specific wording is in EmailReminderHelpers.gs -- search for stateSpecificText
- General wording is at the top of SendLinestaffEmailReminders.gs and SendSupervisorEmailReminders.gs

Once the test emails look good, you can send emails by running the code in
SendLinestaffEmailReminders.gs and SendSupervisorEmailReminders.gs!
Logs of emails sent will be written to the associated spreadsheet.

Common errors and how to deal with them:

- "Invalid argument: email-reports@recidiviz.org": This error means this script is
  being run from a Google account that doesn't have this email address set up as an
  alternate email. There are two ways to address this issue. If you have trouble,
  please post in #it.
  1. Log into the apps-script@recidiviz.org account, which has the alias set up.
  The password and two-factor auth code are both in the shared Recidiviz Employees
  1password vault.
  OR
  2. Set up email-reports@recidiviz.org as an alternate email in your personal @recidiviz.org
  account. Add it in your gmail settings based on https://support.google.com/a/answer/33327
  Then check https://groups.google.com/a/recidiviz.org/g/email-reports for the verification email.

- "Gmail operation not allowed": This error happens randomly sometimes. You can safely
  rerun the script, and it will skip everyone who has already been emailed.

If you have any questions or concerns, please reach out in the #email_reminder_rollouts
Slack channel.

*/
