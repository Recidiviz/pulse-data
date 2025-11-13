## Description of the change

> Description here

## Type of change

> All pull requests must have at least one of the following labels applied (otherwise the PR will fail):

| Label                       	| Description                                                                                               	|
|-----------------------------	|-----------------------------------------------------------------------------------------------------------	|
| Type: Bug                   	| non-breaking change that fixes an issue                                                                   	|
| Type: Feature               	| non-breaking change that adds functionality                                                               	|
| Type: Breaking Change       	| fix or feature that would cause existing functionality to not work as expected                            	|
| Type: Non-breaking refactor 	| change addresses some tech debt item or prepares for a later change, but does not change functionality    	|
| Type: Configuration Change  	| adjusts configuration to achieve some end related to functionality, development, performance, or security 	|
| Type: Dependency Upgrade      | upgrades a project dependency - these changes are not included in release notes                             |

## Related issues

Closes #XXXX

## Checklists

### Development

**This box MUST be checked by the submitter prior to merging**:
- [ ] **Double- and triple-checked that there is no Personally Identifiable Information (PII) being mistakenly added in this pull request**

These boxes should be checked by the submitter prior to merging:
- [ ] Tests have been written to cover the code changed/added as part of this pull request  

  
   
### ⚠️ Changing an opportunity record?
Please [consult this document](https://www.notion.so/recidiviz/How-to-make-a-change-to-the-Opportunity-Record-1d67889f4d19807f8109f9f2f007dde3) first to see if additional changes might be required in the admin panel or a Zod schema. You can confirm with Polaris if unsure.

### Code review

These boxes should be checked by reviewers prior to merging:

- [ ] This pull request has a descriptive title and information useful to a reviewer
- [ ] Potential security implications or infrastructural changes have been considered, if relevant
