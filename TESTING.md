## Tap Specific Caveats:
- Generic caveat 1
- Generic caveat 2

## Automated Test Coverage
- [ ] Retry logic
  - [ ] HTTP errors
  - [ ] Empty result set
- [ ] Exception handling

## Manual Testing

### Set up connector in Bytespree
- [ ] Verify Oauth success redirects back to Bytespree
- [ ] Verify Oauth failure (invalid credentials) works as expected, not breaking app flow
- [ ] Tap tests credentials for validity
- [ ] Tap returns tables appropriately
- [ ] Tap returns columns for table settings
- [ ] Secondary options are populated
- [ ] Verify conditional logic works within connector settings (will require looking @ definition.json)
- [ ] Make sure all fields that are required for authentication are, indeed, required
- [ ] Ensure all settings have appropriate description
- [ ] Ensure all settings have proper data type (e.g. a checkbox vs a textbox)
- [ ] "Known Limitations" is populated and spellechecked
- [ ] "Getting Started" is populated and spellechecked
- [ ] Make sure tap settings appear in a logical order (e.g. user before password or where conditional logic is applied)
- [ ] If tables aren't pre-populated, ensure user can add tables manually by typing them in
- [ ] (FUTURE) Sensitive information should be hidden by default
- [ ] (FUTURE) Test di_partner_integration_tables.minimum_sync_date works as expected (todo: find tap that uses this, maybe rzrs edge)
- [ ] (FUTURE) Ensure 15 minute & hourly sync is disabled if tap is full table replace

### Test Method
- [ ] Valid credentials completes test method successfully
- [ ] Invalid or incomplete credentials fails test method
- [ ] Documented caveats for test methods are provided

### Build Method
- [ ] Verify table was created successfully
- [ ] Verify indexes were created
- [ ] If unique indexes are not used, developer needs to explain why

### Sync Method
- [ ] Appropriate column types are assigned
- [ ] JSON data is in a JSON field
- [ ] Spot check 10-15 records pulled in from sync (note: shuf -n 10 output.log --- expanded in _Spot Checking_)
- [ ] Verify the counts match from Jenkins log matches records in Bytespree
- [ ] Check for duplicate records, within reason
- [ ] Verify columns added to source are added to columns in Bytespree database
- [ ] When connector supports deleting records, ensure physical deletion occurs

### Differential Syncing (Incremental)
- [ ] Ensure last sync date is updated to time sync was started
- [ ] Make sure state file is properly passed (instructions seen toward bottom of document, _Checking the State File_)
- [ ] If manually changing last started date, ensure state file is properly passed and tap handles it correctly (try potentially problematic dates e.g. future dates)
- [ ] Verify the counts match from Jenkins log matches records in Bytespree when running more than once


## Spot Checking
To spot check, locate the tap log file, `cd` to the `/var/connectors/output/{TEAM}/{DATABASE}/sync/{TABLE}/{JENKINS_BUILD_ID}` folder.

Get 10 random lines of output: `shuf -n 10 output.log`

Get 20 random lines of output: `shuf -n 20 output.log`

## Checking the State File
1. Go to the Jenkins sync folder for a table, e.g. `Dashboard` / `Integrations` / `dev` / `virtuous_dec_13` / `sync` / `campaigns`
2. Click Configure
3. At the bottom, click the X to the right of `Delete workspace when build is done`
4. Click Save
5. Re-run this job
6. In the output for the newly launched job, look for: `Building in workspace /var/lib/jenkins/workspace/Integrations/dev/virtuous_dec_13/sync/campaigns`
7. `cd` to the folder outputted in Terminal
8. Execute `cat state.json`
9. Inspect the output, make sure it looks to be what you'd expect