# Automated Data Validation

## External Accuracy Checks
We perform regular, automated accuracy checks between output from our calculations
and values provided by the states. For a list of metrics we validate, see the tables
in the BigQuery dataset corresponding to the `EXTERNAL_ACCURACY_DATASET` dataset
listed in `validation/views/dataset_config.py`.

### Adding New Validation Data  
When a new state is onboarded, we populate the tables in the `EXTERNAL_ACCURACY_DATASET`
dataset containing corresponding numbers from the state. To add new data to these tables,
upload the state data to a table in BigQuery, and run an `INSERT INTO` statement on the
appropriate external accuracy table:

```
INSERT INTO `project_id.dataset_id.table_id`
(-- SELECT statement where the output matches the schema of table_id --) 
```

Adding new data to an external accuracy table automatically includes those rows
in the corresponding external accuracy validation check.
