# Automated Data Validation

## External Accuracy Checks

We perform regular, automated accuracy checks between output from our calculations
and values provided by the states. For a list of metrics we validate, see the tables
in the BigQuery dataset corresponding to the `EXTERNAL_ACCURACY_DATASET` dataset
listed in `validation/views/dataset_config.py`.

### Adding New Validation Data

To add new validation data, see [go/external-validations](http://go/external-validations).
