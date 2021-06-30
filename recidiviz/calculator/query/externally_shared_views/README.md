## About `externally_shared_views`

`externally_shared_views` exists to help one copy existing tables to other datasets as part of the 
calculation pipeline.
As an example use case, suppose you want an external party to have access to 
`state.state_person_external_id`, but you cannot grant them access to the `state` dataset.
Simply copying the table is not a good long term solution since calculation reruns will 
render the copy obsolete.

Instead, one should use `externally_shared_views` to deploy a copy of `state_person_external_id` every
time there is a calc rerun.

## How to use `externally_shared_views`
1. Add a file in `views` that contains a `BigQueryViewBuilder` or list of 
   `BigQueryViewBuilder` objects (in the case of multiple destination datasets).
   1. The query may be something as simple as `SELECT * FROM {table}`.
   1. Importantly, set `materialized_address_override` to the desired 
      destination dataset.
   1. If you want to materialize the same table in multiple locations, you can do so by 
      creating a list of `BigQueryViewBuilder` objects that share the same parameters 
      except for `materialized_address_override` and a unique name (`view_id`) for the 
      view that will live in the `externally_shared_views` dataset.
1. Add the new `BigQueryViewBuilder` or list of `BigQueryViewBuilder` objects to the 
   list of builders in `view_config`.
1. Test that the views populate as expected using `recidiviz.tools.load_views_to_sandbox`.
1. Once the new file is merged, the views will populate as part of future calc runs.
