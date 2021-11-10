Metadata Scripts
===================

Python scripts for enriching, migrating and deleting metadata. Files in this directory are NOT supposed to be included in deployment.
Purpose of writing scripts here is to utilize existing code in the metadata module. Purpose of keeping the scripts, although they typically only need to be run once,
is for traceability.

## Scripts

* `migrate_dataset_processing_stage`
  * This script was used to physically move from one dataset processing stage to another.
* `remove_processing_stage`
  * This script was used to remove the `processing_stage` field from the dataset metadata.
* `set_event_source`
  * This script was used to set source.type = event for all datasets with event streams.
* `set_file_source`
  * This script was used to set source.type = file for all file based datasets.
* `set_none_source`
  * This script was used to set source.type = 'none' for all parent datasets
* `set_dataset_state`
  * This script was used to set `state = active` for all datasets.
* `add_bym_geodata_editions_and_distributions`
  * Adds editions and API distributions for BYM's geo datasets.
* `set_distribution_type`
  * Initializes distribitions' `distribution_type` to either `file` or `api`.
* `delete_datasets`
  * Deletes datasets. These datasets must be identified beforehand explicitly declared in a list in the script. This script is meant to be used for deleting unused datasets from the dev environment.
