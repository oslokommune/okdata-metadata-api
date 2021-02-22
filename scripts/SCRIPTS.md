Metadata Scripts
===================

Python scripts for enriching and migrating metadata. Files in this directory are NOT supposed to be included in deployment.
Purpose of writing scripts here is to utilize existing code in the metadata module. Puspose of keeping the scripts, although they typically only need to be run once,
is for traceability.

## Scripts

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
