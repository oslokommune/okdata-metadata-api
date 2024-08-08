# Update last read

This batch job updates the `last_read` metadata fields on all datasets that were
read from S3 two hours before. We wait two hours to let the S3 access logs
settle; they sometimes take some time to populate.

To determine the datasets that were read two hours ago, the S3 access logs in
the `ok-origo-dataplatform-logs-{dev|prod}` bucket is traversed, looking for GET
requests on keys that look like datasets. The datasets found this way is then
updated with the last read timestamp in the `last_read` metadata field.
