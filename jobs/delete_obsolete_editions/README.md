# Delete obsolete editions

This batch job deletes editions considered "obsolete".

"Obsolete" editions are editions that fulfill both of the following criteria:

1. They are more than 90 days old.
2. The are past the third generation of editions (meaning that we always keep at
least three editions of a dataset version).

When an edition is deleted, any distributions (and their files) belonging to it
is deleted as well.
