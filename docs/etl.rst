Data extraction and cleaning
============================

ETL is handled by ``drake``, which is expected to be called from the top (or
root) directory of this repository. All paths assume that drake was invoked
from the root of the repository.

There are four conceptual steps: extract, clean, deduplicate, and process.

Extract raw data
----------------

The scripts here expect to find a zipped data dump in a ``data`` directory in
the repository root. It will extract that, and restore into the database. The
database configuration is specified within a ``config`` directory in the
repository root.

Clean relevant tables
---------------------

The raw tables, as restored from the database dump, get placed into the public
schema of the database. Before being used by the pipeline, all tables go through
a cleaning and normalization process. All scripts contained within the
``etl/data_cleaning`` directory are executed at this point.

Deduplicate identities
----------------------

Deduplication is handled by `superdeduper <https://github.com/dssg/superdeduper>`_.
A SQL script creates a master "entries" table by combining all relevant columns
from all the data sources, and then superdeduper is called with the saved
configuration in ``etl/dedupe/config.yaml``. After a ``dedupe_id`` is appended
to the entries table, the ``apply_results.py`` script goes back to the
specified tables in the `clean` schema to append a dedupe_id column.

Further processing
------------------

Finally, a few SQL scripts are used to create computed tables for convenience.
These include things like a timeline of events. All scripts contained within the
``etl/data_processing`` directory are executed at this point.
