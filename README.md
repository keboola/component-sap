SAP Extractor
=============

Description

**Table of contents:**

[TOC]

Functionality notes
===================

Prerequisites
=============

Get the API token, register application, etc.

Features
========

| **Feature**             | **Note**                                      |
|-------------------------|-----------------------------------------------|
| Generic UI form         | Dynamic UI form                               |
| Row Based configuration | Allows structuring the configuration in rows. |
| oAuth                   | oAuth authentication enabled                  |
| Incremental loading     | Allows fetching data in new increments.       |
| Backfill mode           | Support for seamless backfill setup.          |
| Date range filter       | Specify date range.                           |

Supported endpoints
===================

If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/)

Configuration
=============

Param 1
-------

Param 2
-------

Date Start
----------

`source.date_from` (string, optional, incremental sync only)

By default an incremental run resumes exactly where the previous one stopped: it
asks SAP for everything changed since the delta pointer stored in the
configuration state. For source tables whose changes cannot be tracked reliably
that leaves late changes behind.

Set Date Start to re-fetch changes since a given date on every run. It accepts an
absolute date (`2026-01-01`) or a relative one (`10 days ago`, `2 weeks ago`). With
`10 days ago`, each run asks SAP for everything changed since *today minus 10 days*.

**The window always runs from Date Start up to now** - there is currently no end
date, so every run fetches through to the present moment. (SAP's delta interface
takes only a lower bound, so an upper bound cannot be offered yet.) Leave Date Start
empty to simply resume from the last delta pointer.

The pointer sent is never newer than the stored one, so a schedule that is behind
still resumes from where it left off rather than skipping the gap - the window is
always a superset of what the run would have fetched otherwise. The pointer written
back to state never moves backwards, so the window stays anchored to the current
date instead of creeping wider run after run.

Two requirements, both enforced with a clear error rather than silently ignored:

- The source's delta pointer must be a `YYYYMMDD` or `YYYYMMDDHHMMSS` timestamp.
  Sequential id pointers cannot express a date window.
- With `incremental_load`, SAP must report key columns for the table, so re-fetched
  rows are updated rather than appended as duplicates.

Keep the window narrow. A delta fetch is a single un-paginated request, so the whole
window has to come back in one response inside the configured `timeout`; a window
wider than 31 days logs a warning.

Date Start applies only to `incremental_sync`. On a `full_sync` row, or on the first
run of an incremental row where no pointer has been stored yet, it is inert - the run
behaves exactly as it would with Date Start empty.

Combining it with `full_load` only logs a warning - every run still overwrites the
destination table with just the fetched window.

Output
======

List of tables, foreign keys, schema.

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://bitbucket.org/kds_consulting_team/kds-team.ex-sap/src/master/ kds-team.ex-sap
cd kds-team.ex-sap
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
