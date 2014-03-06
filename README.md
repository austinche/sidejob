SideJob
=======

Requirements
------------

Ruby 1.9.3 or greater and Redis 2.4 or greater is required.

Workers
-------

* Workers can have any number of child jobs.
* Each job has at most one parent job.
* Workers can be suspended.
* Workers are automatically restarted when child jobs change their status.
* Workers can store arbitrary state
* Workers should be idempotent. They may be run a non-defined number of times.
* Workers on restart are responsible for restoring state properly.
* Inputs and outputs are specified via ports.

Ports
-----

If data1 is pushed to port 1 and then data2 is pushed to port 2, it is guaranteed that
data1 will be visible on port 1 before data2 is visible.
