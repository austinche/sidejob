SideJob
=======

SideJob is built on top of [Sidekiq](https://github.com/mperham/sidekiq) and
[Redis](http://redis.io/). Sidekiq jobs typically complete relatively quickly.
Just like a typical side job, a job in SideJob may depend on other jobs or may make slow progress
and take a long time (such as months). They may be suspended and restarted many times.
The job should be robust to the crashing or downtime of any portion of the infrastructure.

Jobs can be language agnostic. Any language that talks to Redis can be used as a client.
Workers should preferably use a Resque-compatible library.

Requirements
------------

Ruby 1.9.3 or greater and Redis 2.4 or greater is required.

The SideJob::Filter worker depends on [jq](https://github.com/stedolan/jq).

Workers
-------

* A worker is a specific class that implements a job
* Workers should be idempotent as they may be run more than once for a job
* Workers pull jobs from queues

Jobs
----

* A job is specified by a queue and class name
* Jobs have a unique job id
* A job can have any number of child jobs
* Each job has at most one parent job
* Jobs can be suspended
* Jobs are restarted when child jobs change state
* Jobs can store arbitrary state
* Jobs on restart are responsible for restoring state properly

Ports
-----

* Jobs have any number of input and output ports
* Ports are named (case sensitive)
* Any string can be pushed or popped from any input or output port
* JSON is the preferred encoding for complex objects

Graphs
------

A graph specifies the data flow between jobs and ports. Data sent to the output port
of one job will be automatically moved to the input port of a connected job.
