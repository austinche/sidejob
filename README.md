SideJob
=======

SideJob is built on top of [Sidekiq](https://github.com/mperham/sidekiq) and [Redis](http://redis.io/). Sidekiq jobs typically
complete relatively quickly. Just like a typical side job, a job in SideJob may depend on other jobs or may make slow progress
and take a long time (such as months). Jobs should be robust to the crashing or downtime of any portion of the infrastructure
and they may be suspended and restarted many times.

Requirements
------------

Ruby 2.0 or greater and Redis 2.8 or greater is recommended.

Jobs
----

* Jobs have a unique ID assigned using incrementing numbers
    * This ID is used as Sidekiq's jid
    * Note: a job can be queued multiple times on Sidekiq's queues
    * Therefore, Sidekiq's jids are not unique
* Jobs can also have any number of globally unique string names as aliases
* Jobs have a queue and class name
* Jobs have any number of input and output ports
* A job can have any number of named child jobs
* Each job has at most one parent job
* Jobs can store any JSON encoded object in its internal state

Jobs can have a number of different status. The statuses and possible status transitions:

* -> queued
* queued -> running | terminating
* running -> queued | suspended | completed | failed | terminating
* suspended | completed | failed -> queued | terminating
* terminating -> terminated
* terminated -> queued

The difference between suspended, completed, and failed is only in their implications on the internal job state. Completed
implies that the job has processed all data and can be naturally terminated. If additional input arrives, a completed job
could continue running. Suspended implies that the job is waiting for some input. Failed means that an exception was thrown.

Jobs that have been terminated along with all their children can be deleted entirely.

Ports
-----

* Ports are named (case sensitive) and must match `/^[a-zA-Z0-9_]+$/`.
* Any object that can be JSON encoded can be written or read from any input or output port.
* Ports must be explicitly specified for each job either by the worker configuration or when queuing new jobs unless
  a port named `*` exists in which case new ports are dynamically created and inherit its options.
* Currently, the only port option is a default value which is returned when a read is done on the port when its empty.

Channels
--------

Channels provide a global reliable pubsub system. Every port can be associated with some number of channels.
Writes to output ports will publish the data to the associated channels. Any published messages to a channel
are written to all input ports that have subscribed to that channel.

The pubsub system is reliable in that the subscribed jobs do not need to be running to receive messages.
Other clients can also subscribe to channels via standard non-reliable Redis pubsub.

Channel names use slashes to indicate hierarchy. Published messages to a channel are also published to channels
up the hierarchy. For example, a message sent to the channel `/namespace/event` will be sent to the channels
`/namespace/event`, `/namespace` and `/`.

SideJob uses channels starting with /sidejob. The channels used by sidejob:

* `/sidejob/log` : Log message
    * { timestamp: (date), job: (id), read: [{ job: (id), (in|out)port: (port), data: [...] }, ...], write: [{ job: (id), (in|out)port: (port), data: [...] }, ...] }
    * { timestamp: (date), job: (id), error: (message), backtrace: (exception backtrace) }

Workers
-------

* A worker is the implementation of a specific job class
* Workers are required to register themselves
* A Sidekiq process should only handle a single queue so all registered workers in the process are for the same queue
* It should have a perform method that is called on each run
* It may have a shutdown method that is called before the job is terminated
* Workers should be idempotent as they may be run more than once for the same state
* SideJob ensures only one worker thread runs for a given job at a time
* Workers are responsible for managing state across runs
* Workers can suspend themselves when waiting for inputs

Data Structure
--------------

SideJob uses Redis for all job processing and storage. Code using SideJob should use API functions instead of accessing
redis directly, but this is a description of the current data storage format.

The easiest way to set the redis location is via an environment variable SIDEJOB_HOST=redis.myhost.com or
SIDEJOB_URL=redis://redis.myhost.com:6379/4

The keys used by Sidekiq:

* queues - Set containing all queue names
* queue:(queue) - List containing jobs to be run (new jobs pushed on left) on the given queue
    * A sidekiq job is encoded as json and contains at minimum: queue, retry, class, jid, args
* schedule - Sorted set by schedule time of jobs to run in the future
* retry - Sorted set by retry time of jobs to retry
* dead - Sorted set by addition time of dead jobs
* processes - Set of sidekiq processes (values are host:pid)
* (host):(pid) - Hash representing a connected sidekiq process
    * beat - heartbeat timestamp (every 5 seconds)
    * busy - number of busy workers
    * info - JSON encoded info with keys hostname, started_at, pid, tag, concurrency, queues, labels. Expiry of 60 seconds.
* (host):(pid):workers - Hash containing running workers (thread ID -> { queue: (queue), payload: (message), run_at: (timestamp) })
* (host):(pid)-signals - List for remotely sending signals to a sidekiq process (USR1 and TERM), 60 second expiry.
* stat:processed - Cumulative number of jobs processed
* stat:failed - Cumulative number of jobs failed
* stat:processed:(date) - Number of jobs processed for given date
* stat:failed:(date) - Number of jobs failed for given date

Additional keys used by SideJob:

* workers:(queue) - Hash mapping class name to worker configuration. A worker should define
  the inports and outports hashes that map port names to port options.
* jobs - Set with all job ids.
* jobs:last_id - Stores the last job ID (we use incrementing integers from 1).
* jobs:aliases - Hash mapping a name to job id.
* job:(id) - Hash containing job state. Each value is JSON encoded.
    * status - job status
    * queue - queue name
    * class - name of class
    * args - array of arguments passed to worker's perform method
    * parent - parent job ID
    * created_at - timestamp that the job was first queued
    * created_by - string indicating the entity that created the job. SideJob uses job:(id) for jobs created by another job.
    * ran_at - timestamp of the start of the last run
    * Any additional keys used by the worker to track internal job state
* job:(id):aliases - Set with job aliases
* job:(id):in:(inport) and job:(id):out:(outport) - List with unread port data. New data is pushed on the right.
* job:(id):inports and job:(id):outports - Set containing all existing port names.
* job:(id):inports:default and job:(id):outports:default - Hash mapping port name to JSON encoded default value for port.
* job:(id):inports:channels and job:(id):outports:channels - Hash mapping port name to JSON encoded connected channels.
* job:(id):children - Hash mapping child job name to child job ID
* job:(id):rate:(timestamp) - Rate limiter used to prevent run away executing of a job.
    Keys are automatically expired.
* job:(id):lock - Used to control concurrent writes to a job.
    Auto expired to prevent stale locks.
* job:(id):lock:worker - Used to indicate a worker is attempting to acquire the job lock.
    Auto expired to prevent stale locks.
* channel:(channel) - Set with job ids that may have ports subscribed to the channel.
