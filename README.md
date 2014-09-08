SideJob
=======

SideJob is built on top of [Sidekiq](https://github.com/mperham/sidekiq) and
[Redis](http://redis.io/). Sidekiq jobs typically complete relatively quickly.
Just like a typical side job, a job in SideJob may depend on other jobs or may make slow progress
and take a long time (such as months). They may be suspended and restarted many times.
The job should be robust to the crashing or downtime of any portion of the infrastructure.

Requirements
------------

Ruby 2.0 or greater and Redis 2.8 or greater is recommended.

Jobs
----

* Jobs have a unique ID assigned using incrementing numbers
** This ID is used as Sidekiq's jid
** Note: a job can be queued multiple times on Sidekiq's queues
** Therefore, Sidekiq's jids are not unique
* Jobs have a queue and class name
* Jobs have any number of input and output ports
* A job can have any number of child jobs
* Each job has at most one parent job
* Arbitrary metadata or state can be stored with each job

Jobs can have a number of different status. The statuses and possible status transitions:
* -> queued
* queued -> running | terminating
* running -> queued | suspended | completed | failed | terminating
* suspended | completed | failed -> queued | terminating
* terminating -> terminated
* terminated -> queued

The difference between suspended, completed, and failed is only in their implications on the
internal job state. Completed implies that the job has processed all data and can be naturally
terminated. If additional input arrives, a completed job could continue running. Suspended implies
that the job is waiting for some input. Failed means that an exception was thrown.

Ports
-----

* Ports are named (case sensitive)
* Any string can be written or read from any input or output port
* JSON is the preferred encoding for complex objects

Workers
-------

* A worker is the implementation of a specific job class
* It should have a perform method that is called on each run
* It may have a shutdown method that is called before the job is terminated
* Workers should be idempotent as they may be run more than once for the same state
* SideJob ensures only one worker thread runs for a given job at a time
* Workers are responsible for managing state across runs
* Workers can suspend themselves when waiting for inputs

Data Structure
--------------

SideJob uses Redis for all job processing and storage. Code using
SideJob should use API functions instead of accessing redis directly,
but this is a description of the current data storage format.

The easiest way to set the redis location is via the environment
variable SIDEJOB_URL, e.g. redis://redis.myhost.com:6379/4

The keys used by Sidekiq:
* queues - Set containing all queue names
* queue:<queue> - List containing jobs to be run (new jobs pushed on left) on the given queue
** A sidekiq job is encoded as json and contains at minimum: queue, retry, class, jid, args
* schedule - Sorted set by schedule time of jobs to run in the future
* retry - Sorted set by retry time of jobs to retry
* dead - Sorted set by addition time of dead jobs
* processes - Set of sidekiq processes (values are host:pid)
* <host>:<pid> - Hash representing a connected sidekiq process
** beat - heartbeat timestamp (every 5 seconds)
** busy - number of busy workers
** info - JSON encoded info with keys hostname, started_at, pid, tag, concurrency, queues, labels. Expiry of 60 seconds.
* <host>:<pid>:workers - Hash containing running workers (thread ID -> {queue: <queue>, payload: <message>, run_at: <timestamp>})
* <host>:<pid>-signals - List for remotely sending signals to a sidekiq process (USR1 and TERM), 60 second expiry.
* stat:processed - Cumulative number of jobs processed
* stat:failed - Cumulative number of jobs failed
* stat:processed:<date> - Number of jobs processed for given date
* stat:failed:<date> - Number of jobs failed for given date

Additional keys used by SideJob:
* workers:<queue> - Hash for worker registry
* job_id - Stores the last job ID (we use incrementing integers from 1)
* jobs - Set containing all active job IDs
* job:<jid> - Hash containing SideJob managed job data
** description - human readable description of the job
** queue - queue name
** class - name of class
** args - JSON array of arguments
** status - job status
** created_at - timestamp that the job was first queued
** updated_at - timestamp of the last update
** ran_at - timestamp of the start of the last run
* job:<jid>:data - Hash containing job specific metadata
* job:<jid>:inports - Set containing input port names
* job:<jid>:outports - Set containing output port names
* job:<jid>:in:<inport> and job:<jid>:out:<outport> - List with unread port data
* job:<jid>:ancestors - List with parent job IDs up to the root job that has no parent
** Newer jobs are pushed on the left so the immediate parent is on the left and the root job is on the right
* job:<jid>:children - Set containing all children job IDs
* job:<jid>:log - List with job changes, new log entries pushed on left. Each log entry is JSON encoded.
** {type: 'status', status: <new status>, timestamp: <date>}
** {type: 'read', by: <jid>, <in|out>port: <port name>, data: <data>, timestamp: <date>}
** {type: 'write', by: <jid>, <in|out>port: <port name>, data: <data>, timestamp: <date>}
** {type: 'error', error: <message>, backtrace: <exception backtrace>, timestamp: <date>}
* job:<jid>:rate:<timestamp> - Rate limiter used to prevent run away executing of a job
** Keys are automatically expired
* job:<jid>:lock - Used to prevent multiple worker threads from running a job
** Auto expired to prevent stale locks
