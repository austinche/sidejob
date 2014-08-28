SideJob
=======

SideJob is built on top of [Sidekiq](https://github.com/mperham/sidekiq) and
[Redis](http://redis.io/). Sidekiq jobs typically complete relatively quickly.
Just like a typical side job, a job in SideJob may depend on other jobs or may make slow progress
and take a long time (such as months). They may be suspended and restarted many times.
The job should be robust to the crashing or downtime of any portion of the infrastructure.

Requirements
------------

Ruby 2.0 or greater and Redis 2.4 or greater is required.

Workers
-------

* A worker is the implementation of a specific class
* Workers should be idempotent as they may be run more than once for a job
* Workers pull jobs from queues

Jobs
----

* Jobs have a unique ID
* Jobs have a queue and class name
* Jobs have any number of input and output ports
* A job can have any number of child jobs
* Each job has at most one parent job
* Jobs can be suspended if there's nothing to do or when waiting for inputs
* Jobs can store arbitrary state
* Jobs on restart are responsible for restoring state properly
* Jobs are restarted when child jobs complete or suspend

Ports
-----

* Ports are named (case sensitive)
* Any string can be written or read from any input or output port
* JSON is the preferred encoding for complex objects

Data Structure
--------------

SideJob uses Redis for all job processing and storage. Code using
SideJob should use API functions instead of accessing redis directly,
but this is a description of the current data storage format.

The easiest way to set the redis location is via the environment
variable REDIS_URL, e.g. redis://redis.myhost.com:6379/4

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
* job_id - Stores the last job ID (we use incrementing integers from 1)
* jobs - Set containing all active job IDs
* job:<jid> - Hash containing SideJob managed job data
** queue - queue name
** class - name of class
** args - JSON array of arguments
** parent - parent job id
** top - the job id of the top job with no parent in this job's hierarchy
** restart - if set, the job will be requeued for the specified time once it completes (0 means queue immediately)
** status - job status: starting, queued, scheduled, running, suspended, completed, failed
** created_at - timestamp that the job was first queued
** updated_at - latest timestamp for when something happened (a log entry was generated)
*** Allowed status transitions:
**** queued | scheduled -> running
**** scheduled -> queued
**** starting | suspended | completed | failed -> queued | scheduled
**** running -> suspended | completed | failed
* job:<jid>:data - Hash containing the job's internal data
* job:<jid>:inports - Set containing input port names
* job:<jid>:outports - Set containing output port names
* job:<jid>:in:<inport> and job:<jid>:out:<outport> - List with unread port data
* job:<jid>:children - Set containing all children job IDs
* job:<jid>:log - List with job changes, new log entries pushed on left. Each log entry is JSON encoded.
** {type: 'status', status: <new status>, timestamp: <date>}
** {type: 'read', <in|out>port: <port name>, data: <data>, timestamp: <date>}
** {type: 'write', <in|out>port: <port name>, data: <data>, timestamp: <date>}
** {type: 'error', error: <message>, backtrace: <exception backtrace>, timestamp: <date>}
