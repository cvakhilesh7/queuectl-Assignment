\# QueueCTL – Backend Developer Assignment (Node.js)



\### Author  

\*\*Candidate:\*\* Chakka Venkata Akhilesh  

\*\*Date:\*\* November 8, 2025  

\*\*Tech Stack:\*\* Node.js, SQLite (better-sqlite3), Commander.js, UUID, Child Processes  





\## 📋 Project Overview



`QueueCTL` is a CLI-based \*\*background job queue system\*\* built using Node.js and SQLite.  

It manages background jobs with retry, persistence, Dead Letter Queue (DLQ), and trace logging.  

This project implements all core requirements from the \*\*Flam Company Backend Developer Internship Assignment\*\*.





⚙️ Setup Instructions



1️⃣ Install dependencies  

Run this command once to install all required Node.js packages:



npm install

2️⃣ Run basic test

To confirm everything works:



\# Add a new job

node queuectl.js enqueue "echo Hello from QueueCTL"



\# Start worker

node queuectl.js worker:start



\# List all jobs

node queuectl.js list

You should see a new database file queuectl.db created automatically.



🧩 Core CLI Commands

Command	Description	                                   Example

enqueue <cmd>	Add a job to the queue	node queuectl.js enqueue "echo Hello"

worker:start --count <n>	Start worker(s) to process jobs	node queuectl.js worker:start --count 2

worker:stop	Stop workers gracefully	node queuectl.js worker:stop

list \[--state <state>]	List jobs (optionally filter by state)	node queuectl.js list --state dead

status	Show job summary (completed/pending/dead)	node queuectl.js status

dlq:list	List jobs in Dead Letter Queue	node queuectl.js dlq:list

dlq:retry <id>	Move DLQ job back to pending	node queuectl.js dlq:retry <job-id>

show <id>	Display full job details and trace	node queuectl.js show <job-id>

replay <id>	Re-run a job (with --confirm to execute)	node queuectl.js replay <job-id> --confirm

config-set <key> <value>	Set configuration parameter	node queuectl.js config-set backoff\_base 3

config-get <key>	Get configuration value	node queuectl.js config-get backoff\_base

test --count <n> --fail-rate <f>	Enqueue multiple test jobs	node queuectl.js test --count 5 --fail-rate 0.5



🧠 Architecture Overview

Job Lifecycle

State	           Description

pending	        Waiting for a worker to pick it up

processing	    Currently running

completed	      executed

dead	           Permanently failed after retries (moved to DLQ)



Retry and Backoff Logic

Failed jobs are retried automatically.



Exponential backoff formula:



ini



delay = backoff\_base ^ attempts

Default base = 2, configurable via CLI.



Persistence

Jobs and configurations are stored in queuectl.db using SQLite.



Two tables:



jobs → all job data, retry info, stdout/stderr, etc.



meta → stores configuration and worker stop flag.



Timeout Handling

Each job can have --timeout to auto-kill long tasks.



When exceeded → job fails → retried or sent to DLQ.



Graceful Shutdown

Handles SIGINT and SIGTERM (Ctrl+C) safely.



Active jobs complete before shutdown.



🧪 Verification Checklist

Feature	             Command	                                 Expected Result

Enqueue \& Process	node queuectl.js enqueue "echo Hello" → worker:start	Job completes successfully

Retry \& DLQ	node queuectl.js enqueue "invalidcmd"	Job retries → moves to DLQ

DLQ Listing	node queuectl.js dlq:list	Shows failed jobs

Retry from DLQ	node queuectl.js dlq:retry <id>	Job returns to pending

Show Trace	node queuectl.js show <id>	Displays stdout, stderr

Replay	node queuectl.js replay <id> --confirm	Job runs again

Timeout	node queuectl.js enqueue "node -e \\"setTimeout(()=>{},10000)\\"" --timeout 3	Job killed after 3s

Configuration	node queuectl.js config-set backoff\_base 3	Config updated

Persistence	Stop and restart → list	Jobs persist in DB

Test Mode	node queuectl.js test --count 6 --fail-rate 0.5	Mix of success/fail



🧰 Example Demo Flow (for video)

Record these exact steps (about 40 seconds total):



node queuectl.js test --count 4 --fail-rate 0.5

node queuectl.js worker:start

node queuectl.js dlq:list

node queuectl.js show <dead-job-id>

node queuectl.js replay <completed-job-id> --confirm

node queuectl.js status

🎥 Record screen while these commands run — that’s your submission demo video.



🧱 Configuration Keys

Key	Default	Description

backoff\_base	2	Retry delay exponent base

lock\_timeout	3600	Recover stuck jobs after 1 hr

stop\_workers	0	Flag used for graceful shutdown





⭐ Conclusion



This project demonstrates:



Complete background job queue system



Fault-tolerant retry and DLQ handling



Persistent, recoverable architecture



Configurable runtime behavior



Strong documentation and testing



© 2025 — QueueCTL by Chakka Venkata Akhilesh
