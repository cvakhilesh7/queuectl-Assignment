#!/usr/bin/env node
// queuectl - Clean submission version
// Node.js single-file implementation (SQLite) - CLI job queue with retries, DLQ, traces, replay, timeouts, config

const { Command } = require('commander');
const Database = require('better-sqlite3');
const { spawn } = require('child_process');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const program = new Command();
const dbPath = path.join(__dirname, 'queuectl.db');
const db = new Database(dbPath);

// -------------------- DB: schema & migration --------------------
db.exec(`
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  command TEXT,
  state TEXT,
  attempts INTEGER,
  max_retries INTEGER,
  run_after INTEGER,
  created_at INTEGER,
  updated_at INTEGER,
  last_error TEXT,
  stdout TEXT DEFAULT '',
  stderr TEXT DEFAULT '',
  exit_code INTEGER,
  runtime_sec INTEGER,
  timeout_sec INTEGER DEFAULT 0,
  priority INTEGER DEFAULT 0,
  replayable_cmd TEXT DEFAULT '',
  trace_created_at INTEGER
);

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_run_after ON jobs(run_after);
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority);
`);

// -------------------- Helpers --------------------
function now() {
  return Math.floor(Date.now() / 1000);
}

function configGet(key) {
  const row = db.prepare('SELECT value FROM meta WHERE key = ?').get(key);
  return row ? row.value : null;
}
function configSet(key, value) {
  const up = db.prepare(`INSERT INTO meta (key,value) VALUES (@key,@value)
    ON CONFLICT(key) DO UPDATE SET value = @value`);
  up.run({ key, value: String(value) });
}

function backoffBase() {
  const v = parseFloat(configGet('backoff_base') || '2');
  return Number.isFinite(v) && v > 0 ? v : 2;
}
function exponentialBackoff(attempt) {
  // delay = base ^ attempt
  const base = backoffBase();
  return Math.pow(base, attempt);
}

// Recover stale processing jobs (left in 'processing' due to crash)
(function recoverStale() {
  const lockTimeout = parseInt(configGet('lock_timeout') || '3600'); // seconds
  const cutoff = now() - lockTimeout;
  const res = db.prepare(`
    UPDATE jobs SET state = 'pending', run_after = @now WHERE state = 'processing' AND updated_at <= @cutoff
  `).run({ now: now(), cutoff });
  if (res.changes > 0) {
    console.log(`♻️ Recovered ${res.changes} stale processing job(s).`);
  }
})();

// -------------------- Core job functions --------------------
function enqueueJob(command, maxRetries = 3, runAt = 0, timeoutSec = 0, priority = 0) {
  const job = {
    id: uuidv4(),
    command,
    state: 'pending',
    attempts: 0,
    max_retries: maxRetries,
    run_after: now() + runAt,
    created_at: now(),
    updated_at: now(),
    last_error: '',
    stdout: '',
    stderr: '',
    exit_code: null,
    runtime_sec: null,
    timeout_sec: timeoutSec,
    priority: priority,
    replayable_cmd: command,
    trace_created_at: null
  };
  const insert = db.prepare(`
    INSERT INTO jobs (id, command, state, attempts, max_retries, run_after, created_at, updated_at, last_error,
      stdout, stderr, exit_code, runtime_sec, timeout_sec, priority, replayable_cmd, trace_created_at)
    VALUES (@id, @command, @state, @attempts, @max_retries, @run_after, @created_at, @updated_at, @last_error,
      @stdout, @stderr, @exit_code, @runtime_sec, @timeout_sec, @priority, @replayable_cmd, @trace_created_at)
  `);
  insert.run(job);
  console.log(`✅ Job added: ${job.id} → "${job.command}" (runs in ${runAt}s, timeout=${timeoutSec}s, priority=${priority})`);
  return job.id;
}

function listJobs(state = null) {
  let rows;
  if (state) rows = db.prepare('SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC').all(state);
  else rows = db.prepare('SELECT * FROM jobs ORDER BY created_at DESC').all();
  if (!rows || rows.length === 0) {
    console.log('ℹ️  No jobs found.');
    return;
  }
  const out = rows.map(r => ({
    id: r.id,
    cmd: r.command,
    state: r.state,
    attempts: r.attempts,
    max_retries: r.max_retries,
    run_after: r.run_after,
    timeout: r.timeout_sec,
    priority: r.priority
  }));
  console.table(out);
}

function statusSummary() {
  const counts = db.prepare('SELECT state, COUNT(*) as count FROM jobs GROUP BY state').all();
  if (!counts || counts.length === 0) {
    console.log('ℹ️  No jobs yet.');
    return;
  }
  console.log('📊 Job Status Summary:');
  console.table(counts);
}

// Atomic pick-and-lock: select pending job with highest priority and earliest created, lock it to processing
function pickAndLockJob() {
  const nowSec = now();
  const tx = db.transaction(() => {
    const row = db.prepare(`
      SELECT id FROM jobs
      WHERE state = 'pending' AND run_after <= ?
      ORDER BY priority DESC, created_at ASC
      LIMIT 1
    `).get(nowSec);
    if (!row) return null;
    const upd = db.prepare(`UPDATE jobs SET state = 'processing', updated_at = @now WHERE id = @id AND state = 'pending'`);
    const info = upd.run({ id: row.id, now: nowSec });
    if (info.changes === 1) {
      return db.prepare('SELECT * FROM jobs WHERE id = ?').get(row.id);
    }
    return null;
  });
  return tx();
}

function updateJobState(id, data) {
  const keys = Object.keys(data).map(k => `${k} = @${k}`).join(', ');
  const stmt = db.prepare(`UPDATE jobs SET ${keys}, updated_at = @updated_at WHERE id = @id`);
  stmt.run({ ...data, updated_at: now(), id });
}

// -------------------- Execution with timeout & trace --------------------
function executeJobCapture(job) {
  return new Promise((resolve) => {
    const start = Date.now();
    const child = spawn(job.command, { shell: true });

    let stdout = '';
    let stderr = '';
    let killedByTimeout = false;

    child.stdout.on('data', d => { stdout += d.toString(); });
    child.stderr.on('data', d => { stderr += d.toString(); });

    let timer = null;
    if (job.timeout_sec && job.timeout_sec > 0) {
      timer = setTimeout(() => {
        killedByTimeout = true;
        try { child.kill('SIGKILL'); } catch (e) { /* ignore */ }
      }, job.timeout_sec * 1000);
    }

    child.on('close', (code) => {
      if (timer) clearTimeout(timer);
      const runtime = Math.floor((Date.now() - start) / 1000);
      // persist trace
      db.prepare(`
        UPDATE jobs SET stdout=@stdout, stderr=@stderr, exit_code=@exit_code, runtime_sec=@runtime_sec, trace_created_at=@trace_created_at
        WHERE id = @id
      `).run({
        stdout, stderr,
        exit_code: killedByTimeout ? null : code,
        runtime_sec: runtime,
        trace_created_at: now(),
        id: job.id
      });
      const success = (!killedByTimeout && code === 0);
      resolve({ success, exitCode: code, stdout, stderr, runtime, killedByTimeout });
    });

    child.on('error', (err) => {
      if (timer) clearTimeout(timer);
      db.prepare(`
        UPDATE jobs SET stdout=@stdout, stderr=@stderr, exit_code=@exit_code, runtime_sec=@runtime_sec, trace_created_at=@trace_created_at
        WHERE id = @id
      `).run({
        stdout, stderr: err.message,
        exit_code: -1,
        runtime_sec: 0,
        trace_created_at: now(),
        id: job.id
      });
      resolve({ success: false, exitCode: -1, stdout, stderr: err.message, runtime: 0, killedByTimeout: false });
    });
  });
}

// -------------------- Worker loop & graceful shutdown --------------------
let shuttingDown = false;

async function workerLoop(workerId) {
  console.log(`🛠️ Worker ${workerId} started`);
  while (!shuttingDown) {
    // check global stop flag (set by worker:stop)
    const stopFlag = configGet('stop_workers') === '1';
    if (stopFlag) {
      console.log(`🛑 Worker ${workerId} received stop signal (meta stop_workers=1). Exiting...`);
      break;
    }

    const job = pickAndLockJob();
    if (!job) {
      await new Promise(r => setTimeout(r, 1000));
      continue;
    }

    console.log(`🟡 [${workerId}] Processing ${job.id} → ${job.command}`);
    const res = await executeJobCapture(job);

    if (!res.success) {
      const nextAttempt = job.attempts + 1;
      const errMsg = res.killedByTimeout ? `Timeout after ${job.timeout_sec}s` : (res.stderr || `exit ${res.exitCode}`);
      if (nextAttempt >= job.max_retries) {
        updateJobState(job.id, { state: 'dead', last_error: errMsg });
        console.log(`💀 [${workerId}] Job moved to DLQ: ${job.id}`);
      } else {
        const delay = Math.floor(exponentialBackoff(nextAttempt));
        updateJobState(job.id, {
          state: 'pending',
          attempts: nextAttempt,
          run_after: now() + delay,
          last_error: errMsg
        });
        console.log(`🔁 [${workerId}] Retry scheduled in ${delay}s (attempt ${nextAttempt})`);
      }
    } else {
      console.log(`✅ [${workerId}] Success ${job.id} → ${ (res.stdout || '').trim() }`);
      updateJobState(job.id, { state: 'completed' });
    }
    // small pause before next pick
    await new Promise(r => setTimeout(r, 200));
  }
  console.log(`🛑 Worker ${workerId} stopped`);
}

function startWorkers(count = 1) {
  shuttingDown = false;
  // clear stop flag
  configSet('stop_workers', '0');
  for (let i = 0; i < count; i++) {
    const wid = `W-${process.pid}-${i}`;
    workerLoop(wid).catch(e => console.error(`[worker ${wid}] error:`, e));
  }
}

// graceful shutdown signals
process.on('SIGINT', () => {
  console.log('SIGINT received — shutting down gracefully...');
  shuttingDown = true;
});
process.on('SIGTERM', () => {
  console.log('SIGTERM received — shutting down gracefully...');
  shuttingDown = true;
});

// -------------------- CLI commands --------------------
program.name('queuectl').description('CLI job queue - submission version').version('1.0.0');

program
  .command('enqueue <cmd>')
  .option('-r, --retries <num>', 'max retries', '3')
  .option('--run-at <seconds>', 'delay in seconds before running', '0')
  .option('--timeout <seconds>', 'kill job if runs longer (0 = no timeout)', '0')
  .option('--priority <n>', 'priority (higher first)', '0')
  .description('Add a new job to queue')
  .action((cmd, opts) => {
    const runAt = parseInt(opts.runAt || opts.run_at || '0');
    const retries = parseInt(opts.retries || '3');
    const timeout = parseInt(opts.timeout || '0');
    const priority = parseInt(opts.priority || '0');
    enqueueJob(cmd, retries, runAt, timeout, priority);
  });

program
  .command('list')
  .option('-s, --state <state>', 'filter by state')
  .description('List jobs (all or by state)')
  .action((opts) => listJobs(opts.state));

program
  .command('status')
  .description('Show job summary by state')
  .action(() => statusSummary());

program
  .command('worker:start')
  .option('-c, --count <n>', 'number of workers', '1')
  .description('Start worker(s) to process jobs')
  .action((opts) => {
    const c = parseInt(opts.count || '1');
    console.log(`Starting ${c} worker(s)...`);
    startWorkers(c);
  });

program
  .command('worker:stop')
  .description('Signal running workers to stop gracefully')
  .action(() => {
    configSet('stop_workers', '1');
    console.log('Sent stop signal to workers (set meta.stop_workers=1). Running workers will exit after current job.');
  });

// DLQ retry and list
program
  .command('dlq:retry <id>')
  .description('Retry a job from DLQ (move back to pending)')
  .action((id) => {
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(id);
    if (!job) { console.log('❌ Job not found'); return; }
    if (job.state !== 'dead') { console.log('⚠️ Job is not in DLQ'); return; }
    updateJobState(id, { state: 'pending', attempts: 0, run_after: now(), last_error: null });
    console.log(`🔁 Moved job ${id} back to pending`);
  });

// show job trace
program
  .command('show <id>')
  .description('Show job details and execution trace')
  .action((id) => {
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(id);
    if (!job) { console.log('❌ Job not found'); return; }
    console.log('--- JOB ---');
    console.table([{
      id: job.id,
      command: job.command,
      state: job.state,
      attempts: job.attempts,
      max_retries: job.max_retries,
      run_after: job.run_after,
      timeout_sec: job.timeout_sec,
      priority: job.priority,
      created_at: job.created_at,
      updated_at: job.updated_at
    }]);
    console.log('--- TRACE ---');
    console.log('exit_code:', job.exit_code);
    console.log('runtime_sec:', job.runtime_sec);
    console.log('last_error:', job.last_error);
    console.log('stdout:\n', job.stdout || '(empty)');
    console.log('stderr:\n', job.stderr || '(empty)');
  });

// replay job command (dry-run default, requires --confirm to actually execute)
program
  .command('replay <id>')
  .option('--dry-run', 'show command without executing', true)
  .option('--confirm', 'actually execute the replay', false)
  .description('Replay a job command (dry-run unless --confirm is provided)')
  .action((id, opts) => {
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(id);
    if (!job) { console.log('❌ Job not found'); return; }
    console.log('Replay command:', job.replayable_cmd);
    if (!opts.confirm) {
      console.log('Dry-run mode. Add --confirm to actually execute.');
      return;
    }
    console.log('▶️ Executing replay now (inherited stdio)...');
    const c = spawn(job.replayable_cmd, { shell: true, stdio: 'inherit' });
    c.on('close', code => console.log(`Replay finished with exit code ${code}`));
  });

// config commands
program
  .command('config-set <key> <value>')
  .description('Set configuration (e.g., backoff_base, lock_timeout)')
  .action((key, value) => {
    configSet(key, value);
    console.log(`Config set: ${key} = ${value}`);
  });

program
  .command('config-get <key>')
  .description('Get configuration value')
  .action((key) => {
    console.log(configGet(key));
  });


// test mode - deterministic injection for demo
program
  .command('test')
  .option('--count <n>', 'how many test jobs', '5')
  .option('--fail-rate <f>', 'approx fraction of jobs that fail (0-1)', '0.5')
  .description('Enqueue deterministic test jobs (some fail) for demo')
  .action((opts) => {
    const n = parseInt(opts.count || '5');
    const f = parseFloat(opts.failRate || opts['fail-rate'] || '0.5');
    for (let i = 0; i < n; i++) {
      // deterministic pattern: fail every k-th where k = round(1/f)
      const k = Math.max(1, Math.round(1 / Math.max(0.01, f)));
      const shouldFail = (i % k) === 0;
      const cmd = shouldFail ? 'node -e "process.exit(1)"' : 'echo OK';
      enqueueJob(cmd, 3, 0, 5, 0);
    }
    console.log(`Enqueued ${n} test jobs (approx fail-rate ${f})`);
  });

// Aliases & convenience: dlq list -> list --state dead
program
  .command('dlq:list')
  .description('List DLQ jobs')
  .action(() => listJobs('dead'));

// Default help if no args
if (process.argv.length <= 2) {
  program.outputHelp();
}

// parse CLI args
program.parse(process.argv);
