<!-- go/g3mark-in-g3doc -->

# baseline-perf

<!--* freshness: { owner: 'jybao' reviewed: '2026-10-03' } *-->

`baseline-perf` (`bperf`) is a tool to answer three simple questions about
[Substrate](https://github.com/agent-substrate/substrate):

1.  **Unit cost**: On an idle node, what does one actor's Resume, Execute,
    Pause, or Suspend cost, and where does the time go?
2.  **Capacity**: How many actors can one node run at once before Resume or
    Suspend slows down, and what runs out first?
3.  **Comparison**: Did a change (atelet, guest kernel or rootfs, machine type,
    ...) make things faster or snapshots smaller?

It answers them by replaying a recorded coding-agent session in an actor (in
many at once, for capacity) and timing every step; see [Benchmark
setup](#benchmark).

[TOC]

## Run it

**First time?** Set up the cluster once: see [Cluster setup](#setup).

Set `KO_DOCKER_REPO` to a registry (the first run builds the runner pod's image
and pushes it there), and make `baseline-perf` a command:

```shell
export KO_DOCKER_REPO=gcr.io/<your-project>/ate-images
alias baseline-perf='python3 /google/src/head/depot/google3/experimental/users/jybao/baseline_perf/baseline-perf'
```

Every run uses your current `kubectl` context, checks that the worker node is
idle before starting, and cleans up at the end. Each question has its own kind
of run.

### Unit cost {#unit-cost}

What does one Resume, Execute, Pause, or Suspend cost on an idle node, and where
does the time go? Run the benchmark once (about 90 seconds):

```shell
baseline-perf
```

It replays the session twice, each time on a new actor: the **Pause path** parks
the actor between cycles with `PauseActor` (checkpoint kept on the node), and
the **Suspend path** with `SuspendActor` (snapshot uploaded to GCS). The report
gives each path's median Resume, Exec, and Park, its cold start, and where
Resume and Park spend their time, plus the size of the suspend path's snapshots.
See [Example output](#example-output), and [Benchmark setup](#benchmark) for the
details.

### Capacity {#capacity}

How many actors can one node run at once before Resume or Suspend slows down,
and what runs out first? Run steps of more and more actors at once (about 6
minutes with the default steps):

```shell
baseline-perf --capacity                            # 1, 2, 4, 8 actors at once, 60 s each
baseline-perf --capacity 1,2,4,8,16 --duration 90   # your own steps
```

In each step, every actor goes round Resume → Exec → Suspend on its own, back to
back. The first step in which a call fails, or Resume or Suspend P90 is more
than twice the 1-actor step's, is over capacity: the node takes as many actors
as the step before, and the report says what ran out on the node. See [Measuring
capacity](#capacity-run).

What one actor uses is in every run's header: the node's limits (machine type;
disk type, IOPS, and throughput) next to its load in the 10 seconds before the
run and during it (busy cores, disk utilization, and I/O pressure), measured by
a pod on the worker node. In the [example](#example-output), one actor took the
node from 0.2 to 1.0 busy cores and from 2% to 4% disk utilization.

### Comparison {#comparison}

Did a change make things faster or snapshots smaller? `--compare` runs the
benchmark on two setups in turn (`A, B, A, B, ...`, 3 runs each by default) on
the same worker node, then prints them side by side with the difference (see the
[example](#compare-output)). You can compare (each links to its commands):

*   [**atelet**](#compare-atelet): your working tree, a git ref, or a prebuilt
    image.
*   [**Guest files**](#compare-guest-files): the guest kernel, rootfs,
    `kata-config`, `cloud-hypervisor`, or `virtiofsd`.
*   [**A whole guest**](#compare-guest): a directory from `assemble.sh`, or a
    `SandboxConfig` on the cluster.
*   [**Machine types or disks**](#compare-machine): not with `--compare`; run
    `baseline-perf` on each cluster instead.

To run once on other guest files without comparing, see [Testing a
guest](#guest).

## Example output {#example-output}

<!-- The en dashes below are what baseline-perf prints for empty cells. -->

<!-- disableFinding(SNIPPET_EM_DASH) -->

```
══════════════════════════════════════════════════════════════
 baseline-perf · swebench-astropy-7336 · 70 s
 substrate v0.1.0-gke.1 · commit fa6d9496 (dirty)
 c3-highmem-192-metal, bare metal · k8s 1.35
 disk  hyperdisk-balanced 100 GB · 3,600 IOPS · 290 MiB/s · no local SSD
 node idle: 0.2 busy cores · disk 2% · PSI io 0%
 node during the run: 1.0 busy cores · disk 4% · PSI io 0%
══════════════════════════════════════════════════════════════
 Median of 4 cycles       Resume      Exec      Park
 Pause (on the node)      522 ms   2293 ms    382 ms
 Suspend (to GCS)        1084 ms   2403 ms   2099 ms
 Cold start (golden)      715 ms         –         –

 ── Pause path (checkpoint kept on the node) ──
 Resume  522 ms
   vm_restore       274 ms   53%  ███████████
   local copy        93 ms   18%  ████
   other            155 ms   30%  ██████

 Pause  382 ms
   snapshot         236 ms   62%  ████████████
   teardown         109 ms   29%  ██████
   other             37 ms   10%  ██

 ── Suspend path (snapshot to GCS) ──
 Resume  1084 ms
   download         640 ms   59%  ████████████
   vm_restore       251 ms   23%  █████
   other            194 ms   18%  ████

 Suspend  2099 ms
   snapshot upload 1226 ms   58%  ████████████
   snapshot         219 ms   10%  ██
   other            653 ms   31%  ██████

 Snapshot  2,026 MB image · 257 MB populated · 111 MB in GCS

 Note: 5 resumes waited up to 107 ms for a free worker (not counted)
```

<!-- enableFinding(SNIPPET_EM_DASH) -->

*   **Header**: Hardware, disk, Substrate version/commit, and node load in the
    10s before the run (**node idle**) and during it. If something else was
    using the node, it warns with **⚠ node busy before the run**.
*   **Median of 4 cycles**: End-to-end latency for **Resume**, **Exec** (through
    `atenet-router`), and **Park** (`PauseActor` or `SuspendActor`), plus the
    initial **Cold start** from the golden snapshot.
*   **Phase breakdown**: The two slowest phases of the median Resume and Park,
    built from `ate-api`, `atelet`, and `ateom` logs.
*   **Snapshot**: Size of the suspend path's snapshots — logical **image** size
    (mostly sparse VM RAM), **populated** bytes actually read, and compressed
    size **in GCS**.

## Measuring capacity (`--capacity`) {#capacity-run}

`--capacity` checks that the node is idle, then runs a step for each level
(`1,2,4,8` by default; the first must be 1). A step of N actors:

1.  **Workers**: Scales the template's `WorkerPool` to N + 1 workers, all on the
    same node: one per actor and a spare, so that no Resume waits for a free
    worker. Then it writes the node's dirty pages to disk (`sync`), so that what
    came before isn't written back during the step.
2.  **Cold starts**: The runner pod creates the actors `perf-eval-actor-0` to
    `perf-eval-actor-<N-1>`, resumes each from the golden snapshot, and suspends
    it. This part isn't timed.
3.  **Load**: For `--duration` seconds (default 60), each actor goes round
    Resume → Exec → Suspend on its own, back to back, through the session's 4
    cycles and back to cycle 1. Their first cycles start spread over 10 seconds,
    so that their calls don't line up.
4.  **Numbers**: Only the cycles that start after the first 15 seconds count.
    The step's row gives their rate (cycles/s), Resume and Suspend P50 / P90,
    Exec P50, and the node's load meanwhile (busy cores, disk utilization and
    MiB/s, PSI io), from the sampler pod.
5.  **Cleanup**: The runner deletes its actors.

A step is **over capacity** if a call fails (every actor then stops), or if its
Resume or Suspend P90 is more than twice the 1-actor step's. The run stops
there, and the node takes as many actors as the step before. The report then
says what ran out on the node during that step:

*   **disk**: ≥ 90% busy, ≥ 90% of its provisioned MiB/s or IOPS, or ≥ 10% PSI
    io.
*   **CPU**: ≥ 90% of its cores busy, or ≥ 10% PSI cpu.
*   **memory**: ≥ 10% PSI memory.
*   **nothing on the node**: look at atelet, `ate-api`, GCS, or the network,
    which the sampler can't see.

At the end, even if a step fails or you press Ctrl-C, the `WorkerPool` goes back
to its size; until then, its annotation `bperf.ate.dev/original-replicas` holds
that size. After a run that was killed, `baseline-perf --capacity-restore`
deletes the run's pods and actors and puts the `WorkerPool` back.

> [!NOTE]
> Only the suspend path is measured, and with no think time between
> cycles each actor parks and resumes as fast as it can: a worst case for the
> node, not a model of real agents.

For example, `baseline-perf --capacity 1,2,4,8,16,32` on the node of the [unit
cost example](#example-output) (7 minutes, as it stopped at 16):

```
════════════════════════════════════════════════════════════════════════════════════════
 baseline-perf --capacity · swebench-astropy-7336 · suspend path · 7 min
 substrate v0.1.0-gke.1 · commit fa6d9496 (dirty)
 c3-highmem-192-metal, bare metal · k8s 1.35
 disk  hyperdisk-balanced 100 GB · 3,600 IOPS · 290 MiB/s · no local SSD
 node idle: 0.2 busy cores · disk 2% · PSI io 0%
════════════════════════════════════════════════════════════════════════════════════════
   N  cycles/s  Resume P50/P90  Suspend P50/P90  Exec P50  cores  disk  MiB/s  PSI io
   1      0.18   1,237 / 1,309    1,819 / 1,960     1,861    1.3   10%      9      2%
   2      0.36   1,243 / 1,384    1,763 / 1,973     1,962    2.5   17%     18      2%
   4      0.69   1,305 / 1,830    1,824 / 3,069     1,457    4.6   27%     33      2%
   8      1.38   1,528 / 1,774    1,892 / 2,404     1,797    9.1   47%     60      2%
  16      2.09   1,909 / 2,337    2,274 / 4,369     2,917   15.8   69%     95      4%  ✗

 Capacity: 8 actors at once; with 16, Suspend P90 4,369 ms > 2 × 1,960 ms (its P90 with 1 actor)
 Ran out first: nothing on the node: CPU, disk and memory had room, so look at atelet, ate-api, GCS or the network

 Note: latencies in ms, over the last 45 s of each 60 s step; each actor goes round Resume -> Exec -> Suspend back to back, with a spare worker
```

With 16 actors, Suspend P90 more than doubled while the node's CPU, disk, and
memory still had room, so the node takes 8 actors at once and the limit is
elsewhere. With the default steps, every step on this node holds, and the report
says it takes at least 8.

## Comparing setups (`--compare`) {#compare}

`--compare` runs the benchmark on two setups alternately (`A, B, A, B, ...`) on
the same worker node and prints a side-by-side comparison:

*   **1 spec (`--compare SPEC`)**: Compares the cluster as it is (`stock`, arm
    `A`) against `SPEC` (arm `B`).
*   **2 specs (`--compare SPEC_A SPEC_B`)**: Compares `SPEC_A` (arm `A`) against
    `SPEC_B` (arm `B`).

A spec is `stock`, or one or more `KEY=VALUE` pairs separated by commas, so one
arm can change several things at once (`atelet=~/substrate,kernel=./vmlinux`). A
key set on only one arm defaults to `stock` on the other. Each arm runs on its
own temporary `ActorTemplate` copy and golden snapshot (and its own `atelet`
layer cache when comparing `atelet`, with dirty disk pages flushed before each
run). At the end (or via `baseline-perf --compare-restore` if killed), the
cluster is restored to its original state.

### atelet {#compare-atelet}

`atelet=IMAGE|DIR[@REF]` runs another `atelet` on the worker node: a prebuilt
image, or one that `ko` builds from a local Substrate checkout (`DIR`, with its
uncommitted changes) or from a git ref in it (`DIR@REF`):

```shell
# The cluster's atelet vs. your working tree:
baseline-perf --compare atelet=~/substrate

# v0.1.0 vs. your working tree:
baseline-perf --compare atelet=~/substrate@v0.1.0 atelet=~/substrate

# Two prebuilt images, 5 runs each:
baseline-perf --compare atelet=IMAGE_A atelet=IMAGE_B --rounds 5
```

Build each arm from the Substrate release the cluster runs, plus your change
(the report's `substrate v0.1.0-gke.1` is the tag `v0.1.0`). Only `atelet` is
swapped, and the cluster's workers don't work with an `atelet` from a newer
`main`: the arm's golden snapshot then fails with the workers' error, for
example `connect: no such file or directory` on `credential-broker.sock`.

### Guest files {#compare-guest-files}

Each of these keys replaces one file of the microVM guest; the others stay
`stock`:

| Key                     | What it replaces                                                |
| :---------------------- | :-------------------------------------------------------------- |
| `kernel=PATH`           | The guest kernel: an uncompressed `vmlinux` ELF, not a bzImage. |
| `rootfs=PATH`           | The guest rootfs image (`rootfs.img`).                          |
| `kata-config=PATH`      | The Kata configuration (`configuration-clh.toml`).              |
| `cloud-hypervisor=PATH` | The `cloud-hypervisor` binary (the VMM).                        |
| `virtiofsd=PATH`        | The `virtiofsd` binary.                                         |

```shell
# The cluster's guest kernel vs. yours:
baseline-perf --compare kernel=./vmlinux

# Two kernels:
baseline-perf --compare kernel=./vmlinux-a kernel=./vmlinux-b

# A new kernel and rootfs together, in one arm:
baseline-perf --compare kernel=./vmlinux,rootfs=./rootfs.img
```

### A whole guest {#compare-guest}

`guest=DIR` takes whichever guest files a directory from `assemble.sh` holds
(`vmlinux`, `rootfs.img`, ...), and `guest=CONFIG` uses a `SandboxConfig` that's
already on the cluster:

```shell
baseline-perf --compare guest=./my-guest/
baseline-perf --compare guest=microvm-test
```

### Machine types or disks {#compare-machine}

`--compare` runs both arms on the same node, so it can't compare machine types
or disks. Run `baseline-perf` on a cluster with each one, and compare the two
reports.

### Example comparison output {#compare-output}

Here, arm B fixes `atelet` layer unpacking to preserve file `mtime`, which
avoids recompiling Python `.pyc` files:

```
════════════════════════════════════════════════════════════════════════════════════════
 baseline-perf A/B · swebench-astropy-7336 · 3 rounds · 9 min
 substrate v0.1.0-gke.1
 c3-highmem-192-metal, bare metal · k8s 1.35
 disk  hyperdisk-balanced 100 GB · 3,600 IOPS · 290 MiB/s · no local SSD
 A  atelet=atelet:v0.1.0-bperf-base@f79efcec · atelet commit fa6d9496
 B  atelet=atelet:v0.1.0-bperf-mtime@662323fd · atelet commit fa6d9496 (dirty)
════════════════════════════════════════════════════════════════════════════════════════
 Mean (range) of 3 runs             A                       B                   B - A
 Pause path (checkpoint kept on the node)
   Cold start (golden)         925 ms 813-1,073      1,040 ms 839-1,218       +115 ms  +12%
   Resume, median              500 ms 487-519          458 ms 436-488          -41 ms   -8%
   Exec, median              2,353 ms 2,281-2,434    1,989 ms 1,965-2,027     -364 ms  -15% *
   Park, median                350 ms 345-356          327 ms 323-330          -22 ms   -6% *
   Exec, 4 cycles            9,475 ms 9,344-9,634    8,457 ms 8,378-8,606   -1,019 ms  -11% *
   Exec, cycle 1             2,128 ms 2,118-2,140    1,462 ms 1,392-1,506     -666 ms  -31% *
 Suspend path (snapshot to GCS)
   Cold start (golden)         879 ms 706-1,204        831 ms 785-896          -48 ms   -5%
   Resume, median            1,051 ms 1,012-1,128      939 ms 930-944         -112 ms  -11% *
   Exec, median              2,352 ms 2,296-2,411    1,937 ms 1,914-1,970     -415 ms  -18% *
   Park, median              1,888 ms 1,681-2,084    1,751 ms 1,704-1,838     -137 ms   -7%
   Exec, 4 cycles            9,468 ms 9,239-9,588    8,360 ms 8,204-8,492   -1,109 ms  -12% *
   Exec, cycle 1             2,169 ms 2,129-2,241    1,403 ms 1,387-1,419     -766 ms  -35% *
 Suspend path snapshots
   rootfs upper, total        79.9 MB 79.9-79.9       30.9 MB 30.9-30.9      -49.0 MB  -61% *
   Last snapshot, populated  303.6 MB 303.6-303.6    272.3 MB 272.2-272.3    -31.3 MB  -10% *
   Last snapshot, in GCS     111.3 MB 111.3-111.3    101.8 MB 101.8-101.8     -9.5 MB   -9% *
   rootfs upper per suspend, A: 2.4 · 10.6 · 10.7 · 13.4 · 42.9 MB
   rootfs upper per suspend, B: 0.0 · 0.0 · 0.1 · 0.6 · 30.2 MB

 Run  Arm       pause exec  suspend exec    cold start   upper total  node
   1  A           9,448 ms      9,239 ms        813 ms       79.9 MB  idle
   2  B           8,386 ms      8,204 ms      1,218 ms       30.9 MB  idle
   3  A           9,344 ms      9,588 ms        890 ms       79.9 MB  idle
   4  B           8,606 ms      8,382 ms      1,063 ms       30.9 MB  idle
   5  A           9,634 ms      9,578 ms      1,073 ms       79.9 MB  idle
   6  B           8,378 ms      8,492 ms        839 ms       30.9 MB  idle

 Note: * the arms' ranges don't overlap: the difference is bigger than the spread between runs
```

*   `*` marks metrics where the two arms' min–max ranges don't overlap.
*   **rootfs upper** is the uncompressed size of `rootfs-upper.tar` (files added
    or modified in the container rootfs since the base image) across the cold
    start and 4 cycles.

## Testing a guest (`--guest`) {#guest}

For a single run (without comparing against `stock`), `--guest` runs the
benchmark on custom guest assets (`kernel`, `rootfs`, `kata-config`,
`cloud-hypervisor`, or `virtiofsd`) without modifying the stock template:

```shell
baseline-perf --guest kernel=./vmlinux                       # one file
baseline-perf --guest kernel=./vmlinux,rootfs=./rootfs.img   # multiple files
baseline-perf --guest ./my-guest/                            # a directory from assemble.sh
baseline-perf --guest microvm-test                           # an existing SandboxConfig
```

It validates and uploads changed files to GCS, creates a content-addressed
`SandboxConfig` and temporary `ActorTemplate` copy, takes a fresh golden
snapshot, runs the benchmark, and deletes the temporary template and its
snapshots at the end. Add `--cleanup-guest` to also delete the uploaded GCS
objects and `SandboxConfig`.

## Flags {#flags}

| Flag                      | Effect                                                                                                  |
| :------------------------ | :------------------------------------------------------------------------------------------------------ |
| `--compare SPEC [SPEC_B]` | Compare two setups back-to-back (`A, B, A, B, ...`). See [Comparing setups](#compare).                  |
| `--rounds N`              | With `--compare`: runs per arm (default: 3).                                                            |
| `--compare-restore`       | Clean up after a killed `--compare` run (restore DaemonSet, templates, and layer caches).               |
| `--capacity [LEVELS]`     | Steps of N actors at once, N in `LEVELS` (default: `1,2,4,8`). See [Measuring capacity](#capacity-run). |
| `--duration SECONDS`      | With `--capacity`: seconds of load in each step (default: 60, at least 30).                             |
| `--capacity-restore`      | Clean up after a killed `--capacity` run (its pods and actors, and the `WorkerPool`'s size).            |
| `--guest ASSETS`          | Single run on another guest kernel, rootfs, or hypervisor. See [Testing a guest](#guest).               |
| `--cleanup-guest`         | Also remove the `SandboxConfig` and GCS uploads created from guest files.                               |
| `--keep`                  | Keep the actor at the end for debugging.                                                                |
| `--kubectl-ate PATH`      | `kubectl-ate` binary to use (default: `$KUBECTL_ATE` or `kubectl-ate` on `PATH`).                       |
| `--runner-image IMAGE`    | Prebuilt runner image with `python3` and `grpcio` (`$BASELINE_PERF_RUNNER_IMAGE`).                      |

## Benchmark setup {#benchmark}

### The actor image: SWE-Perf {#sweperf}

`bperf` runs one actor from the `swebench-astropy-7336` `ActorTemplate`
([swebench-astropy-7336-template.yaml](swebench-astropy-7336-template.yaml)).
Its image is the public [SWE-Perf](https://github.com/gke-labs/sweperf) image
for the SWE-bench task `astropy__astropy-7336`, which packs three things:

*   **The task's environment**: The astropy repo in `/testbed`, with its Python
    environment.
*   **A recorded agent session** (`/trace.json`): The 21 shell commands that an
    LLM coding agent ran to fix the task's bug (`@u.quantity_input` failing on a
    constructor annotated `-> None`).
*   **A replay server** (`/replay.py`, the container's command): `POST /execute`
    runs a range of steps in order, with `bash` in `/testbed`, and `GET /status`
    reports when they're done.

The template sets `SWEPERF_DISABLE_INTERNAL_SLEEP=1`, so the server doesn't add
simulated LLM "think time" between steps: a cycle's **Exec** time is spent
running its commands, not waiting.

The 21 steps are split into 4 cycles, the same way the boomer load client splits
them:

| Cycle | Steps | What the agent does                                                                                                                               |
| :---- | :---- | :------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1     | 1–6   | Lists the repo, reproduces the bug with a script (the first `import astropy`), reads the decorator's code, and tries `inspect` in another script. |
| 2     | 7–11  | Writes a helper script, patches `astropy/units/decorators.py` with `sed`, tries the `sed` command on a scratch file, and runs the patch again.    |
| 3     | 12–16 | Re-runs the reproduction, checks the patched lines, looks for the tests, and runs the first `pytest` file.                                        |
| 4     | 17–21 | Runs a second `pytest` file and a script that checks the fix, deletes its scratch files, and stages the diff.                                     |

### The actor's lifecycle {#lifecycle}

`bperf` replays the session twice, once for each way of parking an actor between
cycles. Each time, it uses a new actor, `perf-eval-actor` in the
`benchmark-workloads` atespace:

1.  **Create**: `CreateActor` from the template, after deleting any actor left
    over from an earlier run.
2.  **Cold start**: The first `ResumeActor` restores the template's golden
    snapshot, taken when the template was created. As soon as the replay server
    answers, `bperf` parks the actor, so every cycle resumes from the actor's
    own snapshot.
3.  **4 cycles**, each:
    1.  **Resume**: `ResumeActor`.
    2.  **Execute** (**Exec** in the report): `POST /execute` with the cycle's
        steps, then poll `GET /status` until the job is `COMPLETED`.
    3.  **Park**: `PauseActor` on the **Pause path** (the checkpoint stays on
        the node's local disk, and the next Resume restores it), or
        `SuspendActor` on the **Suspend path** (the snapshot is uploaded to GCS,
        and the next Resume downloads it).
4.  **Delete**: `DeleteActor`, unless you pass `--keep`.

The Pause path runs first, then the Suspend path. Every call is timed from a pod
inside the cluster (see [How it works](#how-it-measures)). The report gives the
median Resume, Exec, and Park of the 4 cycles, and the cold start. `--capacity`
runs the same suspend-path cycles in many actors at once; see [Measuring
capacity](#capacity-run).

## How it works {#how-it-measures}

*   **In-cluster runner (`baseline-perf-runner`)**: Runs inside
    `benchmark-workloads`, drives the [actor's lifecycle](#lifecycle), and makes
    all timed gRPC calls to `ate-api` and HTTP calls to `atenet-router`,
    avoiding workstation network latency. With `--capacity`, it drives each
    actor from its own thread.
*   **Node load sampler (`baseline-perf-sampler`)**: Runs on the worker node and
    reads host `/proc/stat`, `/proc/diskstats`, and `/proc/pressure/*` every
    second. A node counts as busy before the run if it has ≥ 2 busy cores, ≥ 10%
    disk utilization, or ≥ 5% PSI. With `--capacity`, it keeps running through
    the steps and gives each step's load.
*   **Log breakdown**: Each RPC carries a trace ID. After the timed run,
    `baseline-perf` collects matching log lines from `ate-api`, `atelet`, and
    `ateom` to break down Resume and Park into phases without any server
    changes.

## Cluster setup {#setup}

Prerequisites:

*   A GKE cluster with Substrate installed as your current `kubectl` context
    (for microVM, run `hack/install-microvm-deps.sh --install`).
*   `kubectl`, `kubectl-ate` (`go install ./cmd/kubectl-ate`), Python 3,
    `docker` (or pass `--runner-image`), and optionally `gcloud` (for node disk
    info and GCS snapshot sizes).

One-time cluster setup:

1.  **Deploy the benchmark worker pool** (from a Substrate checkout):

    ```shell
    export BUCKET_NAME=<your-snapshot-bucket>
    export KO_DOCKER_REPO=gcr.io/<your-project>/ate-images
    WORKLOAD_TEMPLATES=sleep ./benchmarking/workloads/deploy.sh --deploy \
        --sandbox-class microvm --worker-count 1
    ```

2.  **Create the `ActorTemplate`** from
    [swebench-astropy-7336-template.yaml](swebench-astropy-7336-template.yaml):

    ```shell
    envsubst '${BUCKET_NAME}' \
        < /google/src/head/depot/google3/experimental/users/jybao/baseline_perf/swebench-astropy-7336-template.yaml \
        | kubectl-ate create actor-template -f -
    ```

    Wait \~30 seconds for its golden snapshot to become ready, then run
    `baseline-perf`.

## The code {#the-code}

`baseline-perf` is the entry script; the implementation lives in `bperf/` and
uses only the Python standard library (except `runner.py`, which uses `grpcio`
inside the runner pod).

| Module            | What it does                                                                                    |
| :---------------- | :---------------------------------------------------------------------------------------------- |
| `cli.py`          | CLI flags, preflight checks, single-run, `--compare`, and `--capacity` orchestration.           |
| `common.py`       | Constants and step definitions shared with the runner pod.                                      |
| `runner.py`       | In-cluster runner: timed `ate-api` and `atenet-router` calls, for one actor or many at once.    |
| `pod.py`          | Builds/starts the runner and sampler pods.                                                      |
| `sampler.py`      | Samples `/proc` CPU, disk (utilization, MiB/s, IOPS), and PSI counters on the worker node.      |
| `kube.py`         | `kubectl`, `kubectl-ate`, and `gcloud` wrappers.                                                |
| `cluster_info.py` | Reads machine type, disk, Substrate version, and binary commits.                                |
| `logs.py`         | Fetches and parses `ate-api`, `atelet`, and `ateom` logs.                                       |
| `breakdown.py`    | Builds per-call phase trees from trace logs.                                                    |
| `guest.py`        | `--guest` asset upload, `SandboxConfig`, and temporary template copy.                           |
| `atelet.py`       | Builds `atelet` via `ko` and swaps/restores the worker node's pod.                              |
| `ab.py`           | `--compare` spec parsing, alternating run order, and comparison table.                          |
| `capacity.py`     | `--capacity` steps: `WorkerPool` scaling, each step's percentiles, the verdict, and its report. |
| `report.py`       | Formats the single-run report and snapshot sizes.                                               |

Modules start with `# fmt: off` and a `pylint: disable` comment to keep lines up
to \~120 columns with compact one-line docstrings.

Run the unit tests (no cluster needed):

```shell
cd experimental/users/jybao/baseline_perf
python3 -m unittest discover -s bperf -p '*_test.py'
```
