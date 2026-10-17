# Node startup profiling

Use node profiles to distinguish CNI CPU work, scheduler contention, blocking
syscalls, and containerd goroutine waits. Capture is opt-in because it adds load
to the selected worker. Compare with an unprofiled run before treating throughput
or latency changes as regressions.

For the kOps GCP scenario:

```sh
STRESS_PROFILE_NODE=auto dev/ci/presubmits/benchmarks-kops-gcp-cilium
```

`auto` selects the first ready, schedulable Linux worker by name. A node name can
be supplied instead. The scenario enables containerd's debug endpoint on
`/run/containerd/debug.sock` only when this variable is set. Presubmits leave
profiling disabled by default; use the same variable with the kindnet scenario
for a comparison.

On an existing test cluster, configure that Unix debug socket first, then run:

```sh
go run ./test/stress --profile-node=auto --phases=throughput-mif:100
```

The collector requires permission to create privileged, host-PID pods and exec
into them in `kube-system`. It uses an Ubuntu 24.04 pod with bpftrace and curl,
read-only mounts of `/sys` and `/run/containerd`, and the node's kernel BTF and
tracepoints. Tool installation and a live probe/socket preflight finish before
load starts. Failures abort the run instead of silently omitting profiles. The
pod is deleted on exit, even with `--cleanup=false`, and has a deadline as a
backstop if the harness is killed.

Each throughput phase starts a capture after five seconds:

- 20 seconds of CPU samples at 49 Hz, including containerd, Cilium, CNI binaries,
  containerd shims, and runc.
- Scheduler stacks separating blocked time from runnable queue delay. Threads
  without an observed wakeup are reported separately.
- Count, total and mean elapsed nanoseconds for selected networking, namespace,
  mount and filesystem-sync syscalls, and CNI/runc process lifetimes.
- A 20-second containerd Go CPU profile and a five-second Go execution trace.
  The trace exposes goroutine blocking that OS-thread stacks alone cannot explain.

Artifacts are listed in `node-profiles.json` and linked from the report's CPU
Profiles page. Metadata and readiness/end markers record the node capture
window; phase numbers keep repeated phase names distinct. Short or cancelled
phases stop their capture before the next phase, preserve partial eBPF output,
and mark the manifest as interrupted. Incomplete pprof/trace downloads are
removed. Phases ending before the initial five-second delay skip capture.
Artifact transfer is excluded from the phase duration.

For example (substitute a filename from your run):

```sh
go tool pprof -http=:8080 pprof-containerd-3-throughput-mif-100-worker.pprof
go tool trace node-profile-3-throughput-mif-100-worker.trace
```

The eBPF JSON maps contain CPU sample counts and nanoseconds of thread time.
They overlap with syscall elapsed time and process lifetime; do not add them as
sandbox latency. CNI ADD and DEL share a process name, and runtime subprocesses
may re-exec, so process lifetimes are not per-CRI-request spans. Captures include
only waits and process lifetimes whose boundaries were observed. Go binaries
without ELF symbols can leave user-stack addresses unresolved; kernel stacks,
syscall names, and the containerd Go profiles remain usable. Inspect the capture
log for lost-event or stack-resolution warnings before drawing conclusions.
