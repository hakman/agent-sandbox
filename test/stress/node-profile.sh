#!/bin/bash
# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

root=/tmp/stress-node-profile
socket=/run/containerd/debug.sock
export BPFTRACE_CACHE_USER_SYMBOLS=PER_PID
export BPFTRACE_MAX_MAP_KEYS=16384

case "$1" in
  preflight)
    # Exercise exec, exit and fsync probes before measuring any workload.
    bpftrace -B line -f json "${root}/node-profile.bt" 2 > "${root}/preflight.jsonl" &
    probe_pid=$!
    trap 'kill -INT "${probe_pid}" 2>/dev/null || true; wait "${probe_pid}" || true' EXIT
    for ((i=0; i<300; i++)); do
      if grep -q STRESS_PROFILE_READY "${root}/preflight.jsonl"; then break; fi
      kill -0 "${probe_pid}" 2>/dev/null || { wait "${probe_pid}"; exit 1; }
      sleep 0.1
    done
    grep -q STRESS_PROFILE_READY "${root}/preflight.jsonl"
    cp /usr/bin/dd "${root}/cilium-cni"
    "${root}/cilium-cni" if=/dev/zero of="${root}/probe-data" bs=4096 count=1 conv=fsync status=none
    wait "${probe_pid}"
    trap - EXIT
    rm "${root}/cilium-cni" "${root}/probe-data"
    cat "${root}/preflight.jsonl"
    grep -q '@process_lifetime_ns' "${root}/preflight.jsonl"
    grep -q '@syscall_ns' "${root}/preflight.jsonl"
    curl --fail --silent --show-error --max-time 5 --unix-socket "${socket}" \
      http://localhost/debug/pprof/ > /dev/null
    ;;
  start)
    dir="${root}/$2"
    mkdir "${dir}"
    nohup setsid bash "$0" run "$2" > "${dir}/capture.log" 2>&1 < /dev/null &
    echo "$!" > "${dir}/pid"
    for ((i=0; i<30; i++)); do
      if [[ -f "${dir}/ready" ]]; then exit 0; fi
      if [[ -f "${dir}/done" ]]; then cat "${dir}/capture.log" >&2; exit 1; fi
      sleep 1
    done
    echo 'Profiler did not attach within 30 seconds' >&2
    exit 1
    ;;
  run)
    dir="${root}/$2"
    bpf_pid='' cpu_pid='' trace_pid=''
    finish() {
      status=$?
      trap - EXIT TERM INT
      [[ -z "${bpf_pid}" ]] || kill -INT "${bpf_pid}" 2>/dev/null || true
      [[ -z "${cpu_pid}" ]] || kill -TERM "${cpu_pid}" 2>/dev/null || true
      [[ -z "${trace_pid}" ]] || kill -TERM "${trace_pid}" 2>/dev/null || true
      wait || true
      date -u '+end=%Y-%m-%dT%H:%M:%SZ' >> "${dir}/metadata.txt"
      echo "${status}" > "${dir}/done"
    }
    trap finish EXIT
    trap 'exit 143' TERM INT
    {
      uname -a
      bpftrace --version
      date -u '+start=%Y-%m-%dT%H:%M:%SZ'
      echo 'cpu_sample_hz=49 duration_seconds=20 trace_seconds=5'
    } > "${dir}/metadata.txt"
    bpftrace -B line -f json "${root}/node-profile.bt" 20 > "${dir}/bpftrace.jsonl" &
    bpf_pid=$!
    until grep -q STRESS_PROFILE_READY "${dir}/bpftrace.jsonl"; do
      kill -0 "${bpf_pid}" 2>/dev/null || { wait "${bpf_pid}"; exit 1; }
      sleep 0.1
    done
    date -u '+attached=%Y-%m-%dT%H:%M:%SZ' >> "${dir}/metadata.txt"
    # Write completed profiles atomically; interrupted HTTP bodies are unusable.
    curl --fail --silent --show-error --max-time 25 --unix-socket "${socket}" \
      'http://localhost/debug/pprof/profile?seconds=20' -o "${dir}/cpu.tmp" &
    cpu_pid=$!
    curl --fail --silent --show-error --max-time 10 --unix-socket "${socket}" \
      'http://localhost/debug/pprof/trace?seconds=5' -o "${dir}/trace.tmp" &
    trace_pid=$!
    touch "${dir}/ready"
    wait "${trace_pid}"
    trace_pid=
    mv "${dir}/trace.tmp" "${dir}/containerd.trace"
    wait "${cpu_pid}"
    cpu_pid=
    mv "${dir}/cpu.tmp" "${dir}/containerd.pprof"
    wait "${bpf_pid}"
    bpf_pid=
    grep -q '"type": "map"' "${dir}/bpftrace.jsonl"
    ;;
  wait)
    dir="${root}/$2"
    for ((i=0; i<40; i++)); do
      if [[ -f "${dir}/done" ]]; then exit "$(cat "${dir}/done")"; fi
      sleep 1
    done
    exit 1
    ;;
  stop)
    dir="${root}/$2"
    [[ -f "${dir}/pid" && ! -f "${dir}/done" ]] || exit 0
    # Signal only this capture's process group, never host runtime processes.
    pid=$(cat "${dir}/pid")
    if [[ ! -f "${dir}/done" ]]; then
      kill -TERM -- "-${pid}" 2>/dev/null || true
      for ((i=0; i<10; i++)); do
        if ! kill -0 -- "-${pid}" 2>/dev/null; then exit 0; fi
        sleep 1
      done
    fi
    kill -KILL -- "-${pid}" 2>/dev/null || true
    ;;
  *) exit 2 ;;
esac
