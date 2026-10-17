// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

//go:embed node-profile.bt
var nodeProfileBPF string

//go:embed node-profile.sh
var nodeProfileScript string

const nodeProfileRoot = "/tmp/stress-node-profile"

type nodeProfile struct {
	Phase       PhaseName   `json:"phase"`
	Number      PhaseNumber `json:"phaseNumber"`
	Node        string      `json:"node"`
	Files       []string    `json:"files"`
	Error       string      `json:"error,omitempty"`
	Interrupted bool        `json:"interrupted,omitempty"`
}

type nodeProfiler struct {
	node, pod, outputDir string
	kube                 kubernetes.Interface
	exec                 func(context.Context, []string, io.Reader, io.Writer) error
	profiles             []nodeProfile
}

func selectProfileNode(nodes []corev1.Node, requested string) (string, error) {
	slices.SortFunc(nodes, func(a, b corev1.Node) int { return strings.Compare(a.Name, b.Name) })
	for _, node := range nodes {
		if requested != "auto" && node.Name != requested {
			continue
		}
		u := &unstructured.Unstructured{}
		u.SetLabels(node.Labels)
		if isControlPlaneNode(u) || node.Spec.Unschedulable || node.Status.NodeInfo.OperatingSystem != "linux" {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				return node.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no ready, schedulable Linux worker matches --profile-node=%s", requested)
}

func newNodeProfiler(ctx context.Context, config *rest.Config, node, outputDir string) (*nodeProfiler, error) {
	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("building node profiler client: %w", err)
	}
	nodes, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing profiling nodes: %w", err)
	}
	node, err = selectProfileNode(nodes.Items, node)
	if err != nil {
		return nil, err
	}
	p := &nodeProfiler{node: node, kube: kube, outputDir: outputDir}
	spec := nodeProfilerPod(node)
	if deadline, ok := ctx.Deadline(); ok {
		spec.Spec.ActiveDeadlineSeconds = new(max(int64(time.Until(deadline).Seconds())+60, 60))
	}
	pod, err := kube.CoreV1().Pods("kube-system").Create(ctx, spec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating profiler on %s: %w", node, err)
	}
	p.pod = pod.Name
	p.exec = func(ctx context.Context, command []string, stdin io.Reader, stdout io.Writer) error {
		req := kube.CoreV1().RESTClient().Post().Namespace("kube-system").Resource("pods").Name(p.pod).SubResource("exec").
			VersionedParams(&corev1.PodExecOptions{Container: "profiler", Command: command, Stdin: stdin != nil, Stdout: true, Stderr: true}, scheme.ParameterCodec)
		executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
		if err != nil {
			return fmt.Errorf("building profiler exec: %w", err)
		}
		var stderr bytes.Buffer
		if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: stdin, Stdout: stdout, Stderr: &stderr}); err != nil {
			return fmt.Errorf("profiler exec: %w: %s", err, stderr.String())
		}
		if stderr.Len() > 0 {
			log.Printf("[node-profile] %s", stderr.String())
		}
		return nil
	}
	if err := p.prepare(ctx); err != nil {
		logCtx, logCancel := context.WithTimeout(context.Background(), 15*time.Second)
		raw, logErr := kube.CoreV1().Pods("kube-system").GetLogs(p.pod, &corev1.PodLogOptions{Container: "profiler"}).DoRaw(logCtx)
		logCancel()
		if logErr == nil {
			if writeErr := os.WriteFile(filepath.Join(outputDir, "node-profile-setup.log"), raw, 0o644); writeErr != nil {
				log.Printf("[node-profile] saving setup log: %v", writeErr)
			}
		}
		p.Close()
		return nil, err
	}
	log.Printf("[node-profile] profiling worker %s using pod %s", node, p.pod)
	return p, nil
}

func nodeProfilerPod(node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "stress-node-profiler-", Namespace: "kube-system"},
		Spec: corev1.PodSpec{
			NodeName: node, HostPID: true, HostNetwork: true, RestartPolicy: corev1.RestartPolicyNever,
			// Bound the lifetime even if the stress process is killed before cleanup.
			ActiveDeadlineSeconds: new(int64(3600)),
			Containers: []corev1.Container{{
				Name: "profiler", Image: "ubuntu:24.04",
				Command:         []string{"bash", "-ec", "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq bpftrace curl && mkdir -p " + nodeProfileRoot + " && touch /tmp/ready && exec sleep infinity"},
				SecurityContext: &corev1.SecurityContext{Privileged: new(true), RunAsUser: new(int64(0))},
				ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/ready"}}}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "sys", MountPath: "/sys", ReadOnly: true}, {Name: "containerd", MountPath: "/run/containerd", ReadOnly: true}},
			}},
			Volumes: []corev1.Volume{
				{Name: "sys", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/sys", Type: new(corev1.HostPathDirectory)}}},
				{Name: "containerd", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/run/containerd", Type: new(corev1.HostPathDirectory)}}},
			},
		},
	}
}

func (p *nodeProfiler) prepare(ctx context.Context) error {
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		pod, err := p.kube.CoreV1().Pods("kube-system").Get(ctx, p.pod, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if pod.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("profiler pod failed: %s", pod.Status.Message)
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("waiting for profiler tools on %s: %w", p.node, err)
	}
	for name, content := range map[string]string{"node-profile.bt": nodeProfileBPF, "node-profile.sh": nodeProfileScript} {
		if err := p.exec(ctx, []string{"bash", "-c", "cat > \"$1\"", "--", nodeProfileRoot + "/" + name}, strings.NewReader(content), io.Discard); err != nil {
			return fmt.Errorf("installing %s: %w", name, err)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	f, err := os.Create(filepath.Join(p.outputDir, "node-profile-preflight.log"))
	if err != nil {
		return err
	}
	defer f.Close()
	if err := p.exec(ctx, []string{"bash", nodeProfileRoot + "/node-profile.sh", "preflight"}, nil, f); err != nil {
		return fmt.Errorf("node profiling preflight: %w", err)
	}
	return nil
}

func (p *nodeProfiler) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.kube.CoreV1().Pods("kube-system").Delete(ctx, p.pod, metav1.DeleteOptions{GracePeriodSeconds: new(int64(0))}); err != nil && !apierrors.IsNotFound(err) {
		log.Printf("[node-profile] deleting profiler pod %s: %v", p.pod, err)
	}
}

// The returned function joins the capture before the next phase can start.
func (p *nodeProfiler) startPhase(ctx context.Context, phase PhaseName, number PhaseNumber, delay time.Duration) func() error {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			log.Printf("[node-profile] %s#%d ended before capture started", phase, number)
			done <- nil
		case <-time.After(delay):
			done <- p.capture(ctx, phase, number)
		}
	}()
	return func() error { cancel(); return <-done }
}

func (p *nodeProfiler) capture(ctx context.Context, phase PhaseName, number PhaseNumber) error {
	id := fmt.Sprintf("%d-%s-%s", number, fileSafePhase(phase), p.node)
	record := nodeProfile{Phase: phase, Number: number, Node: p.node, Files: []string{}}
	command := func(action string) []string {
		return []string{"bash", nodeProfileRoot + "/node-profile.sh", action, id}
	}
	captureCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()
	// Wait for the start acknowledgement even if the phase ends meanwhile,
	// so stop cannot race ahead of a detached process still being launched.
	startCtx, startCancel := context.WithTimeout(context.WithoutCancel(ctx), 40*time.Second)
	err := p.exec(startCtx, command("start"), nil, io.Discard)
	startCancel()
	if err != nil {
		// An unacknowledged start might still be executing remotely.
		defer p.Close()
		record.Error = err.Error()
	}
	if err == nil {
		err = p.exec(captureCtx, command("wait"), nil, io.Discard)
	}
	record.Interrupted = ctx.Err() != nil
	if err != nil && !record.Interrupted {
		record.Error = err.Error()
	}

	// Exec cancellation does not guarantee that remote children were killed.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	if stopErr := p.exec(cleanupCtx, command("stop"), nil, io.Discard); stopErr != nil {
		// Delete the collector before allowing another phase to run.
		p.Close()
		return fmt.Errorf("stopping node profile: %w", stopErr)
	}
	for _, artifact := range []struct{ remote, local string }{
		{"capture.log", "node-profile-" + id + ".log"},
		{"metadata.txt", "node-profile-" + id + ".txt"},
		{"bpftrace.jsonl", "node-profile-" + id + ".jsonl"},
		{"containerd.pprof", "pprof-containerd-" + id + ".pprof"},
		{"containerd.trace", "node-profile-" + id + ".trace"},
	} {
		// Missing files are expected when a short phase interrupts a capture.
		path := filepath.Join(p.outputDir, artifact.local)
		f, fileErr := os.Create(path)
		if fileErr != nil {
			return fmt.Errorf("creating node profile artifact: %w", fileErr)
		}
		copyErr := p.exec(cleanupCtx, []string{"cat", nodeProfileRoot + "/" + id + "/" + artifact.remote}, nil, f)
		closeErr := f.Close()
		if copyErr != nil || closeErr != nil {
			_ = os.Remove(path)
			if !record.Interrupted {
				record.Error += fmt.Sprintf("; collecting %s: %v", artifact.remote, errors.Join(copyErr, closeErr))
			}
			continue
		}
		record.Files = append(record.Files, artifact.local)
	}
	p.profiles = append(p.profiles, record)
	data, marshalErr := json.MarshalIndent(p.profiles, "", "  ")
	if marshalErr != nil {
		return marshalErr
	}
	if err := os.WriteFile(filepath.Join(p.outputDir, "node-profiles.json"), data, 0o644); err != nil {
		return err
	}
	if record.Error != "" {
		return fmt.Errorf("node profile %s: %s", id, record.Error)
	}
	return nil
}
