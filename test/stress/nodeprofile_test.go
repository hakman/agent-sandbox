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
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSelectProfileNode(t *testing.T) {
	worker := func(name string) corev1.Node {
		return corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.NodeStatus{
			NodeInfo:   corev1.NodeSystemInfo{OperatingSystem: "linux"},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		}}
	}
	cp := worker("a-control-plane")
	cp.Labels = map[string]string{"node-role.kubernetes.io/control-plane": ""}
	cordoned := worker("b-cordoned")
	cordoned.Spec.Unschedulable = true
	unready := worker("c-unready")
	unready.Status.Conditions = nil
	windows := worker("d-windows")
	windows.Status.NodeInfo.OperatingSystem = "windows"
	nodes := []corev1.Node{worker("z-worker"), cp, cordoned, unready, windows, worker("e-worker")}
	for _, tc := range []struct{ requested, want string }{{"auto", "e-worker"}, {"z-worker", "z-worker"}, {"a-control-plane", ""}, {"b-cordoned", ""}, {"c-unready", ""}, {"d-windows", ""}, {"missing", ""}} {
		t.Run(tc.requested, func(t *testing.T) {
			got, err := selectProfileNode(nodes, tc.requested)
			if got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestNodeProfileCancelledBeforeDelay(t *testing.T) {
	p := &nodeProfiler{exec: func(context.Context, []string, io.Reader, io.Writer) error {
		t.Error("cancelled phase started a remote profile")
		return nil
	}}
	finish := p.startPhase(t.Context(), "throughput-mif:1", 1, time.Hour)
	if err := finish(); err != nil {
		t.Fatal(err)
	}
}

func TestNodeProfileCancellationStopsBeforeReturning(t *testing.T) {
	started := make(chan struct{})
	var mu sync.Mutex
	var calls []string
	p := &nodeProfiler{node: "worker", outputDir: t.TempDir()}
	p.exec = func(ctx context.Context, args []string, _ io.Reader, out io.Writer) error {
		action := "copy"
		if args[0] != "cat" {
			action = args[2]
		}
		mu.Lock()
		calls = append(calls, action)
		mu.Unlock()
		switch action {
		case "start":
			return nil
		case "wait":
			close(started)
			<-ctx.Done()
			return ctx.Err()
		case "stop":
			if ctx.Err() != nil {
				t.Error("cleanup used cancelled context")
			}
			return nil
		default:
			if strings.HasSuffix(args[1], ".pprof") || strings.HasSuffix(args[1], ".trace") {
				return errors.New("incomplete profile")
			}
			_, err := io.WriteString(out, "partial capture\n")
			return err
		}
	}
	finish := p.startPhase(t.Context(), "throughput-mif:10", 2, 0)
	<-started
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(calls[:3], ",") != "start,wait,stop" {
		t.Fatalf("capture ordering: %v", calls)
	}
	data, err := os.ReadFile(filepath.Join(p.outputDir, "node-profiles.json"))
	if err != nil {
		t.Fatal(err)
	}
	var records []nodeProfile
	if err := json.Unmarshal(data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || !records[0].Interrupted || len(records[0].Files) != 3 {
		t.Fatalf("unexpected manifest: %s", data)
	}
	for _, pattern := range []string{"*.pprof", "*.trace"} {
		files, err := filepath.Glob(filepath.Join(p.outputDir, pattern))
		if err != nil || len(files) != 0 {
			t.Fatalf("partial files retained: %v, %v", files, err)
		}
	}
}

func TestNodeProfileRepeatedPhaseAndFailedCopy(t *testing.T) {
	p := &nodeProfiler{node: "worker", outputDir: t.TempDir()}
	failCopy := false
	p.exec = func(_ context.Context, args []string, _ io.Reader, out io.Writer) error {
		if args[0] != "cat" {
			return nil
		}
		if failCopy && strings.HasSuffix(args[1], ".trace") {
			return errors.New("connection lost")
		}
		_, err := io.WriteString(out, "completed capture")
		return err
	}
	for _, n := range []PhaseNumber{1, 2} {
		if err := p.capture(t.Context(), "throughput-mif:10", n); err != nil {
			t.Fatal(err)
		}
	}
	if p.profiles[0].Files[0] == p.profiles[1].Files[0] {
		t.Fatal("repeated phases overwrote artifacts")
	}
	failCopy = true
	if err := p.capture(t.Context(), "throughput-mif:10", 3); err == nil {
		t.Fatal("missing trace passed silently")
	}
	if p.profiles[2].Error == "" || len(p.profiles[2].Files) != 4 {
		t.Fatalf("missing failure details: %+v", p.profiles[2])
	}
}

func TestNodeProfileFailedStartDeletesCollector(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "collector", Namespace: "kube-system"}}
	kube := fake.NewClientset(pod)
	p := &nodeProfiler{node: "worker", pod: pod.Name, kube: kube, outputDir: t.TempDir()}
	p.exec = func(_ context.Context, args []string, _ io.Reader, out io.Writer) error {
		if args[0] != "cat" && args[2] == "start" {
			return errors.New("start acknowledgement lost")
		}
		_, err := io.WriteString(out, "diagnostics")
		return err
	}
	if err := p.capture(t.Context(), "throughput-mif:10", 1); err == nil {
		t.Fatal("failed start passed")
	}
	if _, err := kube.CoreV1().Pods("kube-system").Get(t.Context(), pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("collector was not deleted after an unacknowledged start: %v", err)
	}
	if len(p.profiles) != 1 || !strings.Contains(p.profiles[0].Error, "acknowledgement lost") {
		t.Fatalf("missing failure record: %+v", p.profiles)
	}
}
