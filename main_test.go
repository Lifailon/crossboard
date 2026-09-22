package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const samplePodJSON = `{
  "items": [
    {
      "metadata": {
        "name": "web-0",
        "namespace": "prod",
        "creationTimestamp": "2024-01-05T10:20:30Z",
        "labels": {"app": "web", "tier": "frontend"}
      },
      "spec": {
        "nodeName": "node-1",
        "volumes": [{"persistentVolumeClaim": {"claimName": "web-data"}}],
        "containers": [{
          "name": "web", "image": "nginx:1.25",
          "resources": {
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"memory": "512Mi"}
          }
        }],
        "initContainers": [{
          "name": "init-x", "image": "busybox:1.36",
          "resources": {"limits": {"cpu": "250m"}}
        }]
      },
      "status": {
        "phase": "Running",
        "qosClass": "Burstable",
        "podIP": "10.0.0.1",
        "containerStatuses": [
          {"name": "web", "ready": true, "restartCount": 2,
           "state": {"running": {"startedAt": "2024-01-05T10:20:31Z"}}}
        ],
        "initContainerStatuses": [
          {"name": "init-x", "ready": true, "restartCount": 0,
           "state": {"terminated": {"reason": "Completed"}}}
        ]
      }
    },
    {
      "metadata": {
        "name": "db-0",
        "namespace": "prod",
        "creationTimestamp": "2024-01-06T01:02:03Z",
        "labels": {"app": "db"}
      },
      "spec": {
        "nodeName": "node-2",
        "containers": [{
          "name": "db", "image": "postgres:16",
          "resources": {
            "requests": {"cpu": "1", "memory": "1Gi"},
            "limits": {"cpu": "2", "memory": "2Gi"}
          }
        }]
      },
      "status": {
        "phase": "Pending",
        "qosClass": "Guaranteed",
        "podIP": "",
        "containerStatuses": [
          {"name": "db", "ready": false, "restartCount": 0,
           "state": {"waiting": {"reason": "CrashLoopBackOff"}}}
        ]
      }
    }
  ]
}`

func TestSplitLines(t *testing.T) {
	got := splitLines("ctx-a\nctx-b\n\n  ctx-c  \n")
	want := []string{"ctx-a", "ctx-b", "ctx-c"}
	if len(got) != len(want) {
		t.Fatalf("splitLines: len=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("splitLines[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParsePodList(t *testing.T) {
	mounted := make(map[string]bool)
	pods, err := parsePodList("ctx-prod", []byte(samplePodJSON), nil, mounted)
	if err != nil {
		t.Fatalf("parsePodList: %v", err)
	}
	if len(pods) != 3 {
		t.Fatalf("expected 3 rows (web, init-x, db), got %d", len(pods))
	}
	if !mounted["prod|web-data"] {
		t.Errorf("expected mounted PVC prod|web-data, got %v", mounted)
	}

	web := pods[0]
	if web.ID != "ctx-prod|prod|web-0|web" {
		t.Errorf("web.ID = %q", web.ID)
	}
	if web.Context != "ctx-prod" || web.Namespace != "prod" || web.Name != "web-0" || web.Container != "web" {
		t.Errorf("web identity mismatch: %+v", web)
	}
	if web.Node != "node-1" || web.IP != "10.0.0.1" {
		t.Errorf("web node/ip mismatch: %+v", web)
	}
	if web.Phase != "Running" || web.PhaseCss != "ok" || web.State != "Running" || web.StateCss != "ok" {
		t.Errorf("web phase/state mismatch: %+v", web)
	}
	if !web.Ready || web.RestartCount != 2 {
		t.Errorf("web ready/restarts mismatch: %+v", web)
	}
	if web.Image != "nginx:1.25" {
		t.Errorf("web image = %q", web.Image)
	}
	if web.Created != "2024-01-05 10:20" {
		t.Errorf("web created = %q", web.Created)
	}

	initX := pods[1]
	if initX.Container != "init-x" || initX.State != "Terminated:Completed" || initX.StateCss != "err" {
		t.Errorf("init-x mismatch: %+v", initX)
	}
	if initX.Res != "-/250m · -/-" {
		t.Errorf("init-x res = %q", initX.Res)
	}

	db := pods[2]
	if db.Container != "db" || db.Phase != "Pending" || db.PhaseCss != "warn" {
		t.Errorf("db mismatch: %+v", db)
	}
	if db.Ready || db.State != "Waiting:CrashLoopBackOff" || db.StateCss != "warn" {
		t.Errorf("db state mismatch: %+v", db)
	}
	if db.Labels != "app=db" {
		t.Errorf("db labels = %q", db.Labels)
	}
	if db.Qos != "Guaranteed" || db.Res != "1/2 · 1Gi/2Gi" {
		t.Errorf("db qos/res mismatch: %+v", db)
	}
	if web.Labels != "app=web, tier=frontend" {
		t.Errorf("web labels = %q", web.Labels)
	}
	if web.Qos != "Burstable" {
		t.Errorf("web qos = %q", web.Qos)
	}
	if web.Res != "100m/- · 128Mi/512Mi" {
		t.Errorf("web res = %q", web.Res)
	}
	if web.Age == "" || !strings.HasSuffix(web.Age, "d") {
		t.Errorf("web age = %q, want something like 908d", web.Age)
	}
}

func TestJoinLabels(t *testing.T) {
	if got := joinLabels(nil); got != "" {
		t.Errorf("joinLabels(nil) = %q", got)
	}
	if got := joinLabels(map[string]string{"b": "2", "a": "1"}); got != "a=1, b=2" {
		t.Errorf("joinLabels sorted = %q", got)
	}
	if got := joinLabels(map[string]string{"only": "x"}); got != "only=x" {
		t.Errorf("joinLabels single = %q", got)
	}
}

func TestResPart(t *testing.T) {
	// Использование относительно лимита.
	if got, want := resPart("100m", "500m", "25m", cpuMilli), "25m/500m (5%)"; got != want {
		t.Errorf("resPart cpu = %q, want %q", got, want)
	}
	// Лимита нет — процент от запрошенных ресурсов.
	if got, want := resPart("200m", "", "50m", cpuMilli), "50m/200m (25%)"; got != want {
		t.Errorf("resPart no-limit = %q, want %q", got, want)
	}
	// Без потребления — падает на req/lim.
	if got, want := resPart("100m", "500m", "", cpuMilli), "100m/500m"; got != want {
		t.Errorf("resPart no-usage = %q, want %q", got, want)
	}
	// Целые ядра и двоичные/десятичные суффиксы памяти.
	if got, want := resPart("1", "2", "500m", cpuMilli), "500m/2 (25%)"; got != want {
		t.Errorf("resPart cores = %q, want %q", got, want)
	}
	if got, want := resPart("128Mi", "512Mi", "64Mi", memBytes), "64Mi/512Mi (13%)"; got != want {
		t.Errorf("resPart mem = %q, want %q", got, want)
	}
	if got, want := resPart("1Gi", "4Gi", "1Gi", memBytes), "1Gi/4Gi (25%)"; got != want {
		t.Errorf("resPart GiB = %q, want %q", got, want)
	}
}

func TestTopPodsUsage(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") != "--context c1 top pods -A --no-headers" {
			return []byte("boom"), fmt.Errorf("unexpected: %s", strings.Join(args, " "))
		}
		return []byte("web-0          prod      12m     40Mi\ndb-0           prod      1       132Mi\n"), nil
	}
	defer func() { runKubectlFn = oldRun }()

	usage := topPodsUsage("c1")
	if got, want := usage["prod|web-0"], [2]string{"12m", "40Mi"}; got != want {
		t.Errorf("usage web-0 = %v, want %v", got, want)
	}
	if got, want := usage["prod|db-0"], [2]string{"1", "132Mi"}; got != want {
		t.Errorf("usage db-0 = %v, want %v", got, want)
	}
}

func TestAgeString(t *testing.T) {
	if got := ageString(time.Time{}); got != "" {
		t.Errorf("ageString(zero) = %q", got)
	}
	for _, tc := range []struct {
		ago  time.Duration
		want string // suffix / prefix
	}{
		{40 * time.Second, "s"},
		{3 * time.Hour, "h"},
		{3 * 24 * time.Hour, "d"},
		{7 * 24 * time.Hour, "d"},
	} {
		got := ageString(time.Now().Add(-tc.ago))
		if !strings.HasSuffix(got, tc.want) {
			t.Errorf("ageString(-%v) = %q, want suffix %q", tc.ago, got, tc.want)
		}
	}
}

func TestParsePodListInvalidJSON(t *testing.T) {
	if _, err := parsePodList("ctx", []byte("{not json"), nil, nil); err == nil {
		t.Fatal("expected error for invalid json")
	}
}

func TestParsePodListNoContainers(t *testing.T) {
	data := []byte(`{"items":[{"metadata":{"name":"hang-pod","namespace":"ns"},"spec":{},"status":{"phase":"Pending"}}]}`)
	pods, err := parsePodList("ctx", data, nil, nil)
	if err != nil {
		t.Fatalf("parsePodList: %v", err)
	}
	if len(pods) != 0 {
		t.Fatalf("expected 0 rows for pod without containers, got %d", len(pods))
	}
}

func TestCollectStats(t *testing.T) {
	pods := []Pod{
		{Context: "c1", Namespace: "ns1", Name: "a", Container: "a", Phase: "Running", State: "Running", Ready: true},
		{Context: "c1", Namespace: "ns1", Name: "a", Container: "b", Phase: "Running", State: "Running", Ready: true, RestartCount: 2},
		{Context: "c1", Namespace: "ns2", Name: "b", Container: "x", Phase: "Pending", State: "Waiting", Ready: false, RestartCount: 5},
		{Context: "c2", Namespace: "ns1", Name: "c", Container: "y", Phase: "Running", State: "Running", Ready: true},
		// Succeeded не считается проблемным подом, но и не Running.
		{Context: "c1", Namespace: "ns2", Name: "s", Container: "z", Phase: "Succeeded", State: "Terminated:Completed", Ready: false, RestartCount: 0},
		// Phase Running, но контейнер в Waiting — должен попасть в Running (по фазе).
		{Context: "c2", Namespace: "ns1", Name: "d", Container: "z", Phase: "Running", State: "Waiting:CrashLoopBackOff", Ready: false, RestartCount: 9},
	}
	st := collectStats(pods)
	if st.Contexts != 2 {
		t.Errorf("Contexts = %d, want 2", st.Contexts)
	}
	if st.Namespaces != 2 {
		t.Errorf("Namespaces = %d, want 2", st.Namespaces)
	}
	if st.Pods != 5 {
		t.Errorf("Pods = %d, want 5", st.Pods)
	}
	if st.Containers != 6 {
		t.Errorf("Containers = %d, want 6", st.Containers)
	}
	if st.Running != 4 {
		t.Errorf("Running = %d, want 4 (by phase, includes d/z)", st.Running)
	}
	if st.NotReady != 3 {
		t.Errorf("NotReady = %d, want 3", st.NotReady)
	}
	if st.Restarts != 16 {
		t.Errorf("Restarts = %d, want 16", st.Restarts)
	}
}

func TestBuildCards(t *testing.T) {
	cards := buildCards(Stats{
		AvailCtx: 2, Contexts: 3,
		Namespaces: 3,
		NodeReady:  4, NodeTotal: 5,
		Pods: 10, PodsRunning: 8,
		Containers: 20, Running: 15,
		Services: 7,
		CpuMilli: 1200, CpuTotalMilli: 4000,
		MemBytes: 2 << 30, MemTotalBytes: 4 << 30,
		PVCInUse: 2, PVCTotal: 5, PVCUsedBytes: 1 << 30, PVCTotalBytes: 2 << 30, PVCOk: 1,
		NotReady: 3, Restarts: 16,
	})
	if len(cards) != 12 {
		t.Fatalf("len(cards) = %d, want 12", len(cards))
	}
	row1 := []string{"Clusters", "Namespaces", "Nodes", "Pods", "Containers", "Services", "Not ready", "Restarts"}
	row2 := []string{"CPU", "Memory", "PVC count", "PVC size"}
	for i, l := range append(row1, row2...) {
		if cards[i].Label != l {
			t.Errorf("cards[%d].Label = %q, want %q", i, cards[i].Label, l)
		}
	}
	scopes := []string{"ctx", "ns", "nodes", "", "", "svc", "notready", "restarts", "", "", "pvc", "pvc"}
	for i, s := range scopes {
		if cards[i].Scope != s {
			t.Errorf("cards[%d].Scope = %q, want %q (%s)", i, cards[i].Scope, s, cards[i].Label)
		}
	}
	if cards[0].Value != "2/3" || cards[2].Value != "4/5" {
		t.Errorf("clusters/nodes = %+v", cards)
	}
	if cards[3].Value != "8/10" || cards[4].Value != "15/20" || cards[5].Value != "7" {
		t.Errorf("pods/containers/services = %+v", cards[:6])
	}
	if cards[6].Value != "3" || cards[7].Value != "16" {
		t.Errorf("not ready/restarts = %+v", cards[6:8])
	}
	if cards[8].Value != "1.20 / 4.00" || cards[8].Hint != "cores in use / allocatable" {
		t.Errorf("cpu card: %+v", cards[8])
	}
	if cards[9].Value != "2.00 / 4.00 GiB" {
		t.Errorf("memory card: %+v", cards[9])
	}
	if cards[10].Value != "2/5" {
		t.Errorf("pvc count card: %+v", cards[10])
	}
	if cards[11].Value != "1.00 / 2.00 GiB" || cards[11].Label != "PVC size" {
		t.Errorf("pvc size card: %+v", cards[11])
	}
}

func TestVolumeCountValue(t *testing.T) {
	if got := volumeCountValue(Stats{PVCOk: 1, PVCInUse: 2, PVCTotal: 5}); got != "2/5" {
		t.Errorf("volumeCountValue ok = %q, want 2/5", got)
	}
	if got := volumeCountValue(Stats{PVCInUse: 3}); got != "3" {
		t.Errorf("volumeCountValue fallback = %q, want 3", got)
	}
	if got := volumeCountValue(Stats{}); got != "0" {
		t.Errorf("volumeCountValue empty = %q, want 0", got)
	}
}

func TestServicesByContext(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") != "--context c1 get svc -A -o json" {
			return nil, fmt.Errorf("unexpected: %s", strings.Join(args, " "))
		}
		return []byte(`{"items":[{},{},{}]}`), nil
	}
	if n := servicesByContext("c1"); n != 3 {
		t.Errorf("services = %d, want 3", n)
	}

	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte("Forbidden"), fmt.Errorf("exit status 1")
	}
	if n := servicesByContext("c1"); n != 0 {
		t.Errorf("services on RBAC error = %d, want 0", n)
	}
}

func TestParseSelection(t *testing.T) {
	sel, ok := parseSelection("ctx|ns|pod|container")
	if !ok || sel.Context != "ctx" || sel.Namespace != "ns" || sel.Pod != "pod" || sel.Container != "container" {
		t.Errorf("parseSelection full: %+v ok=%v", sel, ok)
	}
	sel, ok = parseSelection("ctx|ns|pod")
	if !ok || sel.Container != "" {
		t.Errorf("parseSelection without container: %+v ok=%v", sel, ok)
	}
	if _, ok = parseSelection("only-two"); ok {
		t.Error("expected failure for malformed selection")
	}
	if _, ok = parseSelection(""); ok {
		t.Error("expected failure for empty selection")
	}
}

func TestParseSelectionsDedup(t *testing.T) {
	sels := parseSelections([]string{
		"c1|ns|p1|c1",
		"c1|ns|p1|c1", // дубликат
		"bad",
		"c2|ns|p2|",
		"c2|ns|p2|c2",
	})
	if len(sels) != 3 {
		t.Fatalf("len(sels) = %d, want 3 (%+v)", len(sels), sels)
	}
}

func TestParseLogLineWithTimestamp(t *testing.T) {
	le := parseLogLine(Selection{"c1", "ns1", "pod1", "ct1"},
		"2024-05-01T12:34:56.123456789Z INFO hello world")
	if le.Timestamp != "2024-05-01T12:34:56.123456789Z" {
		t.Errorf("Timestamp = %q", le.Timestamp)
	}
	if le.Message != "INFO hello world" {
		t.Errorf("Message = %q", le.Message)
	}
	if le.Pod != "pod1" || le.Container != "ct1" || le.Context != "c1" {
		t.Errorf("metadata mismatch: %+v", le)
	}
}

func TestParseLogLinePlain(t *testing.T) {
	le := parseLogLine(Selection{"c1", "ns1", "pod1", "ct1"}, "this is a plain line")
	if le.Timestamp != "" {
		t.Errorf("Timestamp = %q, want empty", le.Timestamp)
	}
	if le.Message != "this is a plain line" {
		t.Errorf("Message = %q", le.Message)
	}
}

func TestParseLogLineEmpty(t *testing.T) {
	le := parseLogLine(Selection{"c1", "ns1", "pod1", "ct1"}, "")
	if le.Message != "" {
		t.Errorf("Message = %q, want empty", le.Message)
	}
}

// fakeSource возвращает фиксированные строки для заданного пода.
func fakeSource(linesByPod map[string]string) sourceFunc {
	return func(sel Selection, _ string, _ bool) (io.ReadCloser, error) {
		text, ok := linesByPod[sel.Pod]
		if !ok {
			return nil, fmt.Errorf("no source for %s", sel.Pod)
		}
		return io.NopCloser(strings.NewReader(text)), nil
	}
}

func TestRunLogStreamMultiplexesSources(t *testing.T) {
	src := fakeSource(map[string]string{
		"podA": "2024-01-01T00:00:00.000000000Z a1\nplain-a2\n",
		"podB": "b1-only\n",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := runLogStream(ctx, []Selection{
		{Context: "c1", Namespace: "nsA", Pod: "podA", Container: "ctA"},
		{Context: "c2", Namespace: "nsB", Pod: "podB", Container: "ctB"},
	}, "1h", true, src)

	var errs, lines []LogStreamEvent
	for ev := range events {
		if ev.Err != nil {
			errs = append(errs, ev)
		} else {
			lines = append(lines, ev)
		}
	}

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %+v", errs)
	}
	if len(lines) != 3 {
		t.Fatalf("expected 3 log lines, got %d", len(lines))
	}

	byPod := map[string][]string{}
	for _, ev := range lines {
		byPod[ev.Sel.Pod] = append(byPod[ev.Sel.Pod], ev.Line.Message)
	}
	if got := strings.Join(byPod["podA"], ","); got != "a1,plain-a2" {
		t.Errorf("podA messages = %q", got)
	}
	if got := strings.Join(byPod["podB"], ","); got != "b1-only" {
		t.Errorf("podB messages = %q", got)
	}

	// Проверяем метаданные каждой строки.
	for _, ev := range lines {
		if ev.Line.Container != ev.Sel.Container || ev.Line.Context != ev.Sel.Context {
			t.Errorf("metadata mismatch: %+v", ev.Line)
		}
		if ev.Sel.Pod == "podA" && ev.Line.Message == "a1" && ev.Line.Timestamp == "" {
			t.Errorf("строка 'a1': ожидали timestamp из kubectl --timestamps")
		}
		if ev.Sel.Pod == "podA" && ev.Line.Message == "plain-a2" && ev.Line.Timestamp != "" {
			t.Errorf("строка 'plain-a2' не должна иметь timestamp, получили %q", ev.Line.Timestamp)
		}
	}
}

func TestRunLogStreamSourceError(t *testing.T) {
	src := fakeSource(map[string]string{"podA": "ok\n"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := runLogStream(ctx, []Selection{
		{Context: "c1", Namespace: "ns", Pod: "podA", Container: "ctA"},
		{Context: "c2", Namespace: "ns", Pod: "podMISSING", Container: "ctB"},
	}, "", false, src)

	var errs int
	var lines int
	for ev := range events {
		if ev.Err != nil {
			errs++
			if ev.Sel.Pod != "podMISSING" {
				t.Errorf("error sel = %+v, want podMISSING", ev.Sel)
			}
		} else {
			lines++
		}
	}
	if errs != 1 {
		t.Errorf("errs = %d, want 1", errs)
	}
	if lines != 1 {
		t.Errorf("lines = %d, want 1", lines)
	}
}

// blockRC блокирует Read до тех пор, пока не будет вызван Close.
type blockRC struct {
	mu     sync.Mutex
	closed bool
	readC  chan struct{}
}

func (b *blockRC) Read(p []byte) (int, error) {
	<-b.readC
	return 0, io.EOF
}

func (b *blockRC) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		close(b.readC)
	}
	return nil
}

func TestRunLogStreamKillsOnCancel(t *testing.T) {
	var rc *blockRC
	src := func(sel Selection, since string, follow bool) (io.ReadCloser, error) {
		rc = &blockRC{readC: make(chan struct{})}
		return rc, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // сразу отменённый контекст

	events := runLogStream(ctx, []Selection{
		{Context: "c1", Namespace: "ns", Pod: "podA", Container: "ctA"},
	}, "1h", true, src)

	for ev := range events {
		if ev.Err == nil && ev.Line.Message != "" {
			t.Fatalf("ожидали пусто, но получили строку: %+v", ev.Line)
		}
	}
	if rc == nil {
		t.Fatal("source не был вызван")
	}
	rc.mu.Lock()
	closed := rc.closed
	rc.mu.Unlock()
	if !closed {
		t.Error("source должен быть закрыт при отмене контекста")
	}
}

// ---------- HTTP-хендлеры ----------

func withFakes(t *testing.T, pods []Pod, ctxs []string) func() {
	oldF, oldCtx := fetchAllFn, getContextsFn
	fetchAllFn = func() Overview { return Overview{Pods: pods, TotalCtx: len(ctxs), AvailCtx: len(ctxs)} }
	getContextsFn = func() ([]string, error) { return ctxs, nil }
	return func() {
		fetchAllFn, getContextsFn = oldF, oldCtx
	}
}

func fakePods() []Pod {
	return []Pod{
		{ID: "k8s-prod|prod|web-0|web", Context: "k8s-prod", Namespace: "prod", Name: "web-0",
			Container: "web", Node: "node-1", Phase: "Running", PhaseCss: "ok",
			State: "Running", StateCss: "ok", Ready: true, RestartCount: 1, Image: "nginx:1.25",
			Labels: "app=web", Qos: "Burstable", Res: "100m/- · 128Mi/512Mi",
			Cpu: "100m/-", Mem: "128Mi/512Mi",
			Created: "2024-01-05 10:20", Age: "500d"},
		{ID: "k8s-prod|prod|db-0|db", Context: "k8s-prod", Namespace: "prod", Name: "db-0",
			Container: "db", Node: "node-2", Phase: "Pending", PhaseCss: "warn",
			State: "Waiting:CrashLoopBackOff", StateCss: "warn", Ready: false, RestartCount: 5,
			Image: "postgres:16", Qos: "Guaranteed", Res: "1/2 · 1Gi/2Gi",
			Cpu: "1/2", Mem: "1Gi/2Gi",
			Created: "2024-01-06 01:02", Age: "499d"},
	}
}

func TestIndexHandlerRendersPage(t *testing.T) {
	defer withFakes(t, fakePods(), []string{"k8s-prod"})()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handleIndex(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"KubeLogs",
		"web-0",
		"db-0",
		"k8s-prod",
		"nginx:1.25",
		"Clusters",
		"Namespaces",
		"row-check",
		"CrashLoopBackOff",
		"100m/-",
		"128Mi/512Mi",
		"data-key=\"cpu\"",
		"data-key=\"age\"",
		"selected",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("страница не содержит %q", want)
		}
	}
}

func TestIndexHandlerNoData(t *testing.T) {
	defer withFakes(t, nil, nil)()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handleIndex(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "No pods found") {
		t.Error("ожидали сообщение об отсутствии подов")
	}
}

func TestIndexHandlerFetchErrorShown(t *testing.T) {
	oldF, oldCtx := fetchAllFn, getContextsFn
	fetchAllFn = func() Overview {
		return Overview{Error: "kubectl: access denied for cluster A", TotalCtx: 1}
	}
	getContextsFn = func() ([]string, error) { return []string{"c1"}, nil }
	defer func() { fetchAllFn, getContextsFn = oldF, oldCtx }()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handleIndex(rec, req)
	if !strings.Contains(rec.Body.String(), "access denied for cluster A") {
		t.Error("ожидали баннер с ошибкой")
	}
}

func TestAPIHandlerJSON(t *testing.T) {
	defer withFakes(t, fakePods(), []string{"k8s-prod"})()
	req := httptest.NewRequest(http.MethodGet, "/api/pods", nil)
	rec := httptest.NewRecorder()
	handleAPI(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{`"pods":`, `"contexts":`, `"nodes":`, `"phases":`, `"cards":`, `"labels":`} {
		if !strings.Contains(body, want) {
			t.Errorf("json не содержит %q", want)
		}
	}
	var data PageData
	if err := json.Unmarshal(rec.Body.Bytes(), &data); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(data.Pods) != 2 {
		t.Errorf("len(Pods) = %d, want 2", len(data.Pods))
	}
	if data.Stats.Contexts != 1 || data.Stats.AvailCtx != 1 {
		t.Errorf("stats mismatch: %+v", data.Stats)
	}
	if len(data.Cards) != 12 {
		t.Errorf("len(Cards) = %d, want 12", len(data.Cards))
	}
}

func TestLogsHandlerStreamsSSE(t *testing.T) {
	old := openLogStreamFn
	openLogStreamFn = func(sel Selection, since string, follow bool) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(
			"2024-01-02T03:04:05.000000000Z line-alpha\nline-beta\n")), nil
	}
	defer func() { openLogStreamFn = old }()

	srv := httptest.NewServer(http.HandlerFunc(handleLogs))
	defer srv.Close()

	sel := "k8s-prod|prod|web-0|web"
	resp, err := http.Get(srv.URL + "/logs?sel=" + url.QueryEscape(sel) + "&since=1h&follow=1")
	if err != nil {
		t.Fatalf("GET /logs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	body := string(bodyBytes)

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q", ct)
	}
	for _, want := range []string{
		"data: {",
		"\"context\":\"k8s-prod\"",
		"\"pod\":\"web-0\"",
		"line-alpha",
		"line-beta",
		"event: done",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("SSE-поток не содержит %q", want)
		}
	}
}

func TestLogsHandlerNoSelection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(handleLogs))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/logs")
	if err != nil {
		t.Fatalf("GET /logs: %v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(bodyBytes), "error") {
		t.Error("ожидали event: error для пустого выбора")
	}
}

func TestLogsHandlerEmptySelectionIgnored(t *testing.T) {
	old := openLogStreamFn
	openLogStreamFn = func(sel Selection, since string, follow bool) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("hello\n")), nil
	}
	defer func() { openLogStreamFn = old }()

	srv := httptest.NewServer(http.HandlerFunc(handleLogs))
	defer srv.Close()
	// Используем URL-кодирование для обоих sel, в т.ч. невалидного.
	raw := url.QueryEscape("good|ns1|pod1|c1") + "&sel=" + url.QueryEscape("мусор")
	resp, err := http.Get(srv.URL + "/logs?sel=" + raw)
	if err != nil {
		t.Fatalf("GET /logs: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "\"pod\":\"pod1\"") {
		t.Errorf("ожидали строку от валидной выборки, получили: %q", s)
	}
	if strings.Contains(s, "error") && strings.Contains(s, "мусор") {
		t.Error("невалидная выборка не должна создавать поток")
	}
}

func TestGetDefaultNamespace(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "jsonpath") {
			return []byte("dev-ns"), nil
		}
		return nil, fmt.Errorf("unexpected call: %v", args)
	}
	defer func() { runKubectlFn = oldRun }()
	if got := getDefaultNamespace("ctx"); got != "dev-ns" {
		t.Errorf("getDefaultNamespace = %q, want dev-ns", got)
	}
}

func TestGetDefaultNamespaceFallsBackToDefault(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) { return []byte(""), nil }
	defer func() { runKubectlFn = oldRun }()
	if got := getDefaultNamespace("ctx"); got != "default" {
		t.Errorf("getDefaultNamespace = %q, want default", got)
	}
}

func withFakeKubectl(t *testing.T, fn func(args ...string) ([]byte, error)) func() {
	oldCtx, oldRun := getContextsFn, runKubectlFn
	getContextsFn = func() ([]string, error) { return []string{"prod-ctx"}, nil }
	runKubectlFn = fn
	return func() {
		getContextsFn, runKubectlFn = oldCtx, oldRun
	}
}

func TestFetchPodsRealFallsBackToDefaultNamespace(t *testing.T) {
	var calls []string
	fn := func(args ...string) ([]byte, error) {
		joined := strings.Join(args, " ")
		calls = append(calls, joined)
		switch {
		case strings.Contains(joined, "config view"):
			return []byte(""), nil // без конфигурированного namespace
		case strings.Contains(joined, "get pods -A"):
			return []byte("Error from server (Forbidden): pods is forbidden"), fmt.Errorf("exit status 1")
		case strings.Contains(joined, "get pods -n"):
			return []byte(samplePodJSON), nil
		}
		return nil, fmt.Errorf("unexpected call: %v", args)
	}
	defer withFakeKubectl(t, fn)()

	ov := fetchAllReal()
	if ov.Error != "" {
		t.Fatalf("errMsg = %q", ov.Error)
	}
	if len(ov.Pods) != 3 {
		t.Fatalf("len(pods) = %d, want 3", len(ov.Pods))
	}
	if ov.AvailCtx != 1 || ov.TotalCtx != 1 {
		t.Errorf("availability = %d/%d, want 1/1", ov.AvailCtx, ov.TotalCtx)
	}
	foundFallback := false
	for _, c := range calls {
		if strings.Contains(c, "get pods -n default") {
			foundFallback = true
		}
	}
	if !foundFallback {
		t.Errorf("expected fallback call with -n default, calls: %v", calls)
	}
}

func TestFetchPodsRealNoFallbackWhenAllWorks(t *testing.T) {
	var calls []string
	fn := func(args ...string) ([]byte, error) {
		joined := strings.Join(args, " ")
		calls = append(calls, joined)
		if strings.Contains(joined, "get pods -A") {
			return []byte(samplePodJSON), nil
		}
		return nil, fmt.Errorf("unexpected call: %v", args)
	}
	defer withFakeKubectl(t, fn)()

	ov := fetchAllReal()
	if ov.Error != "" {
		t.Fatalf("errMsg = %q", ov.Error)
	}
	if len(ov.Pods) != 3 {
		t.Fatalf("len(pods) = %d, want 3", len(ov.Pods))
	}
	for _, c := range calls {
		if strings.Contains(c, "config view") || strings.Contains(c, "get pods -n") {
			t.Errorf("fallback не должен срабатывать, но есть вызов: %q", c)
		}
	}
}

func TestFetchPodsRealBothFailuresReported(t *testing.T) {
	fn := func(args ...string) ([]byte, error) {
		joined := strings.Join(args, " ")
		if strings.Contains(joined, "config view") {
			return []byte(""), nil
		}
		return []byte("boom"), fmt.Errorf("exit status 1")
	}
	defer withFakeKubectl(t, fn)()

	ov := fetchAllReal()
	if len(ov.Pods) != 0 {
		t.Errorf("len(pods) = %d, want 0", len(ov.Pods))
	}
	if ov.AvailCtx != 0 {
		t.Errorf("AvailCtx = %d, want 0", ov.AvailCtx)
	}
	if !strings.Contains(ov.Error, "default") {
		t.Errorf("errMsg должен упоминать fallback namespace: %q", ov.Error)
	}
}

func TestNodesContextParsesJSON(t *testing.T) {
	fn := func(args ...string) ([]byte, error) {
		return []byte(`{"items":[
			{"metadata":{"name":"n1"},"status":{"conditions":[{"type":"Ready","status":"True"}],"allocatable":{"cpu":"2","memory":"8127596Ki"}}},
			{"metadata":{"name":"n2"},"status":{"conditions":[{"type":"Ready","status":"Unknown"}],"allocatable":{"cpu":"500m","memory":"1Gi"}}},
			{"metadata":{"name":"n3"},"status":{"conditions":[{"type":"Ready","status":"False"}],"allocatable":{"cpu":"1","memory":"512Mi"}}}
		]}`), nil
	}
	defer withFakeKubectl(t, fn)()
	ready, total, cpu, mem := nodesContext("c1")
	if ready != 1 || total != 3 {
		t.Errorf("ready/total = %d/%d, want 1/3", ready, total)
	}
	if cpu != 3500 { // 2 + 500m + 1
		t.Errorf("cpu = %d, want 3500", cpu)
	}
	if mem != 8127596<<10+1<<30+512<<20 {
		t.Errorf("mem = %d, want sum of allocatable", mem)
	}
}

func TestPVCByContextParsesJSON(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") != "--context c1 get pvc -A -o json" {
			return nil, fmt.Errorf("unexpected: %s", strings.Join(args, " "))
		}
		return []byte(`{"items":[
			{"metadata":{"name":"web-data","namespace":"prod"},"spec":{"resources":{"requests":{"storage":"10Gi"}}}},
			{"metadata":{"name":"db-data","namespace":"prod"},"spec":{"resources":{"requests":{"storage":"200Gi"}}}},
			{"metadata":{"name":"logs","namespace":"ops"},"spec":{"resources":{"requests":{"storage":"5Gi"}}}}
		]}`), nil
	}
	defer func() { runKubectlFn = oldRun }()

	inUse, total, used, totalBytes, ok := pvcByContext("c1", map[string]bool{"prod|web-data": true})
	if !ok {
		t.Error("ok = false, want true")
	}
	if inUse != 1 || total != 3 {
		t.Errorf("inUse/total = %d/%d, want 1/3", inUse, total)
	}
	if used != 10<<30 || totalBytes != 215<<30 {
		t.Errorf("used = %d, total = %d; want 10GiB, 215GiB", used, totalBytes)
	}
}

func TestPVCByContextRBACDeniedFallsBackToMounted(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte("Error from server (Forbidden): persistentvolumeclaims is forbidden"), fmt.Errorf("exit status 1")
	}
	defer func() { runKubectlFn = oldRun }()

	inUse, total, used, totalBytes, ok := pvcByContext("c1", map[string]bool{"prod|web-data": true, "ops|logs": true})
	if ok {
		t.Error("ok = true, want false when listing denied")
	}
	if inUse != 2 {
		t.Errorf("inUse = %d, want 2 (fallback to mounted)", inUse)
	}
	if total != 0 || used != 0 || totalBytes != 0 {
		t.Errorf("expected zeros, got total=%d used=%d bytes=%d", total, used, totalBytes)
	}
}

func TestUniqueNodesAndPhases(t *testing.T) {
	pods := []Pod{
		{Node: "b", Phase: "Running"},
		{Node: "a", Phase: "Pending"},
		{Node: "a", Phase: "Running"},
		{Node: "", Phase: "Failed"},
	}
	nodes := uniqueNodes(pods)
	if strings.Join(nodes, ",") != "a,b" {
		t.Errorf("uniqueNodes = %v", nodes)
	}
	ph := uniquePhases(pods)
	if strings.Join(ph, ",") != "Failed,Pending,Running" {
		t.Errorf("uniquePhases = %v", ph)
	}
}

func TestObjectHandlerReturnsYAML(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if strings.Contains(j, "get configmap") {
			return []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app-conf\n  namespace: prod\n"), nil
		}
		return nil, fmt.Errorf("unexpected: %v", args)
	}
	defer func() { runKubectlFn = oldRun }()

	req := httptest.NewRequest(http.MethodGet,
		"/api/object?ctx=c1&ns=prod&name=app-conf&kind=configmap", nil)
	rec := httptest.NewRecorder()
	handleObject(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp struct {
		Kind  string
		Name  string
		Yaml  string
		Error string
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if resp.Kind != "configmap" || resp.Name != "app-conf" {
		t.Errorf("resp kind/name = %q/%q", resp.Kind, resp.Name)
	}
	if resp.Error != "" || !strings.Contains(resp.Yaml, "kind: ConfigMap") {
		t.Errorf("resp = %+v", resp)
	}
	if !strings.Contains(rec.Body.String(), `"kind":"configmap"`) {
		t.Errorf("lowercase json keys expected: %s", rec.Body.String())
	}
}

func TestObjectHandlerMissingName(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/object?ctx=c1", nil)
	rec := httptest.NewRecorder()
	handleObject(rec, req)
	if !strings.Contains(rec.Body.String(), "no object name") {
		t.Errorf("body = %s", rec.Body.String())
	}
}

func TestClusterSnippetYAML(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") != "config view -o json" {
			return nil, fmt.Errorf("unexpected: %s", strings.Join(args, " "))
		}
		return []byte(`{
			"current-context":"c1",
			"contexts":[{"name":"c1","context":{"cluster":"prod","user":"u1","namespace":"kube-system"}}],
			"clusters":[{"name":"prod","cluster":{"server":"https://10.0.0.1:6443","certificate-authority-data":"Y2E="}}],
			"users":[{"name":"u1","user":{"token":"t0k3n"}}]
		}`), nil
	}
	defer func() { runKubectlFn = oldRun }()

	y, err := clusterSnippetYAML("c1")
	if err != nil {
		t.Fatalf("clusterSnippetYAML: %v", err)
	}
	for _, want := range []string{
		`current-context: "c1"`,
		`    cluster: "prod"`,
		`    user: "u1"`,
		`    namespace: "kube-system"`,
		`    server: "https://10.0.0.1:6443"`,
		`certificate-authority-data: "Y2E="`,
		`    token: "t0k3n"`,
	} {
		if !strings.Contains(y, want) {
			t.Errorf("snippet не содержит %q:\n%s", want, y)
		}
	}

	if _, err := clusterSnippetYAML("nope"); err == nil {
		t.Error("ожидалась ошибка для отсутствующего контекста")
	}
}

func TestObjectHandlerClusterScope(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") == "config view -o json" {
			return []byte(`{"contexts":[{"name":"c1","context":{"cluster":"prod","user":"u1"}}]}`), nil
		}
		return nil, fmt.Errorf("unexpected: %s", strings.Join(args, " "))
	}
	defer func() { runKubectlFn = oldRun }()

	req := httptest.NewRequest(http.MethodGet, "/api/object?ctx=c1&kind=cluster&name=c1", nil)
	rec := httptest.NewRecorder()
	handleObject(rec, req)
	if !strings.Contains(rec.Body.String(), `"yaml"`) || !strings.Contains(rec.Body.String(), `kind: Config`) {
		t.Errorf("body = %s", rec.Body.String())
	}
}

func TestObjectHandlerSkipsNSForClusterScoped(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if strings.Contains(j, "-n ") {
			return nil, fmt.Errorf("cluster-scoped: namespace flag forbidden: %s", j)
		}
		return []byte("apiVersion: v1\nkind: Node\nmetadata:\n  name: n1\n"), nil
	}
	defer func() { runKubectlFn = oldRun }()

	req := httptest.NewRequest(http.MethodGet, "/api/object?ctx=c1&kind=node&name=n1", nil)
	rec := httptest.NewRecorder()
	handleObject(rec, req)
	if rec.Body.String() == "" || strings.Contains(rec.Body.String(), `"error":"cluster-scoped`) {
		t.Errorf("body = %s", rec.Body.String())
	}
}

func TestOverviewScopes(t *testing.T) {
	oldCtx, oldRun := getContextsFn, runKubectlFn
	getContextsFn = func() ([]string, error) { return []string{"c1", "c2"}, nil }
	defer func() { getContextsFn, runKubectlFn = oldCtx, oldRun }()

	runPVC := `{"items":[
		{"metadata":{"name":"web","namespace":"prod","creationTimestamp":"2026-01-01T00:00:00Z"},"spec":{"resources":{"requests":{"storage":"10Gi"}}}},
		{"metadata":{"name":"logs","namespace":"ops","creationTimestamp":"2026-01-02T00:00:00Z"},"spec":{"resources":{"requests":{"storage":"5Gi"}}}}
	]}`
	runSVC := `{"items":[{"metadata":{"name":"api","namespace":"ops","creationTimestamp":"2026-01-01T00:00:00Z"}}]}`
	runNodes := `{"items":[
		{"metadata":{"name":"n1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
		{"metadata":{"name":"n2"},"status":{"conditions":[{"type":"Ready","status":"False"}]}}
	]}`
	runNS := `{"items":[{"metadata":{"name":"default"},"status":{"phase":"Active"}}]}`

	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		switch {
		case strings.Contains(j, "get persistentvolumeclaims"):
			return []byte(runPVC), nil
		case strings.Contains(j, "get services"):
			return []byte(runSVC), nil
		case strings.Contains(j, "get nodes"):
			return []byte(runNodes), nil
		case strings.Contains(j, "get namespaces"):
			return []byte(runNS), nil
		}
		return []byte("unexpected: " + j), fmt.Errorf("unexpected")
	}

	call := func(scope string) relatedResp {
		req := httptest.NewRequest(http.MethodGet, "/api/overview?scope="+scope, nil)
		rec := httptest.NewRecorder()
		handleOverview(rec, req)
		var r relatedResp
		if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
			t.Fatalf("scope %s: bad json: %v", scope, err)
		}
		return r
	}

	// pvc: оба контекста, у каждого по 2 PVC → 4 элемента с ёмкостью.
	r := call("pvc")
	if len(r.Items) != 4 {
		t.Fatalf("pvc items = %d, want 4", len(r.Items))
	}
	found := false
	for _, it := range r.Items {
		if it.Kind != "persistentvolumeclaim" || it.Ns == "" || it.Ctx == "" || it.Reason != "10.00 GiB" && it.Reason != "5.00 GiB" {
			t.Errorf("pvc item = %+v", it)
		}
		if it.Name == "web" && it.Ns == "prod" {
			found = true
		}
	}
	if !found {
		t.Errorf("pvc web/prod не найден: %+v", r.Items)
	}

	// svc: 2 контекста × 1 сервис.
	r = call("svc")
	if len(r.Items) != 2 || r.Items[0].Kind != "service" || r.Items[0].Ns == "" {
		t.Errorf("svc items = %+v", r.Items)
	}

	// nodes: ready/not ready.
	r = call("nodes")
	if len(r.Items) != 4 {
		t.Fatalf("nodes items = %d, want 4", len(r.Items))
	}
	got := map[string]string{}
	for _, it := range r.Items {
		if it.Ns != "" {
			t.Errorf("node ns должен быть пуст: %+v", it)
		}
		got[it.Name] = it.Reason
	}
	if got["n1"] != "ready" || got["n2"] != "not ready" {
		t.Errorf("node reasons = %v", got)
	}

	// ns: namespace + phase.
	r = call("ns")
	if len(r.Items) != 2 || r.Items[0].Kind != "namespace" || r.Items[0].Reason != "Active" {
		t.Errorf("ns items = %+v", r.Items)
	}

	// ctx: список контекстов.
	r = call("ctx")
	if len(r.Items) != 2 || r.Items[0].Kind != "cluster" || r.Items[0].Name != "c1" || r.Items[0].Ctx != "c1" {
		t.Errorf("ctx items = %+v", r.Items)
	}
}

func TestOverviewScopeRBACSkipsDeniedContext(t *testing.T) {
	oldCtx, oldRun := getContextsFn, runKubectlFn
	getContextsFn = func() ([]string, error) { return []string{"a", "b"}, nil }
	defer func() { getContextsFn, runKubectlFn = oldCtx, oldRun }()
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if strings.Contains(j, `--context a`) {
			return []byte("Forbidden"), fmt.Errorf("exit status 1")
		}
		if strings.Contains(j, "--context b") && strings.Contains(j, "get services") {
			return []byte(`{"items":[{"metadata":{"name":"api","namespace":"ops"}}]}`), nil
		}
		return nil, fmt.Errorf("unexpected: %s", j)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/overview?scope=svc", nil)
	rec := httptest.NewRecorder()
	handleOverview(rec, req)
	var r relatedResp
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if len(r.Items) != 1 || r.Items[0].Ctx != "b" {
		t.Errorf("должен остаться только доступный контекст: %+v", r.Items)
	}
	if r.Error != "" {
		t.Errorf("error не ожидался: %q", r.Error)
	}
}

func TestRelatedHandlerListsResources(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		switch {
		case strings.Contains(j, "custom-columns=NAME"):
			switch {
			case strings.Contains(j, "get deployments"):
				return []byte("NAME CREATED\ncoredns  2020-01-01T00:00:00Z\ningress-svc  2020-06-01T00:00:00Z\n"), nil
			case strings.Contains(j, "get services"):
				return []byte("NAME CREATED\nkube-dns  2021-01-01T00:00:00Z\nkubernetes  2020-01-01T00:00:00Z\n"), nil
			case strings.Contains(j, "get configmaps"):
				return []byte("NAME CREATED\nkube-root-ca.crt  2020-01-01T00:00:00Z\napp-cm  2021-01-01T00:00:00Z\ncore-cm  2021-01-01T00:00:00Z\ncm-from  2021-01-01T00:00:00Z\n"), nil
			case strings.Contains(j, "get serviceaccounts"):
				return []byte("NAME CREATED\ncoredns  2021-01-01T00:00:00Z\n"), nil
			case strings.Contains(j, "get secrets"):
				return []byte("NAME CREATED\ntls-secret  2021-01-01T00:00:00Z\n"), nil
			case strings.Contains(j, "get persistentvolumeclaims"):
				return []byte("NAME CREATED\ndata-pvc  2021-01-01T00:00:00Z\n"), nil
			}
			return []byte("NAME CREATED\n"), nil // у остальных типов просто нет ресурсов
		case strings.Contains(j, "get pod ") && strings.Contains(j, "-o json"):
			return []byte(`{
				"metadata": {
					"labels": {"k8s-app": "kube-dns"},
					"ownerReferences": [{"kind": "ReplicaSet", "name": "coredns-xyz"}]
				},
				"spec": {
					"serviceAccountName": "coredns",
					"volumes": [
						{"name": "conf", "configMap": {"name": "cm-from"}},
						{"name": "data", "persistentVolumeClaim": {"claimName": "data-pvc"}}
					],
					"containers": [{
						"env": [
							{"valueFrom": {"configMapKeyRef": {"name": "app-cm"}}},
							{"valueFrom": {"secretKeyRef": {"name": "tls-secret"}}}
						],
						"envFrom": [{"configMapRef": {"name": "core-cm"}}]
					}]
				}
			}`), nil
		case strings.Contains(j, "get services") && strings.Contains(j, "-o json"):
			return []byte(`{"items":[
				{"metadata": {"name": "kube-dns"}, "spec": {"selector": {"k8s-app": "kube-dns"}}},
				{"metadata": {"name": "kubernetes"}, "spec": {"selector": {}}}
			]}`), nil
		case strings.Contains(j, "get ingresses") && strings.Contains(j, "-o json"):
			return []byte(`{"items":[{"metadata": {"name": "web"},
				"spec": {"rules": [{"http": {"paths": [{"backend": {"service": {"name": "kube-dns"}}}]}}]}}]}`), nil
		case strings.Contains(j, "get replicasets") && strings.Contains(j, "-o json"):
			return []byte(`{"items":[{"metadata": {"name": "coredns-xyz",
				"ownerReferences": [{"kind": "Deployment", "name": "coredns"}]}}]}`), nil
		}
		return []byte(""), nil
	}
	defer func() { runKubectlFn = oldRun }()

	req := httptest.NewRequest(http.MethodGet,
		"/api/related?ctx=c1&ns=kube-system&pod=coredns-x&node=n1", nil)
	rec := httptest.NewRecorder()
	handleRelated(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp struct {
		Labels map[string]string
		Items  []ObjRef
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if resp.Labels["k8s-app"] != "kube-dns" {
		t.Errorf("labels = %v", resp.Labels)
	}
	cat := make(map[string]string)
	reason := make(map[string]string)
	for _, it := range resp.Items {
		cat[it.Kind+"/"+it.Name] = it.Cat
		reason[it.Kind+"/"+it.Name] = it.Reason
	}
	age := make(map[string]string)
	for _, it := range resp.Items {
		if it.Age != "" {
			age[it.Kind+"/"+it.Name] = it.Age
		}
	}
	// Возраст берётся из creationTimestamp, кроме namespace/node (scope).
	for _, withAge := range []string{
		"deployment/coredns", "service/kube-dns", "configmap/core-cm",
		"serviceaccount/coredns", "secret/tls-secret",
	} {
		if age[withAge] == "" {
			t.Errorf("ожидали age у %s, имеем %v", withAge, age)
		}
	}
	for _, noAge := range []string{"namespace/kube-system", "node/n1"} {
		if age[noAge] != "" {
			t.Errorf("namespace/node не должны иметь age, имеют %v", age[noAge])
		}
	}
	wantCat := map[string]string{
		"pod/coredns-x":                  "Workload",
		"deployment/coredns":             "Workload",
		"service/kube-dns":               "Network",
		"ingress/web":                    "Network",
		"configmap/app-cm":               "Config",
		"configmap/core-cm":              "Config",
		"configmap/cm-from":              "Config",
		"secret/tls-secret":              "Config",
		"persistentvolumeclaim/data-pvc": "Storage",
		"serviceaccount/coredns":         "Access",
		"namespace/kube-system":          "Cluster",
		"node/n1":                        "Cluster",
	}
	for k, c := range wantCat {
		if cat[k] != c {
			t.Errorf("cat %s = %q (want %q)", k, cat[k], c)
		}
	}
	wantReason := map[string]string{
		"pod/coredns-x":                  rOwner,
		"deployment/coredns":             rOwner,    // цепочка rs -> deployment
		"service/kube-dns":               rSelector, // селектор бьётся по лейблам пода
		"ingress/web":                    rSelector, // backend указывает на подходящий Service
		"serviceaccount/coredns":         rUsed,
		"secret/tls-secret":              rUsed,
		"persistentvolumeclaim/data-pvc": rUsed,
		"configmap/app-cm":               rUsed,
		"configmap/core-cm":              rUsed,
		"configmap/cm-from":              rUsed,
		"namespace/kube-system":          rScope,
		"node/n1":                        rScope,
	}
	for k, w := range wantReason {
		if reason[k] != w {
			t.Errorf("reason %s = %q (want %q)", k, reason[k], w)
		}
	}
	// Объекты, связанные только проживанием в namespace пода, должны отсутствовать.
	for _, absent := range []string{
		"deployment/ingress-svc",     // не владеет подом
		"service/kubernetes",         // нет селектора
		"configmap/kube-root-ca.crt", // под на неё не ссылается
		"replicaset/local-path-provisioner",
	} {
		if _, ok := reason[absent]; ok {
			t.Errorf("лишний объект %s (связан только по namespace)", absent)
		}
	}
	// Категории идут в фиксированном порядке: Workload раньше Cluster.
	podIx, nsIx := -1, -1
	for i, it := range resp.Items {
		if it.Kind == "pod" {
			podIx = i
		}
		if it.Kind == "namespace" {
			nsIx = i
		}
	}
	if podIx < 0 || nsIx < 0 || podIx > nsIx {
		t.Errorf("порядок категорий сломан: pod@%d namespace@%d, items=%+v", podIx, nsIx, resp.Items)
	}
}

func TestGatherDataGeneratedTime(t *testing.T) {
	defer withFakes(t, fakePods(), []string{"k8s-prod"})()
	before := time.Now().Add(-time.Minute)
	data := gatherData()
	after := time.Now().Add(time.Minute)
	if data.Generated.Before(before) || data.Generated.After(after) {
		t.Errorf("Generated вне диапазона: %v", data.Generated)
	}
}
