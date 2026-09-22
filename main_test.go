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
	pods, err := parsePodList("ctx-prod", []byte(samplePodJSON), nil)
	if err != nil {
		t.Fatalf("parsePodList: %v", err)
	}
	if len(pods) != 3 {
		t.Fatalf("expected 3 rows (web, init-x, db), got %d", len(pods))
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

func TestResString(t *testing.T) {
	if got := resString("100m", "", "128Mi", "512Mi"); got != "100m/- · 128Mi/512Mi" {
		t.Errorf("resString = %q", got)
	}
	if got := resString("", "", "", ""); got != "-/- · -/-" {
		t.Errorf("resString empty = %q", got)
	}
}

func TestResLive(t *testing.T) {
	// Использование относительно лимита.
	if got, want := resLive("100m", "500m", "128Mi", "512Mi", "25m", "64Mi"), "25m/500m (5%) · 64Mi/512Mi (13%)"; got != want {
		t.Errorf("resLive = %q, want %q", got, want)
	}
	// Лимита нет — процент от запрошенных ресурсов.
	if got, want := resLive("200m", "", "128Mi", "", "50m", "32Mi"), "50m/200m (25%) · 32Mi/128Mi (25%)"; got != want {
		t.Errorf("resLive no-limit = %q, want %q", got, want)
	}
	// Без потребления — падает на req/lim.
	if got, want := resLive("100m", "500m", "128Mi", "512Mi", "", ""), "100m/500m · 128Mi/512Mi"; got != want {
		t.Errorf("resLive no-usage = %q, want %q", got, want)
	}
	// Целые ядра и двоичные/десятичные суффиксы памяти.
	if got, want := resLive("1", "2", "1Gi", "4Gi", "500m", "1Gi"), "500m/2 (25%) · 1Gi/4Gi (25%)"; got != want {
		t.Errorf("resLive cores = %q, want %q", got, want)
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
	if _, err := parsePodList("ctx", []byte("{not json"), nil); err == nil {
		t.Fatal("expected error for invalid json")
	}
}

func TestParsePodListNoContainers(t *testing.T) {
	data := []byte(`{"items":[{"metadata":{"name":"hang-pod","namespace":"ns"},"spec":{},"status":{"phase":"Pending"}}]}`)
	pods, err := parsePodList("ctx", data, nil)
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
	if st.Issues != 1 {
		t.Errorf("Issues = %d, want 1 (only Pending, Succeeded excluded)", st.Issues)
	}
}

func TestBuildCards(t *testing.T) {
	cards := buildCards(Stats{Contexts: 2, Namespaces: 3, Nodes: 4, Containers: 5})
	if len(cards) != 9 {
		t.Fatalf("len(cards) = %d, want 9", len(cards))
	}
	// Node-карточка стоит сразу после Namespaces.
	if cards[1].Label != "Namespaces" || cards[2].Label != "Nodes" || cards[2].Value != 4 {
		t.Errorf("cards order/values mismatch: %+v", cards)
	}
	if cards[0].Value != 2 || cards[4].Value != 5 {
		t.Errorf("cards values mismatch: %+v", cards)
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
	oldPods, oldCtx, oldNodes := fetchPodsFn, getContextsFn, fetchNodesFn
	fetchPodsFn = func() ([]Pod, string) { return pods, "" }
	getContextsFn = func() ([]string, error) { return ctxs, nil }
	fetchNodesFn = func() int { return 0 }
	return func() {
		fetchPodsFn, getContextsFn, fetchNodesFn = oldPods, oldCtx, oldNodes
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
		"Contexts",
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
	oldPods, oldCtx, oldNodes := fetchPodsFn, getContextsFn, fetchNodesFn
	fetchPodsFn = func() ([]Pod, string) { return nil, "kubectl: access denied for cluster A" }
	getContextsFn = func() ([]string, error) { return []string{"c1"}, nil }
	fetchNodesFn = func() int { return 0 }
	defer func() { fetchPodsFn, getContextsFn, fetchNodesFn = oldPods, oldCtx, oldNodes }()

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
	if data.Stats.Contexts != 1 || data.Stats.Issues != 1 {
		t.Errorf("stats mismatch: %+v", data.Stats)
	}
	if len(data.Cards) != 9 {
		t.Errorf("len(Cards) = %d, want 9", len(data.Cards))
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

	pods, errMsg := fetchPodsReal()
	if errMsg != "" {
		t.Fatalf("errMsg = %q", errMsg)
	}
	if len(pods) != 3 {
		t.Fatalf("len(pods) = %d, want 3", len(pods))
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

	pods, errMsg := fetchPodsReal()
	if errMsg != "" {
		t.Fatalf("errMsg = %q", errMsg)
	}
	if len(pods) != 3 {
		t.Fatalf("len(pods) = %d, want 3", len(pods))
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

	pods, errMsg := fetchPodsReal()
	if len(pods) != 0 {
		t.Errorf("len(pods) = %d, want 0", len(pods))
	}
	if !strings.Contains(errMsg, "default") {
		t.Errorf("errMsg должен упоминать fallback namespace: %q", errMsg)
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

func TestSpecSearchFindsMatchInManifests(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if strings.Contains(j, "get configmaps") && strings.Contains(j, "-o yaml") {
			return []byte("apiVersion: v1\nitems:\n- apiVersion: v1\n" +
				"  kind: ConfigMap\n  metadata:\n    name: app-cm\n  data:\n" +
				"    KEY: \"superSecretValue\"\n"), nil
		}
		if strings.Contains(j, "-o yaml") {
			return []byte("apiVersion: v1\nitems: []\n"), nil
		}
		return []byte(""), nil
	}
	defer func() { runKubectlFn = oldRun }()

	resp := searchManifests("c1", "prod", "supersecret")
	if len(resp) != 1 {
		t.Fatalf("matches = %d, want 1: %+v", len(resp), resp)
	}
	hit := resp[0]
	if hit.Kind != "configmap" || hit.Name != "app-cm" {
		t.Errorf("hit = %+v, want configmap/app-cm", hit)
	}
	if !strings.Contains(hit.Snippet, "superSecretValue") {
		t.Errorf("snippet потерял значение: %+v", hit)
	}
}
