package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// Кеш агрегированных данных в тестах выключен: каждый тест собирает
	// данные заново через подменённые fetchAllFn/getContextsFn.
	dataTTL = 0
	overviewCache = newDataCache(0)
	os.Exit(m.Run())
}

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
	if initX.Container != "init-x" || initX.State != "Terminated:Completed" || initX.StateCss != "ok" {
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
	if db.Qos != "Guaranteed" || db.Res != "-/2 · -/2Gi" {
		t.Errorf("db qos/res mismatch: %+v", db)
	}
	if web.Labels != "app=web, tier=frontend" {
		t.Errorf("web labels = %q", web.Labels)
	}
	if web.Qos != "Burstable" {
		t.Errorf("web qos = %q", web.Qos)
	}
	if web.Res != "-/- · -/512Mi" {
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
	// Использование относительно лимита: сначала %, потом use/lim.
	if got, want := resPart("100m", "500m", "25m", cpuMilli), "5% (25m/500m)"; got != want {
		t.Errorf("resPart cpu = %q, want %q", got, want)
	}
	// Лимита нет — процент от запрошенных ресурсов.
	if got, want := resPart("200m", "", "50m", cpuMilli), "25% (50m/200m)"; got != want {
		t.Errorf("resPart no-limit = %q, want %q", got, want)
	}
	// Без потребления — падает на req/lim.
	if got, want := resPart("100m", "500m", "", cpuMilli), "-/500m"; got != want {
		t.Errorf("resPart no-usage = %q, want %q", got, want)
	}
	// Целые ядра и двоичные/десятичные суффиксы памяти.
	if got, want := resPart("1", "2", "500m", cpuMilli), "25% (500m/2)"; got != want {
		t.Errorf("resPart cores = %q, want %q", got, want)
	}
	if got, want := resPart("128Mi", "512Mi", "64Mi", memBytes), "13% (64Mi/512Mi)"; got != want {
		t.Errorf("resPart mem = %q, want %q", got, want)
	}
	if got, want := resPart("1Gi", "4Gi", "1Gi", memBytes), "25% (1Gi/4Gi)"; got != want {
		t.Errorf("resPart GiB = %q, want %q", got, want)
	}
}

func TestTopPodsUsage(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") != "--context c1 top pods -A --no-headers" {
			return []byte("boom"), fmt.Errorf("unexpected: %s", strings.Join(args, " "))
		}
		return []byte("prod    web-0  12m   40Mi\nprod    db-0   1     132Mi\n"), nil
	}
	defer func() { runKubectlFn = oldRun }()

	usage := topPodsUsage("c1")
	if got, want := usage["prod|web-0"], [2]string{"12m", "40Mi"}; got != want {
		t.Errorf("usage web-0 = %v, want %v", got, want)
	}
	if got, want := usage["prod|db-0"], [2]string{"1", "132Mi"}; got != want {
		t.Errorf("usage db-0 = %v, want %v", got, want)
	}
	if got, ok := usage["web-0|prod"]; ok {
		t.Errorf("перепутан порядок полей kubectl top: ns|pod = %q, есть лишний key web-0|prod", got)
	}
}

func TestWorkloadOwnerMapAndResolve(t *testing.T) {
	oldRun := runKubectlFn
	// Плоский List: ReplicaSet web-abc123 владеет Deployment web; Job migrate —
	// CronJob nightly; cronjob nightly без владельца.
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if !strings.Contains(j, "get deployments,statefulsets,daemonsets,replicasets,jobs,cronjobs") {
			return []byte("boom"), fmt.Errorf("unexpected: %s", j)
		}
		return []byte(`{"kind":"List","items":[
			{"kind":"Deployment","metadata":{"name":"web","namespace":"prod","ownerReferences":[]}},
			{"kind":"ReplicaSet","metadata":{"name":"web-abc123","namespace":"prod","ownerReferences":[{"kind":"Deployment","name":"web"}]}},
			{"kind":"Job","metadata":{"name":"migrate","namespace":"ops","ownerReferences":[{"kind":"CronJob","name":"nightly"}]}},
			{"kind":"CronJob","metadata":{"name":"nightly","namespace":"ops","ownerReferences":[]}}
		]}`), nil
	}
	defer func() { runKubectlFn = oldRun }()

	m := workloadOwnerMap("c1")
	if got, want := m["web-abc123|replicaset"], [2]string{"Deployment", "web"}; got != want {
		t.Errorf("rs->deploy map = %v, want %v", got, want)
	}
	if got, want := m["migrate|job"], [2]string{"CronJob", "nightly"}; got != want {
		t.Errorf("job->cronjob map = %v, want %v", got, want)
	}

	rsPod := Pod{OwnerKind: "ReplicaSet", Owner: "web-abc123"}
	if got := resolveWorkload(&rsPod, m); got != "Deployment" || rsPod.Owner != "web" {
		t.Errorf("resolve rs pod = %q, owner=%q, want Deployment/web", got, rsPod.Owner)
	}
	jobPod := Pod{OwnerKind: "Job", Owner: "migrate"}
	if got := resolveWorkload(&jobPod, m); got != "CronJob" {
		t.Errorf("resolve job pod = %q, want CronJob", got)
	}
	bare := Pod{OwnerKind: "", Owner: ""}
	if got := resolveWorkload(&bare, m); got != "" {
		t.Errorf("resolve bare pod = %q, want empty", got)
	}
	dsPod := Pod{OwnerKind: "DaemonSet", Owner: "node-exporter"}
	if got := resolveWorkload(&dsPod, m); got != "DaemonSet" {
		t.Errorf("resolve ds pod = %q, want DaemonSet", got)
	}
}

func TestWorkloadOwnerMapNestedAndErrors(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	// Новый kubectl оборачивает каждый тип в под-List; у элементов может не быть
	// поля kind — он берётся из имени под-List.
	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte(`{"kind":"List","items":[
			{"kind":"ReplicaSetList","items":[
				{"metadata":{"name":"web-abc123","namespace":"prod","ownerReferences":[{"kind":"Deployment","name":"web"}]}}
			]},
			{"kind":"DeploymentList","items":[
				{"metadata":{"name":"web","namespace":"prod","ownerReferences":[]}}
			]},
			{"kind":"JobList","items":[
				{"metadata":{"name":"migrate","namespace":"ops","ownerReferences":[{"kind":"CronJob","name":"nightly"}]}}
			]}
		]}`), nil
	}
	m := workloadOwnerMap("c1")
	if got, want := m["web-abc123|replicaset"], [2]string{"Deployment", "web"}; got != want {
		t.Errorf("nested rs->deploy = %v, want %v", got, want)
	}
	if got, want := m["migrate|job"], [2]string{"CronJob", "nightly"}; got != want {
		t.Errorf("nested job->cronjob = %v, want %v", got, want)
	}

	// Ошибка kubectl — nil.
	runKubectlFn = func(args ...string) ([]byte, error) { return nil, fmt.Errorf("rbac") }
	if got := workloadOwnerMap("c1"); got != nil {
		t.Errorf("error → %v, want nil", got)
	}

	// Битый JSON — nil.
	runKubectlFn = func(args ...string) ([]byte, error) { return []byte("{not json"), nil }
	if got := workloadOwnerMap("c1"); got != nil {
		t.Errorf("bad json → %v, want nil", got)
	}
}

func TestEgressKinds(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if !strings.Contains(j, "api-resources") {
			return []byte("boom"), fmt.Errorf("unexpected: %s", j)
		}
		return []byte("services\ningresses\negressfirewalls.network.openshift.io\negressnetworkpolicies.network.openshift.io\n"), nil
	}
	defer func() { runKubectlFn = oldRun }()

	kinds := egressKinds("c1")
	if len(kinds) != 2 {
		t.Fatalf("egressKinds len = %d, want 2: %+v", len(kinds), kinds)
	}
	if kinds[0].plural != "egressfirewalls.network.openshift.io" || kinds[0].cat != "Network" {
		t.Errorf("kinds[0] = %+v", kinds[0])
	}

	// Нет egress-CRD — пустой список.
	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte("services\ningresses\n"), nil
	}
	if kinds := egressKinds("c1"); kinds != nil {
		t.Errorf("без egress-CRD kinds = %+v, want nil", kinds)
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
	if st.PodsRunning != 3 {
		t.Errorf("PodsRunning = %d, want 3 (только Running)", st.PodsRunning)
	}
	// Завершённый Job (Succeeded) в «активные» контейнеры не входит.
	if st.Containers != 5 {
		t.Errorf("Containers = %d, want 5 (без Succeeded)", st.Containers)
	}
	if st.Running != 4 {
		t.Errorf("Running = %d, want 4 (by phase, includes d/z)", st.Running)
	}
}

func TestBuildCards(t *testing.T) {
	cards := buildCards(Stats{
		AvailCtx: 2, Contexts: 3,
		Namespaces: 3,
		NodeReady:  4, NodeTotal: 5,
		Pods: 10, PodsRunning: 8,
		Containers: 20, Running: 15,
		Networks:   7,
		Services:   7,
		Jobs:       4,
		JobsDone:   3,
		Configs:    6,
		ConfigMaps: 4,
		Secrets:    2,
		Charts:     5,
		CpuMilli:   1200, CpuTotalMilli: 4000,
		MemBytes: 2 << 30, MemTotalBytes: 4 << 30,
		CpuLimitMilli: 1200, MemLimitBytes: 2 << 30,
		PVCBound: 2, PVCTotal: 5, PVCBoundBytes: 1 << 30, PVCTotalBytes: 2 << 30, PVTotalBytes: 4 << 30, PVCOk: 1,
	})
	if len(cards) != 15 {
		t.Fatalf("len(cards) = %d, want 15", len(cards))
	}
	row1 := []string{"Clusters", "Namespaces", "Nodes", "Pods", "Containers", "Jobs", "Charts", "Services", "Configs", "Events"}
	row2 := []string{"CPU", "Memory", "Limits", "PVC/PV size", "PVC/PV Count"}
	for i, l := range append(row1, row2...) {
		if cards[i].Label != l {
			t.Errorf("cards[%d].Label = %q, want %q", i, cards[i].Label, l)
		}
	}
	scopes := []string{"ctx", "ns", "nodes", "pods", "workloads", "jobs", "charts", "svc", "configs", "events", "autoscale", "quota", "limits", "pvc", "pvc"}
	for i, s := range scopes {
		if cards[i].Scope != s {
			t.Errorf("cards[%d].Scope = %q, want %q (%s)", i, cards[i].Scope, s, cards[i].Label)
		}
	}
	if cards[0].Value != "2/3" || cards[2].Value != "4/5" {
		t.Errorf("clusters/nodes = %+v", cards)
	}
	if cards[3].Value != "8/10" || cards[4].Value != "15/20" || cards[5].Value != "3/4" {
		t.Errorf("pods/containers/jobs = %+v", cards[:6])
	}
	if cards[6].Label != "Charts" || cards[6].Value != "5" {
		t.Errorf("charts card: %+v", cards[6])
	}
	if cards[7].Value != "7" || cards[8].Value != "4/2" {
		t.Errorf("networks/configs cards = %+v", cards[7:9])
	}
	if cards[9].Label != "Events" || cards[9].Value != "0" {
		t.Errorf("events card: %+v", cards[9])
	}
	if cards[10].Value != "1.20/4.00" || cards[10].Scope != "autoscale" {
		t.Errorf("cpu card: %+v", cards[10])
	}
	if cards[11].Value != "2.00/4.00 GiB" || cards[11].Scope != "quota" {
		t.Errorf("memory card: %+v", cards[11])
	}
	if cards[12].Label != "Limits" || cards[12].Value != "1.20/2.00 GiB" || cards[12].Scope != "limits" {
		t.Errorf("limits card: %+v", cards[12])
	}
	if cards[13].Value != "2.00/4.00 GiB" || cards[13].Label != "PVC/PV size" {
		t.Errorf("pvc size card: %+v", cards[13])
	}
	if cards[14].Value != "2/5" || cards[14].Label != "PVC/PV Count" || cards[14].Scope != "pvc" {
		t.Errorf("pv/pvc card: %+v", cards[14])
	}
}

func TestScopeKindsPVC(t *testing.T) {
	got := scopeKinds("pvc")
	if len(got) != 3 {
		t.Fatalf("scopeKinds(pvc) len = %d, want 3", len(got))
	}
	want := map[string]bool{"persistentvolume": true, "persistentvolumeclaim": true, "storageclass": true}
	for _, k := range got {
		if !want[k.kind] {
			t.Errorf("scopeKinds(pvc) unexpected kind %q", k.kind)
		}
		if k.cat != "Storage" {
			t.Errorf("scopeKinds(pvc)[%s] cat = %q, want Storage", k.kind, k.cat)
		}
	}
}

func TestScopeKindsQuotaAutoscale(t *testing.T) {
	q := scopeKinds("quota")
	if len(q) != 2 {
		t.Fatalf("scopeKinds(quota) len = %d, want 2: %+v", len(q), q)
	}
	wantQ := map[string]string{"resourcequota": "resourcequotas", "limitrange": "limitranges"}
	for _, k := range q {
		if wantQ[k.kind] != k.plural {
			t.Errorf("quota kind %q plural = %q, want %q", k.kind, k.plural, wantQ[k.kind])
		}
	}

	a := scopeKinds("autoscale")
	if len(a) != 1 || a[0].kind != "horizontalpodautoscaler" || a[0].plural != "horizontalpodautoscalers" {
		t.Fatalf("scopeKinds(autoscale) = %+v, want only HPA", a)
	}

	// Неизвестный scope — nil.
	if got := scopeKinds("nope"); got != nil {
		t.Errorf("scopeKinds(nope) = %+v, want nil", got)
	}
}

func TestPVPvcValue(t *testing.T) {
	if got := pvPvcValue(Stats{PVCOk: 1, PVCBound: 2, PVCTotal: 5}); got != "2/5" {
		t.Errorf("pvPvcValue ok = %q, want 2/5", got)
	}
	if got := pvPvcValue(Stats{PVCBound: 3}); got != "3" {
		t.Errorf("pvPvcValue fallback = %q, want 3", got)
	}
	if got := pvPvcValue(Stats{}); got != "0" {
		t.Errorf("pvPvcValue empty = %q, want 0", got)
	}
	if got := pvPvcValue(Stats{PVCOk: 1, PVCBound: 2, PVCTotal: 5, StorageClasses: 3}); got != "2/5" {
		t.Errorf("pvPvcValue + sc = %q, want 2/5 (SC count no longer shown)", got)
	}
}

func TestCountKubectlKinds(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") != "--context c1 get jobs,cronjobs -A -o json" {
			return nil, fmt.Errorf("unexpected: %s", strings.Join(args, " "))
		}
		return []byte(`{"items":[
			{"kind":"Job","metadata":{"name":"j1"},"status":{"succeeded":1,"failed":0}},
			{"kind":"Job","metadata":{"name":"j2"},"status":{"succeeded":0,"failed":0}},
			{"kind":"CronJob","metadata":{"name":"cj1"}}
		]}`), nil
	}
	if n, d := countKubectlKinds("c1", scopeKinds("jobs")); n != 3 || d != 1 {
		t.Errorf("flat count = %d (done %d), want 3/1", n, d)
	}

	// Вложенный формат (новые kubectl): под-List'ы с собственным kind.
	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte(`{"items":[
			{"kind":"ConfigMapList","items":[{"metadata":{"name":"c1"}},{"metadata":{"name":"c2"}}]},
			{"kind":"SecretList","items":[{"metadata":{"name":"s1"}}]}
		]}`), nil
	}
	if n, _ := countKubectlKinds("c1", scopeKinds("configs")); n != 3 {
		t.Errorf("nested count = %d, want 3", n)
	}

	// RBAC-отказ — ноль.
	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte("Forbidden"), fmt.Errorf("exit status 1")
	}
	if n, d := countKubectlKinds("c1", scopeKinds("configs")); n != 0 || d != 0 {
		t.Errorf("count on RBAC error = %d/%d, want 0/0", n, d)
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

func TestNetworksAndServicesBatched(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	var calls []string
	runKubectlFn = func(args ...string) ([]byte, error) {
		joined := strings.Join(args, " ")
		calls = append(calls, joined)
		if strings.Contains(joined, "api-resources") {
			return []byte(""), nil // egress CRD отсутствует
		}
		if strings.Contains(joined, " get services,ingresses,networkpolicies,endpoints,endpointslices") {
			return []byte(`{"items":[
				{"kind":"ServiceList","items":[
					{"metadata":{"name":"svc1","namespace":"ns1"}},
					{"metadata":{"name":"svc2","namespace":"ns2"}}
				]},
				{"kind":"IngressList","items":[{"metadata":{"name":"ing1","namespace":"ns1"}}]},
				{"kind":"NetworkPolicyList","items":[{"metadata":{"name":"np1","namespace":"ns2"}}]}
			]}`), nil
		}
		return nil, fmt.Errorf("unexpected call: %s", joined)
	}

	netByNS, svcByNS := networksAndServicesByContextNS("c1")
	if svcByNS["ns1"] != 1 || svcByNS["ns2"] != 1 {
		t.Errorf("svcByNS = %v, want ns1:1, ns2:1", svcByNS)
	}
	if netByNS["ns1"] != 1 || netByNS["ns2"] != 1 {
		t.Errorf("netByNS = %v, want ns1:1, ns2:1 (без service)", netByNS)
	}
	if len(svcByNS) != 2 || len(netByNS) != 2 {
		t.Errorf("размеры карт: svc=%d net=%d, want 2/2", len(svcByNS), len(netByNS))
	}
	// ровно один get на все сетевые типы + один api-resources
	getCalls := 0
	for _, c := range calls {
		if strings.Contains(c, " get services,ingresses,networkpolicies,endpoints,endpointslices") {
			getCalls++
		}
	}
	if getCalls != 1 {
		t.Errorf("ожидали один объединённый get, получили %d: %v", getCalls, calls)
	}
}

func TestJobsConfigsBatched(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	var calls []string
	runKubectlFn = func(args ...string) ([]byte, error) {
		joined := strings.Join(args, " ")
		calls = append(calls, joined)
		return []byte(`{"items":[
			{"kind":"JobList","items":[
				{"metadata":{"name":"j1","namespace":"ns1"},"status":{"succeeded":1,"failed":0}},
				{"metadata":{"name":"j2","namespace":"ns1"},"status":{"succeeded":0,"failed":0}}
			]},
			{"kind":"CronJobList","items":[{"metadata":{"name":"cj1","namespace":"ns2"}}]},
			{"kind":"ConfigMapList","items":[{"metadata":{"name":"cm1","namespace":"ns1"}}]},
			{"kind":"SecretList","items":[{"metadata":{"name":"s1","namespace":"ns2"}},{"metadata":{"name":"s2","namespace":"ns2"}}]}
		]}`), nil
	}

	byKind := countKindsByKindNS("c1", append(scopeKinds("jobs"), scopeKinds("configs")...))
	jobNS := mergeKindsNS(filterKinds(byKind, "job", "cronjob"))
	cfgNS := mergeKindsNS(filterKinds(byKind, "configmap", "secret"))

	if jobNS["ns1"].total != 2 || jobNS["ns1"].done != 1 {
		t.Errorf("jobNS ns1 = %+v, want total 2 done 1", jobNS["ns1"])
	}
	if jobNS["ns2"].total != 1 || jobNS["ns2"].done != 0 {
		t.Errorf("jobNS ns2 = %+v, want total 1 done 0", jobNS["ns2"])
	}
	if cfgNS["ns1"].total != 1 || cfgNS["ns2"].total != 2 {
		t.Errorf("cfgNS = %+v, want ns1:1 ns2:2", cfgNS)
	}
	// один get на jobs,cronjobs,configmaps,secrets
	if len(calls) != 1 || !strings.Contains(calls[0], " get jobs,cronjobs,configmaps,secrets -A") {
		t.Errorf("ожидали один батч-вызов, получили: %v", calls)
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
	return func(sel Selection, _ string, _ string, _ bool, _ int) (io.ReadCloser, error) {
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
	}, "1h", true, 200, src)

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
	}, "", false, 0, src)

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
	src := func(sel Selection, since string, sinceTime string, follow bool, lines int) (io.ReadCloser, error) {
		rc = &blockRC{readC: make(chan struct{})}
		return rc, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // сразу отменённый контекст

	events := runLogStream(ctx, []Selection{
		{Context: "c1", Namespace: "ns", Pod: "podA", Container: "ctA"},
	}, "1h", true, 200, src)

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

func TestKubectlLogArgs(t *testing.T) {
	sel := Selection{Context: "c1", Namespace: "ns", Pod: "p1", Container: "ct1"}
	args := kubectlLogArgs(sel, "1h", "", true, 200)
	want := []string{"--context", "c1", "logs", "-n", "ns", "p1", "-c", "ct1", "--tail", "200", "-f", "--timestamps=true"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Errorf("args = %v, want %v", args, want)
	}
	has := func(token string) bool {
		for _, a := range args {
			if a == token {
				return true
			}
		}
		return false
	}
	if has("--since") || has("--since-time") {
		t.Errorf("--tail вместе с --since/--since-time не допустим: %v", args)
	}

	// lines<=0 → используется --since.
	args = kubectlLogArgs(sel, "6h", "", false, 0)
	if !has("--since") || has("-f") || has("--tail") {
		t.Errorf("args = %v", args)
	}

	// sinceTime приоритетнее since (переподключение стрима).
	args = kubectlLogArgs(sel, "6h", "2024-01-02T03:04:05.000000000Z", true, 0)
	if !has("--since-time") || has("--since") {
		t.Errorf("args = %v", args)
	}

	// Без since и tail, follow=false → минимум флагов.
	args = kubectlLogArgs(Selection{Context: "c2", Namespace: "ns", Pod: "p2"}, "", "", false, 0)
	for _, not := range []string{"-c", "--tail", "--since", "--since-time", "-f"} {
		if has(not) {
			t.Errorf("аргумент %q не должен присутствовать: %v", not, args)
		}
	}
}

func TestRunLogStreamLongLine(t *testing.T) {
	long := strings.Repeat("X", 4*1024*1024) // 4MB в одной строке без перевода строки
	src := fakeSource(map[string]string{"podA": long + "\nshort\n"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := runLogStream(ctx, []Selection{
		{Context: "c1", Namespace: "ns", Pod: "podA", Container: "ctA"},
	}, "", false, 0, src)

	var msgs []string
	for ev := range events {
		if ev.Err != nil {
			t.Fatalf("неожиданная ошибка: %v", ev.Err)
		}
		msgs = append(msgs, ev.Line.Message)
	}
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %d, want 2 (длинная строка + короткая)", len(msgs))
	}
	if msgs[0] != long {
		t.Errorf("длинная строка испорчена (len=%d, want %d)", len(msgs[0]), len(long))
	}
	if msgs[1] != "short" {
		t.Errorf("вторая строка = %q", msgs[1])
	}
}

func TestRestartFollowStreamReopensOnEOF(t *testing.T) {
	// Эмулируем клиентский/прокси-обрыв kubectl-стрима: каждый reader
	// заканчивается EOF, restartFollowStream должен переоткрыть источник
	// и продолжить с последней метки времени (--since-time), а не обнулять
	// вывод и не рвать SSE.
	ts := "2024-01-02T03:04:05.000000000Z"
	var calls []struct {
		sinceTime string
		lines     int
	}
	bodies := []string{
		ts + " a\n" + ts + " b\n",
		ts + " b\n" + ts + " c\n",
		ts + " c\n",
	}
	old := openLogStreamFn
	// Отдельная переменная-счётчик, чтобы исходный глобальный стример не тронуть.
	src := sourceFunc(func(sel Selection, _ string, sinceTime string, _ bool, lines int) (io.ReadCloser, error) {
		calls = append(calls, struct {
			sinceTime string
			lines     int
		}{sinceTime, lines})
		i := len(calls) - 1
		if i >= len(bodies) {
			return io.NopCloser(strings.NewReader("")), nil
		}
		return io.NopCloser(strings.NewReader(bodies[i])), nil
	})
	defer func() { openLogStreamFn = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := restartFollowStream(ctx, []Selection{
		{Context: "c1", Namespace: "ns", Pod: "podA", Container: "ctA"},
	}, "", true, 200, src)

	var msgs []string
	for ev := range events {
		if ev.Err != nil {
			t.Errorf("неожиданная ошибка: %v", ev.Err)
			break
		}
		msgs = append(msgs, ev.Line.Message)
		if len(msgs) >= 5 {
			cancel()
		}
	}
	// a, b, затем переподключение дублирует границу b, дальше c и т.д.
	want := []string{"a", "b", "b", "c", "c"}
	if len(msgs) < len(want) {
		t.Fatalf("messages = %v, want как минимум %v (переподключения не было?)", msgs, want)
	}
	for i := 0; i < len(want); i++ {
		if msgs[i] != want[i] {
			t.Errorf("messages[%d] = %q, want %q (весь поток: %v)", i, msgs[i], want[i], msgs)
		}
	}
	if len(calls) < 2 {
		t.Fatalf("source вызван %d раз, ждали переоткрытие (>=2)", len(calls))
	}
	if calls[0].sinceTime != "" || calls[0].lines != 200 {
		t.Errorf("первый вызов source = %+v, want sinceTime=\"\" lines=200", calls[0])
	}
	for _, c := range calls[1:] {
		if c.sinceTime != ts || c.lines != 0 {
			t.Errorf("повторный вызов source = %+v, want sinceTime=%q lines=0 (возобновление без дыры)", c, ts)
		}
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

func TestLogRequests(t *testing.T) {
	oldOut := log.Writer()
	oldFlags := log.Flags()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() { log.SetOutput(oldOut); log.SetFlags(oldFlags) }()

	var reached bool
	h := logRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/pods?ctx=prod&ns=ops", nil)
	req.RemoteAddr = "10.0.0.5:4321"
	req.Header.Set("User-Agent", "cb-test")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !reached {
		t.Fatal("next handler not called")
	}
	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want 418", rec.Code)
	}
	got := buf.String()
	if !strings.Contains(got, "[GET] 10.0.0.5:4321 (cb-test) -> /api/pods?ctx=prod&ns=ops") {
		t.Errorf("log = %q", got)
	}

	// Длинный URI обрезается до 200 символов + "...".
	buf.Reset()
	long := "/api/object?name=" + strings.Repeat("x", 400)
	req = httptest.NewRequest(http.MethodGet, long, nil)
	req.RemoteAddr = "10.0.0.6:1"
	req.Header.Set("User-Agent", "ua")
	h.ServeHTTP(httptest.NewRecorder(), req)
	got = buf.String()
	if !strings.Contains(got, "...") {
		t.Errorf("long URI not truncated: %q", got)
	}
	// Путь до "..." не должен превышать 200 символов.
	if i := strings.Index(got, " -> "); i >= 0 {
		uri := strings.TrimSuffix(strings.TrimSpace(got[i+4:]), "\n")
		if len(uri) != 200+3 {
			t.Errorf("truncated uri len = %d, want 203: %q", len(uri), uri)
		}
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
		"CrossBoard",
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
	if len(data.Cards) != 15 {
		t.Errorf("len(Cards) = %d, want 15", len(data.Cards))
	}
}

func TestDataCacheFreshBypass(t *testing.T) {
	var calls int
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	c := &dataCache{now: func() time.Time { return now }, ttl: time.Hour}
	fetch := func() *PageData {
		calls++
		return &PageData{Generated: now}
	}
	render := func(d *PageData) ([]byte, []byte) {
		return nil, []byte(fmt.Sprintf("gen=%d", d.Generated.Unix()))
	}

	var j1 []byte
	if _, _, j1 = c.view(fetch, render); calls != 1 {
		t.Fatalf("первый вызов: calls=%d, want 1", calls)
	}
	if string(j1) != fmt.Sprintf("gen=%d", now.Unix()) {
		t.Fatalf("json1 = %q", j1)
	}
	// второй вызов в пределах ttl — из кеша, fetch не запускается.
	_, _, j2 := c.view(fetch, render)
	if calls != 1 {
		t.Fatalf("кеш-хит: calls=%d, want 1", calls)
	}
	if !bytes.Equal(j1, j2) {
		t.Errorf("кеш вернул другой json")
	}
	// fresh=true — принудительный сбор даже при age < ttl.
	now = now.Add(3 * time.Second)
	_, _, j3 := c.viewFresh(fetch, render)
	if calls != 2 {
		t.Fatalf("fresh: calls=%d, want 2", calls)
	}
	if string(j3) != fmt.Sprintf("gen=%d", now.Unix()) {
		t.Errorf("json3 = %q", j3)
	}
}

func TestAPIForcesFresh(t *testing.T) {
	var calls int
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	oldCache := overviewCache
	overviewCache = &dataCache{now: func() time.Time { return now }, ttl: time.Hour}
	oldF, oldCtx := fetchAllFn, getContextsFn
	fetchAllFn = func() Overview {
		calls++
		return Overview{TotalCtx: 1, AvailCtx: 1, Pods: fakePods()}
	}
	getContextsFn = func() ([]string, error) { return []string{"k8s-prod"}, nil }
	defer func() {
		overviewCache = oldCache
		fetchAllFn, getContextsFn = oldF, oldCtx
	}()

	rec := httptest.NewRecorder()
	handleAPI(rec, httptest.NewRequest(http.MethodGet, "/api/pods", nil))
	if calls != 1 {
		t.Fatalf("после первого запроса calls=%d, want 1", calls)
	}
	var first PageData
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatalf("invalid first json: %v", err)
	}

	// Повторный запрос без параметра — из кеша, сбор не запускается.
	rec2 := httptest.NewRecorder()
	handleAPI(rec2, httptest.NewRequest(http.MethodGet, "/api/pods", nil))
	if calls != 1 {
		t.Fatalf("кеш-хит: calls=%d, want 1", calls)
	}

	// ?refresh=1 — обход кеша: сбор повторяется, generated обновился.
	// Небольшая пауза, чтобы времени: Windows-таймер выдаёт одинаковый
	// time.Now() для двух очень быстрых последовательных вызовов.
	time.Sleep(20 * time.Millisecond)
	rec3 := httptest.NewRecorder()
	handleAPI(rec3, httptest.NewRequest(http.MethodGet, "/api/pods?refresh=1", nil))
	if calls != 2 {
		t.Fatalf("Refresh: calls=%d, want 2", calls)
	}
	if rec3.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec3.Code)
	}
	var d PageData
	if err := json.Unmarshal(rec3.Body.Bytes(), &d); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if !d.Generated.After(first.Generated) {
		t.Errorf("generated не обновился: first=%v refresh=%v", first.Generated, d.Generated)
	}
}

func TestLogsHandlerStreamsSSE(t *testing.T) {
	var gotSel Selection
	type srcCall struct {
		sel    Selection
		since  string
		follow bool
		lines  int
	}
	var got srcCall
	old := openLogStreamFn
	openLogStreamFn = func(sel Selection, since string, sinceTime string, follow bool, lines int) (io.ReadCloser, error) {
		got = srcCall{sel: sel, since: since, follow: follow, lines: lines}
		gotSel = sel
		return io.NopCloser(strings.NewReader(
			"2024-01-02T03:04:05.000000000Z line-alpha\nline-beta\n")), nil
	}
	defer func() { openLogStreamFn = old }()

	srv := httptest.NewServer(http.HandlerFunc(handleLogs))
	defer srv.Close()

	sel := "k8s-prod|prod|web-0|web"
	// follow=0 → одиночный снимок истории (конечный, читается до done).
	resp, err := http.Get(srv.URL + "/logs?sel=" + url.QueryEscape(sel) + "&since=1h&follow=0&lines=200")
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
	if got.sel.Pod != "web-0" || got.since != "1h" || got.follow {
		t.Errorf("sourceFun словила не те параметры: %+v", got)
	}
	if got.lines != 200 {
		t.Errorf("lines = %d, want 200", got.lines)
	}
	if gotSel.Container != "web" {
		t.Errorf("selection container = %q", gotSel.Container)
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
	openLogStreamFn = func(sel Selection, since string, sinceTime string, follow bool, lines int) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("hello\n")), nil
	}
	defer func() { openLogStreamFn = old }()

	srv := httptest.NewServer(http.HandlerFunc(handleLogs))
	defer srv.Close()
	// Используем URL-кодирование для обоих sel, в т.ч. невалидного.
	// follow=0 → конечный снимок, иначе хендлер не завершится.
	raw := url.QueryEscape("good|ns1|pod1|c1") + "&sel=" + url.QueryEscape("мусор") + "&follow=0"
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
	ready, total, cpu, mem, ok := nodesContext("c1")
	if ready != 1 || total != 3 {
		t.Errorf("ready/total = %d/%d, want 1/3", ready, total)
	}
	if !ok {
		t.Errorf("nodesContext ok = false, want true")
	}
	if cpu != 3500 { // 2 + 500m + 1
		t.Errorf("cpu = %d, want 3500", cpu)
	}
	if mem != 8127596<<10+1<<30+512<<20 {
		t.Errorf("mem = %d, want sum of allocatable", mem)
	}
}

func TestSCByContextParsesJSON(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") != "--context c1 get storageclass -o json" {
			return nil, fmt.Errorf("unexpected: %s", strings.Join(args, " "))
		}
		return []byte(`{"items":[
			{"metadata":{"name":"standard"},"provisioner":"kubernetes.io/aws-ebs"},
			{"metadata":{"name":"fast"},"provisioner":"kubernetes.io/aws-ebs"},
			{"metadata":{"name":"ceph"},"provisioner":"rook-ceph.rbd.csi.ceph.com"}
		]}`), nil
	}
	defer func() { runKubectlFn = oldRun }()

	if got := scByContext("c1"); got != 3 {
		t.Errorf("scByContext = %d, want 3", got)
	}
}

func TestSCByContextErrorReturnsMinusOne(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		return nil, fmt.Errorf("boom")
	}
	defer func() { runKubectlFn = oldRun }()

	if got := scByContext("c1"); got != -1 {
		t.Errorf("scByContext on error = %d, want -1", got)
	}
}

func TestPVCByContextParsesJSON(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Join(args, " ") != "--context c1 get pvc -A -o json" {
			return nil, fmt.Errorf("unexpected: %s", strings.Join(args, " "))
		}
		return []byte(`{"items":[
			{"metadata":{"name":"web-data","namespace":"prod"},"spec":{"resources":{"requests":{"storage":"10Gi"}}},"status":{"phase":"Bound","capacity":{"storage":"10Gi"}}},
			{"metadata":{"name":"db-data","namespace":"prod"},"spec":{"resources":{"requests":{"storage":"200Gi"}}},"status":{"phase":"Bound","capacity":{"storage":"190Gi"}}},
			{"metadata":{"name":"logs","namespace":"ops"},"spec":{"resources":{"requests":{"storage":"5Gi"}}},"status":{"phase":"Pending"}}
		]}`), nil
	}
	defer func() { runKubectlFn = oldRun }()

	bound, total, boundBytes, totalBytes, ok := pvcByContext("c1", map[string]bool{"prod|web-data": true})
	if !ok {
		t.Error("ok = false, want true")
	}
	if bound != 2 || total != 3 {
		t.Errorf("bound/total = %d/%d, want 2/3", bound, total)
	}
	// boundBytes: реальная ёмкость Bound PVC (10GiB + 190GiB, а не запрос 200GiB);
	// totalBytes: сумма всех запросов 10+200+5 = 215GiB.
	if boundBytes != 200<<30 || totalBytes != 215<<30 {
		t.Errorf("boundBytes = %d, totalBytes = %d; want 200GiB, 215GiB", boundBytes, totalBytes)
	}
}

func TestPVCByContextRBACDeniedFallsBackToMounted(t *testing.T) {
	oldRun := runKubectlFn
	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte("Error from server (Forbidden): persistentvolumeclaims is forbidden"), fmt.Errorf("exit status 1")
	}
	defer func() { runKubectlFn = oldRun }()

	bound, total, boundBytes, totalBytes, ok := pvcByContext("c1", map[string]bool{"prod|web-data": true, "ops|logs": true})
	if ok {
		t.Error("ok = true, want false when listing denied")
	}
	if bound != 2 {
		t.Errorf("bound = %d, want 2 (fallback to mounted)", bound)
	}
	if total != 0 || boundBytes != 0 || totalBytes != 0 {
		t.Errorf("expected zeros, got total=%d boundBytes=%d bytes=%d", total, boundBytes, totalBytes)
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

func TestObjectHandlerChartAndError(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	// chart: helm-релиз декодируется из секрета.
	rel := map[string]interface{}{"name": "myapp", "info": map[string]interface{}{"status": "deployed"}}
	payload, _ := json.Marshal(rel)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(payload)
	gz.Close()
	releaseB64 := base64.StdEncoding.EncodeToString(buf.Bytes())
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		switch {
		case strings.Contains(j, "get secrets"):
			return []byte(`{"items":[{"metadata":{"name":"sh.helm.release.v1.myapp.v2"}}]}`), nil
		case strings.Contains(j, "get secret sh.helm.release.v1.myapp.v2"):
			return []byte(fmt.Sprintf(`{"type":"helm.sh/release.v1","data":{"release":%q}}`, releaseB64)), nil
		}
		return nil, fmt.Errorf("unexpected: %s", j)
	}
	rec := httptest.NewRecorder()
	handleObject(rec, httptest.NewRequest(http.MethodGet, "/api/object?ctx=c1&ns=ns&name=myapp&kind=chart", nil))
	var resp struct{ Yaml, Error string }
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Error != "" || !strings.Contains(resp.Yaml, "myapp") {
		t.Errorf("chart resp = %+v", resp)
	}

	// Ошибка kubectl с пустым выводом — в Error попадает текст ошибки.
	runKubectlFn = func(args ...string) ([]byte, error) { return nil, fmt.Errorf("boom") }
	rec = httptest.NewRecorder()
	handleObject(rec, httptest.NewRequest(http.MethodGet, "/api/object?ctx=c1&name=x&kind=pod", nil))
	resp = struct{ Yaml, Error string }{}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Error != "boom" {
		t.Errorf("empty-output error = %q, want boom", resp.Error)
	}

	// Ошибка kubectl с выводом — в Error попадает shortOutput.
	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte("Error from server (NotFound)"), fmt.Errorf("exit 1")
	}
	rec = httptest.NewRecorder()
	handleObject(rec, httptest.NewRequest(http.MethodGet, "/api/object?ctx=c1&name=x&kind=pod", nil))
	resp = struct{ Yaml, Error string }{}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Error != "Error from server (NotFound)" {
		t.Errorf("output error = %q", resp.Error)
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

func TestAutoscaleScopeKinds(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	call := func(apiResources string) []string {
		runKubectlFn = func(args ...string) ([]byte, error) {
			j := strings.Join(args, " ")
			if strings.Contains(j, "api-resources") {
				return []byte(apiResources), nil
			}
			return nil, fmt.Errorf("unexpected: %s", j)
		}
		var out []string
		for _, k := range autoscaleScopeKinds("c1") {
			out = append(out, k.kind)
		}
		return out
	}

	// VPA CRD отсутствует — только HPA.
	got := call("horizontalpodautoscalers.autoscaling\npoddisruptionbudgets.policy\n")
	if len(got) != 1 || got[0] != "horizontalpodautoscaler" {
		t.Fatalf("без VPA kinds = %v, want [horizontalpodautoscaler]", got)
	}

	// VPA присутствует (реальный формат с API-группой) — HPA + VPA.
	got = call("horizontalpodautoscalers.autoscaling\nverticalpodautoscalers.autoscaling.k8s.io\nnetworkpolicies.networking.k8s.io\n")
	if len(got) != 2 || got[0] != "horizontalpodautoscaler" || got[1] != "verticalpodautoscaler" {
		t.Fatalf("с VPA kinds = %v, want [horizontalpodautoscaler verticalpodautoscaler]", got)
	}

	// VPA-checkpoints не должен приниматься за основной VPA.
	got = call("verticalpodautoscalercheckpoints.autoscaling.k8s.io\n")
	if len(got) != 1 || got[0] != "horizontalpodautoscaler" {
		t.Fatalf("только checkpoints kinds = %v, want [horizontalpodautoscaler]", got)
	}

	// Ошибка api-resources — деградация к HPA.
	runKubectlFn = func(args ...string) ([]byte, error) {
		return nil, fmt.Errorf("rbac denied")
	}
	got = nil
	for _, k := range autoscaleScopeKinds("c1") {
		got = append(got, k.kind)
	}
	if len(got) != 1 || got[0] != "horizontalpodautoscaler" {
		t.Fatalf("при ошибке kinds = %v, want [horizontalpodautoscaler]", got)
	}
}

func TestOverviewAutoscaleWithVPA(t *testing.T) {
	oldCtx, oldRun := getContextsFn, runKubectlFn
	getContextsFn = func() ([]string, error) { return []string{"c1"}, nil }
	defer func() { getContextsFn, runKubectlFn = oldCtx, oldRun }()

	var gotGet string
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if strings.Contains(j, "api-resources") {
			return []byte("horizontalpodautoscalers.autoscaling\nverticalpodautoscalers.autoscaling.k8s.io\n"), nil
		}
		if strings.Contains(j, " get horizontalpodautoscalers,verticalpodautoscalers ") {
			gotGet = j
			return []byte(`{"kind":"List","items":[
				{"kind":"HorizontalPodAutoscalerList","items":[
					{"metadata":{"name":"web-hpa","namespace":"prod","creationTimestamp":"2026-01-01T00:00:00Z"}}
				]},
				{"kind":"VerticalPodAutoscalerList","items":[
					{"metadata":{"name":"web-vpa","namespace":"prod","creationTimestamp":"2026-01-02T00:00:00Z"}}
				]}
			]}`), nil
		}
		return nil, fmt.Errorf("unexpected: %s", j)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/overview?scope=autoscale", nil)
	rec := httptest.NewRecorder()
	handleOverview(rec, req)
	var r relatedResp
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if r.Error != "" {
		t.Fatalf("error = %q", r.Error)
	}
	if !strings.Contains(gotGet, "get horizontalpodautoscalers,verticalpodautoscalers") {
		t.Fatalf("kubectl get = %q, want both hpa and vpa", gotGet)
	}
	kinds := map[string]int{}
	for _, it := range r.Items {
		kinds[it.Kind]++
	}
	if kinds["horizontalpodautoscaler"] != 1 || kinds["verticalpodautoscaler"] != 1 {
		t.Fatalf("kinds = %v, want 1 hpa + 1 vpa: %+v", kinds, r.Items)
	}
}

func TestOverviewScopes(t *testing.T) {
	oldCtx, oldRun := getContextsFn, runKubectlFn
	getContextsFn = func() ([]string, error) { return []string{"c1", "c2"}, nil }
	defer func() { getContextsFn, runKubectlFn = oldCtx, oldRun }()

	runVolume := `{"kind":"List","items":[
		{"kind":"PersistentVolumeClaimList","items":[
			{"metadata":{"name":"web","namespace":"prod","creationTimestamp":"2026-01-01T00:00:00Z"},"spec":{"resources":{"requests":{"storage":"10Gi"}}}},
			{"metadata":{"name":"logs","namespace":"ops","creationTimestamp":"2026-01-02T00:00:00Z"},"spec":{"resources":{"requests":{"storage":"5Gi"}}}}
		]},
		{"kind":"PersistentVolumeList","items":[
			{"metadata":{"name":"pv-a","creationTimestamp":"2026-01-03T00:00:00Z"}}
		]},
		{"kind":"StorageClassList","items":[
			{"metadata":{"name":"standard","creationTimestamp":"2026-01-04T00:00:00Z"}}
		]}
	]}`
	runSVC := `{"kind":"List","items":[
		{"kind":"Service","metadata":{"name":"api","namespace":"ops","creationTimestamp":"2026-01-01T00:00:00Z"}},
		{"kind":"Ingress","metadata":{"name":"web","namespace":"prod","creationTimestamp":"2026-01-02T00:00:00Z"}}
	]}`
	runNet := `{"kind":"List","items":[
		{"kind":"ServiceList","items":[
			{"metadata":{"name":"api","namespace":"ops","creationTimestamp":"2026-01-01T00:00:00Z"}}
		]},
		{"kind":"IngressList","items":[
			{"metadata":{"name":"web","namespace":"prod","creationTimestamp":"2026-01-02T00:00:00Z"}}
		]},
		{"kind":"NetworkPolicyList","items":[
			{"metadata":{"name":"deny-all","namespace":"prod","creationTimestamp":"2026-01-03T00:00:00Z"}}
		]},
		{"kind":"EndpointsList","items":[
			{"metadata":{"name":"api","namespace":"ops","creationTimestamp":"2026-01-04T00:00:00Z"}}
		]},
		{"kind":"EndpointSliceList","items":[
			{"metadata":{"name":"api-abc","namespace":"ops","creationTimestamp":"2026-01-05T00:00:00Z"}}
		]}
	]}`
	runEvents := `{"items":[
		{"metadata":{"namespace":"ops"},"type":"Warning","reason":"BackOff","message":"Back-off restarting failed container","involvedObject":{"kind":"Pod","name":"web-0"},"lastTimestamp":"2026-01-06T00:00:00Z"},
		{"metadata":{"namespace":"prod"},"type":"Normal","reason":"Started","message":"Started container web","involvedObject":{"kind":"Pod","name":"web-0"},"lastTimestamp":"2026-01-06T01:00:00Z"}
	]}`
	runNodes := `{"items":[
		{"metadata":{"name":"n1"},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
		{"metadata":{"name":"n2"},"status":{"conditions":[{"type":"Ready","status":"False"}]}}
	]}`
	runNS := `{"items":[{"metadata":{"name":"default"},"status":{"phase":"Active"}}]}`
	// Реальный kubectl отдаёт плоский List (объекты с собственным kind).
	runJobs := `{"kind":"List","items":[
		{"kind":"Job","metadata":{"name":"migrate","namespace":"prod","creationTimestamp":"2026-01-01T00:00:00Z"}},
		{"kind":"Job","metadata":{"name":"archive","namespace":"ops","creationTimestamp":"2026-01-02T00:00:00Z"}},
		{"kind":"CronJob","metadata":{"name":"nightly","namespace":"prod","creationTimestamp":"2026-01-03T00:00:00Z"}}
	]}`
	runConfigs := `{"kind":"List","items":[
		{"kind":"ConfigMapList","items":[
			{"metadata":{"name":"app-config","namespace":"ops","creationTimestamp":"2026-01-01T00:00:00Z"}}
		]},
		{"kind":"SecretList","items":[
			{"metadata":{"name":"tls","namespace":"ops","creationTimestamp":"2026-01-02T00:00:00Z"}}
		]}
	]}`

	runWorkloads := `{"kind":"List","items":[
		{"kind":"DeploymentList","items":[
			{"metadata":{"name":"web","namespace":"prod","creationTimestamp":"2026-01-01T00:00:00Z"}}
		]},
		{"kind":"DaemonSetList","items":[
			{"metadata":{"name":"node-exporter","namespace":"kube-system","creationTimestamp":"2026-01-01T00:00:00Z"}}
		]},
		{"kind":"StatefulSetList","items":[
			{"metadata":{"name":"db","namespace":"ops","creationTimestamp":"2026-01-01T00:00:00Z"}}]},
		{"kind":"ReplicaSetList","items":[]}
	]}`

	runAutoscale := `{"kind":"List","items":[
		{"kind":"HorizontalPodAutoscalerList","items":[
			{"metadata":{"name":"web-hpa","namespace":"prod","creationTimestamp":"2026-01-01T00:00:00Z"}},
			{"metadata":{"name":"db-hpa","namespace":"ops","creationTimestamp":"2026-01-02T00:00:00Z"}}
		]},
		{"kind":"VerticalPodAutoscalerList","items":[]}
	]}`

	runQuota := `{"kind":"List","items":[
		{"kind":"ResourceQuotaList","items":[
			{"metadata":{"name":"compute-quota","namespace":"prod","creationTimestamp":"2026-01-01T00:00:00Z"}}
		]},
		{"kind":"LimitRangeList","items":[
			{"metadata":{"name":"cpu-mem-limit","namespace":"prod","creationTimestamp":"2026-01-02T00:00:00Z"}}
		]}
	]}`

	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		switch {
		case strings.Contains(j, "api-resources"):
			return []byte("horizontalpodautoscalers\n"), nil // VPA CRD отсутствует
		case strings.Contains(j, "get persistentvolumeclaims,persistentvolumes,storageclasses"):
			return []byte(runVolume), nil
		case strings.Contains(j, "get services,ingresses,networkpolicies,endpoints,endpointslices"):
			return []byte(runNet), nil
		case strings.Contains(j, "get services"):
			return []byte(runSVC), nil
		case strings.Contains(j, "get nodes"):
			return []byte(runNodes), nil
		case strings.Contains(j, "get namespaces"):
			return []byte(runNS), nil
		case strings.Contains(j, "get jobs,cronjobs"):
			return []byte(runJobs), nil
		case strings.Contains(j, "get configmaps,secrets"):
			return []byte(runConfigs), nil
		case strings.Contains(j, "get events"):
			return []byte(runEvents), nil
		case strings.Contains(j, "get deployments,daemonsets,statefulsets,replicasets"):
			return []byte(runWorkloads), nil
		case strings.Contains(j, "get horizontalpodautoscalers"):
			return []byte(runAutoscale), nil
		case strings.Contains(j, "get resourcequotas,limitranges"):
			return []byte(runQuota), nil
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

	// pvc: оба контекста, у каждого по 2 PVC + 1 PV + 1 StorageClass → 4 элемента PVC.
	r := call("pvc")
	if len(r.Items) != 8 {
		t.Fatalf("pvc items = %d, want 8", len(r.Items))
	}
	found := false
	for _, it := range r.Items {
		if it.Kind == "persistentvolumeclaim" && (it.Ns == "" || it.Ctx == "" || (it.Reason != "10.00 GiB" && it.Reason != "5.00 GiB")) {
			t.Errorf("pvc item = %+v", it)
		}
		if it.Kind != "persistentvolumeclaim" && it.Kind != "persistentvolume" && it.Kind != "storageclass" {
			t.Errorf("pvc unexpected kind = %+v", it)
		}
		if it.Name == "web" && it.Ns == "prod" {
			found = true
		}
	}
	if !found {
		t.Errorf("pvc web/prod не найден: %+v", r.Items)
	}

	// svc: все Network-манифесты из обоих контекстов (2× service+ingress+…) — 10 элементов.
	r = call("svc")
	if len(r.Items) != 10 {
		t.Fatalf("svc items = %d, want 10", len(r.Items))
	}
	kinds := map[string]bool{}
	for _, it := range r.Items {
		kinds[it.Kind] = true
		if it.Ns == "" || it.Ctx == "" {
			t.Errorf("svc item = %+v", it)
		}
	}
	if !kinds["service"] || !kinds["ingress"] || !kinds["networkpolicy"] {
		t.Errorf("svc kinds = %v, want service+ingress+networkpolicy", kinds)
	}

	// events: warning/normal события (по каждому контексту из моков).
	r = call("events")
	if len(r.Items) < 2 {
		t.Fatalf("events items = %d, want >= 2", len(r.Items))
	}
	var hasWarn, hasNormal bool
	for _, it := range r.Items {
		if it.Kind != "event" {
			t.Errorf("event kind = %q, want event", it.Kind)
		}
		if it.Type == "Warning" {
			hasWarn = true
			if it.Message == "" || it.Reason == "" {
				t.Errorf("warning event %+v: empty reason/message", it)
			}
		} else if it.Type == "Normal" {
			hasNormal = true
		}
	}
	if !hasWarn || !hasNormal {
		t.Errorf("events missing Warning/Normal: %+v", r.Items)
	}

	// autoscale: HPA из обоих контекстов; VPA отсутствует (CRD нет в api-resources).
	r = call("autoscale")
	if len(r.Items) != 4 {
		t.Fatalf("autoscale items = %d, want 4 (2 ctx x 2 HPA)", len(r.Items))
	}
	for _, it := range r.Items {
		if it.Kind != "horizontalpodautoscaler" || it.Ns == "" || it.Ctx == "" {
			t.Errorf("autoscale item = %+v", it)
		}
	}

	// quota: ResourceQuota + LimitRange по каждому контексту.
	r = call("quota")
	if len(r.Items) != 4 {
		t.Fatalf("quota items = %d, want 4 (2 ctx x quota+limitrange)", len(r.Items))
	}
	kindsQ := map[string]bool{}
	for _, it := range r.Items {
		kindsQ[it.Kind] = true
		if it.Ns == "" || it.Ctx == "" {
			t.Errorf("quota item = %+v", it)
		}
	}
	if !kindsQ["resourcequota"] || !kindsQ["limitrange"] {
		t.Errorf("quota kinds = %v, want resourcequota+limitrange", kindsQ)
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

	// jobs: 2 контекста × (2 job + 1 cronjob) = 6 элементов.
	r = call("jobs")
	if len(r.Items) != 6 {
		t.Fatalf("jobs items = %d, want 6", len(r.Items))
	}
	gotKind := map[string]int{}
	catOK := true
	for _, it := range r.Items {
		gotKind[it.Kind]++
		if it.Ctx == "" || it.Ns == "" {
			t.Errorf("jobs item должен иметь ctx и ns: %+v", it)
		}
		if it.Cat != "Workload" {
			catOK = false
		}
	}
	if gotKind["job"] != 4 || gotKind["cronjob"] != 2 || !catOK {
		t.Errorf("jobs разбивка = %v", gotKind)
	}

	// workloads: deployment, daemonset, statefulset (replicaset пуст) × 2 контекста.
	r = call("workloads")
	if len(r.Items) != 6 {
		t.Fatalf("workloads items = %d, want 6", len(r.Items))
	}
	gotKind = map[string]int{}
	allWorkload := true
	for _, it := range r.Items {
		gotKind[it.Kind]++
		if it.Cat != "Workload" || it.Ctx == "" || it.Ns == "" {
			allWorkload = false
		}
	}
	if gotKind["deployment"] != 2 || gotKind["daemonset"] != 2 || gotKind["statefulset"] != 2 || !allWorkload {
		t.Errorf("workloads разбивка = %v", gotKind)
	}

	// configs: configmap + secret, разные категории (Config и Secret).
	r = call("configs")
	if len(r.Items) != 4 {
		t.Fatalf("configs items = %d, want 4", len(r.Items))
	}
	gotKind = map[string]int{}
	gotCat := map[string]int{}
	nsOK := true
	for _, it := range r.Items {
		gotKind[it.Kind]++
		gotCat[it.Cat]++
		if it.Ctx == "" || it.Ns == "" {
			nsOK = false
		}
	}
	if gotKind["configmap"] != 2 || gotKind["secret"] != 2 || !nsOK {
		t.Errorf("configs разбивка = %v", gotKind)
	}
	if gotCat["Config"] != 2 || gotCat["Secret"] != 2 {
		t.Errorf("configs категории = %v", gotCat)
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

func TestOverviewScopeEdgeCases(t *testing.T) {
	oldCtx, oldRun := getContextsFn, runKubectlFn
	defer func() { getContextsFn, runKubectlFn = oldCtx, oldRun }()

	call := func(target string) relatedResp {
		rec := httptest.NewRecorder()
		handleOverview(rec, httptest.NewRequest(http.MethodGet, target, nil))
		var r relatedResp
		if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
			t.Fatalf("%s: bad json: %v", target, err)
		}
		return r
	}

	// Нет scope.
	if r := call("/api/overview"); r.Error != "missing scope" {
		t.Errorf("missing scope error = %q", r.Error)
	}

	// Ошибка получения контекстов пробрасывается в Error.
	getContextsFn = func() ([]string, error) { return nil, fmt.Errorf("no kubeconfig") }
	if r := call("/api/overview?scope=ctx"); r.Error != "no kubeconfig" {
		t.Errorf("contexts error = %q", r.Error)
	}

	// Неизвестный scope.
	getContextsFn = func() ([]string, error) { return []string{"c1"}, nil }
	if r := call("/api/overview?scope=bogus"); r.Error != "unknown scope" {
		t.Errorf("unknown scope error = %q", r.Error)
	}

	// onlyCtx ограничивает контексты; charts резолвит имя helm-релиза.
	getContextsFn = func() ([]string, error) { return []string{"c1", "c2"}, nil }
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if strings.Contains(j, "get secrets") && strings.Contains(j, "owner=helm") {
			return []byte(`{"items":[{"metadata":{"name":"sh.helm.release.v1.app.v2","namespace":"ops","creationTimestamp":"2026-01-01T00:00:00Z"}}]}`), nil
		}
		return nil, fmt.Errorf("unexpected: %s", j)
	}
	r := call("/api/overview?scope=charts&ctx=c2")
	if r.Error != "" || len(r.Items) != 1 {
		t.Fatalf("charts items = %+v (err %q)", r.Items, r.Error)
	}
	if r.Items[0].Kind != "chart" || r.Items[0].Name != "app" || r.Items[0].Ctx != "c2" || r.Items[0].Ns != "ops" {
		t.Errorf("charts item = %+v", r.Items[0])
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
		"secret/tls-secret":              "Secret",
		"persistentvolumeclaim/data-pvc": "Storage",
		"serviceaccount/coredns":         "RBAC",
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

// --- кеш агрегированных данных ---

func TestDataCacheTTL(t *testing.T) {
	calls := 0
	c := newDataCache(2 * time.Second)
	c.now = func() time.Time { return time.Unix(1000, 0) }
	d := c.get(func() *PageData {
		calls++
		return &PageData{Count: 1}
	})
	if d.Count != 1 || calls != 1 {
		t.Fatalf("первый вызов: calls=%d count=%d", calls, d.Count)
	}
	d2 := c.get(func() *PageData {
		calls++
		return &PageData{Count: 2}
	})
	if d2 != d || calls != 1 {
		t.Fatalf("свежий кеш: calls=%d, ожидали повторное использование", calls)
	}
}

func TestDataCacheExpiry(t *testing.T) {
	calls := 0
	c := newDataCache(30 * time.Millisecond)
	c.now = time.Now
	d := c.get(func() *PageData { calls++; return &PageData{Count: 1} })
	_ = d
	time.Sleep(60 * time.Millisecond)
	_ = c.get(func() *PageData { calls++; return &PageData{Count: 2} })
	if calls != 2 {
		t.Fatalf("после истечения TTL fetch вызван %d раз, ожидали 2", calls)
	}
}

func TestDataCacheSingleFlight(t *testing.T) {
	calls := 0
	c := newDataCache(0) // TTL 0: кеш выключен, но single-flight активен
	var started sync.WaitGroup
	started.Add(8)
	var wg sync.WaitGroup
	results := make([]*PageData, 8)
	var mu sync.Mutex
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			started.Done()
			started.Wait()
			d := c.get(func() *PageData {
				calls++
				time.Sleep(50 * time.Millisecond)
				return &PageData{Count: 99}
			})
			mu.Lock()
			results[i] = d
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("single-flight: fetch вызван %d раз, ожидали 1", calls)
	}
	for i := 1; i < len(results); i++ {
		if results[i] != results[0] {
			t.Fatalf("goroutine %d получил другой указатель", i)
		}
	}
}

func TestDataCacheBypassZeroTTL(t *testing.T) {
	calls := 0
	c := newDataCache(0)
	d := c.get(func() *PageData { calls++; return &PageData{Count: 1} })
	_ = d
	d2 := c.get(func() *PageData { calls++; return &PageData{Count: 2} })
	if calls != 2 {
		t.Fatalf("при TTL=0 fetch вызван %d раз, ожидали 2", calls)
	}
	if d2.Count != 2 {
		t.Fatalf("при TTL=0 данные должны быть свежими: %d", d2.Count)
	}
}

func TestFlushDataCache(t *testing.T) {
	oldTTL := dataTTL
	dataTTL = time.Hour
	defer func() { dataTTL = oldTTL }()
	flushDataCache()
	c := overviewCache
	_ = c.get(func() *PageData { return &PageData{Count: 1} })
	flushDataCache()
	if overviewCache == c {
		t.Fatal("flushDataCache не заменил экземпляр кеша")
	}
}

// --- env-хелперы ---

func TestEnvHelpers(t *testing.T) {
	old := getenv
	defer func() { getenv = old }()
	getenv = func(k string) string {
		switch k {
		case "CB_PORT":
			return "  9001  "
		case "CB_DATA_CACHE":
			return "42"
		case "EMPTY":
			return "   "
		case "BAD":
			return "abc"
		}
		return ""
	}
	if got := envOr("CB_PORT", ":8866"); got != "9001" {
		t.Errorf("envOr(CB_PORT) = %q, ожидали 9001", got)
	}
	if got := envOr("EMPTY", "def"); got != "def" {
		t.Errorf("envOr(EMPTY) = %q, ожидали def", got)
	}
	if got := envDurationSeconds("CB_DATA_CACHE", 15*time.Second); got != 42*time.Second {
		t.Errorf("envDurationSeconds(CB_DATA_CACHE) = %v, ожидали 42s", got)
	}
	if got := envDurationSeconds("BAD", 15*time.Second); got != 15*time.Second {
		t.Errorf("envDurationSeconds(BAD) = %v, ожидали дефолт", got)
	}
	// CB_DATA_CACHE=30 — кеш отключён (dataTTL=0), а не игнор значения (фикс 2).
	if got := envDurationSeconds("CB_DATA_CACHE", 15*time.Second); got == 15*time.Second {
		t.Errorf("envDurationSeconds не вернул 0 (кеш отключён)")
	}
	getenv = func(k string) string { return "0" }
	if got := envDurationSeconds("CB_DATA_CACHE", 15*time.Second); got != 0 {
		t.Errorf("envDurationSeconds(CB_DATA_CACHE=0) = %v, ожидали 0", got)
	}
	// TestMain выставляет dataTTL = 0 до прогона тестов
	if dataTTL != 0 {
		t.Errorf("dataTTL = %v в тестах, ожидали 0", dataTTL)
	}
}

func TestDataCachePendingOK(t *testing.T) {
	// Паника в fetch не должна вешать single-flight (фикс 3).
	c := newDataCache(30 * time.Second)
	entered := make(chan struct{})
	var leaderDone sync.WaitGroup
	leaderDone.Add(1)
	go func() {
		defer leaderDone.Done()
		c.get(func() *PageData {
			close(entered)
			time.Sleep(30 * time.Millisecond)
			panic("boom")
		})
	}()
	<-entered // лидер вошёл в fetch и вот-вот упадёт

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.get(func() *PageData { return &PageData{Count: 7} })
		}()
	}
	done := make(chan struct{})
	go func() { leaderDone.Wait(); wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("паника в fetch повесила параллельные вызовы (get не завершился)")
	}
	// После паники кеш не должен оставаться заблокированным.
	if d := c.get(func() *PageData { return &PageData{Count: 9} }); d.Count != 9 {
		t.Fatalf("кеш не восстановился после паники: %+v", d)
	}
}

func TestDataCacheViewBytes(t *testing.T) {
	// view() кеширует отрендеренные байты, обновляя их только при cache miss (фикс 6).
	renders := 0
	c := newDataCache(0)
	_, html, json := c.view(func() *PageData {
		return &PageData{Count: 1}
	}, func(d *PageData) ([]byte, []byte) {
		renders++
		return []byte("<html>"), []byte(`{"count":1}`)
	})
	if renders != 1 || string(html) != "<html>" || string(json) != `{"count":1}` {
		t.Fatalf("первый view: renders=%d html=%s json=%s", renders, html, json)
	}
	// TTL=0: каждый вызов рендерит заново.
	_, html2, _ := c.view(func() *PageData {
		return &PageData{Count: 2}
	}, func(d *PageData) ([]byte, []byte) {
		renders++
		return []byte("<html2>"), []byte(`{"count":2}`)
	})
	if renders != 2 || string(html2) != "<html2>" {
		t.Fatalf("TTL=0: renders=%d html=%s", renders, html2)
	}
}

func TestDataCacheViewCachesBytesOnTTL(t *testing.T) {
	c := newDataCache(2 * time.Second)
	c.now = func() time.Time { return time.Unix(1000, 0) }
	renders := 0
	fetch := func() *PageData { return &PageData{Count: 1} }
	render := func(d *PageData) ([]byte, []byte) {
		renders++
		return []byte("<html>"), []byte(`{"count":1}`)
	}
	_, html, _ := c.view(fetch, render)
	if renders != 1 {
		t.Fatalf("первый view renders=%d, ожидали 1", renders)
	}
	_, html2, _ := c.view(func() *PageData { return &PageData{Count: 2} }, render)
	if renders != 1 {
		t.Fatalf("горячий кеш renders=%d, ожидали повторное использование байтов", renders)
	}
	if string(html2) != "<html>" {
		t.Fatalf("горячий кеш вернул другой html: %s", html2)
	}
	_ = html
}

func TestDataCacheStress(t *testing.T) {
	// Стресс single-flight при одновременном доступе (замена go test -race).
	c := newDataCache(0)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d := c.get(func() *PageData {
				time.Sleep(time.Duration(i%5) * time.Millisecond)
				return &PageData{Count: i}
			})
			if d == nil {
				t.Error("get вернул nil")
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("dataCache stress: deadlock")
	}
}

func TestViewRecoverOnRenderPanic(t *testing.T) {
	// Паника в render (фикс 3) — параллельные view() должны завершиться.
	c := newDataCache(0)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = c.view(func() *PageData {
				return &PageData{Count: 1}
			}, func(d *PageData) ([]byte, []byte) {
				panic("render boom")
			})
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("паника в render повесила view()")
	}
	if d := c.get(func() *PageData { return &PageData{Count: 5} }); d.Count != 5 {
		t.Fatalf("кеш не восстановился после паники render: %+v", d)
	}
}

// --- парсеры нагрузки ---

func TestCPUMilli(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"100m", 100, true},
		{"250m", 250, true},
		{"1", 1000, true},
		{"0.5", 500, true},
		{"2", 2000, true},
		{"", 0, false},
		{"abc", 0, false},
		{"m", 0, false},
		{"-1", 0, false},
		{"  300m  ", 300, true},
	}
	for _, c := range cases {
		got, ok := cpuMilli(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("cpuMilli(%q) = %d,%v; ожидали %d,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestMemBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"128Mi", 128 << 20, true},
		{"1Gi", 1 << 30, true},
		{"512M", 512e6, true},
		{"2G", 2e9, true},
		{"1T", 1e12, true},
		{"1Ti", 1 << 40, true},
		{"512", 512, true},
		{"", 0, false},
		{"abc", 0, false},
		{"-1Mi", 0, false},
		{"  64Mi  ", 64 << 20, true},
	}
	for _, c := range cases {
		got, ok := memBytes(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("memBytes(%q) = %d,%v; ожидали %d,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestHelmReleaseYAML(t *testing.T) {
	// Собираем настоящий helm-секрет: base64(gzip(json)) с информацией о релизе.
	rel := map[string]interface{}{
		"name":    "myapp",
		"version": 1,
		"info":    map[string]interface{}{"status": "deployed"},
		"chart":   map[string]interface{}{"metadata": map[string]interface{}{"name": "myapp-chart"}},
	}
	payload, _ := json.Marshal(rel)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(payload)
	gz.Close()
	releaseB64 := base64.StdEncoding.EncodeToString(buf.Bytes())

	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()
	runKubectlFn = func(args ...string) ([]byte, error) {
		if len(args) > 0 && args[0] == "--context" {
			if strings.Contains(strings.Join(args, " "), "get secrets") {
				return []byte(`{"items":[{"metadata":{"name":"sh.helm.release.v1.myapp.v2"}}]}`), nil
			}
			if strings.Contains(strings.Join(args, " "), "get secret sh.helm.release.v1.myapp.v2") {
				sec := fmt.Sprintf(`{"type":"helm.sh/release.v1","data":{"release":%q}}`, releaseB64)
				return []byte(sec), nil
			}
		}
		return nil, fmt.Errorf("unexpected args: %v", args)
	}

	out, err := helmReleaseYAML("ctx", "ns", "myapp")
	if err != nil {
		t.Fatalf("helmReleaseYAML: %v", err)
	}
	if !strings.Contains(out, "myapp") || !strings.Contains(out, "deployed") {
		t.Errorf("вывод не содержит данных релиза: %s", out)
	}

	// Ревизия v2 выбрана, а не v1.
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "get secrets") {
			return []byte(`{"items":[{"metadata":{"name":"sh.helm.release.v1.myapp.v1"}},{"metadata":{"name":"sh.helm.release.v1.myapp.v2"}}]}`), nil
		}
		return nil, fmt.Errorf("unexpected args: %v", args)
	}
	if _, err := helmReleaseYAML("ctx", "ns", "myapp"); !strings.Contains(err.Error(), "v2") {
		t.Errorf("не выбрана свежая ревизия: %v", err)
	}
}

func TestHelmReleaseYAMLErrors(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	secretsList := []byte(`{"items":[{"metadata":{"name":"sh.helm.release.v1.myapp.v2"}}]}`)

	// 1) Ошибка получения списка секретов.
	runKubectlFn = func(args ...string) ([]byte, error) { return nil, fmt.Errorf("rbac") }
	if _, err := helmReleaseYAML("ctx", "ns", "myapp"); err == nil {
		t.Error("ожидалась ошибка releaseSecretForName")
	}

	// 2) Ошибка получения самого секрета.
	runKubectlFn = func(args ...string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "get secrets") {
			return secretsList, nil
		}
		return nil, fmt.Errorf("get secret failed")
	}
	if _, err := helmReleaseYAML("ctx", "ns", "myapp"); err == nil {
		t.Error("ожидалась ошибка get secret")
	}

	withSecret := func(secJSON string) {
		runKubectlFn = func(args ...string) ([]byte, error) {
			if strings.Contains(strings.Join(args, " "), "get secrets") {
				return secretsList, nil
			}
			return []byte(secJSON), nil
		}
	}

	// 3) Битый JSON секрета.
	withSecret("{not json")
	if _, err := helmReleaseYAML("ctx", "ns", "myapp"); err == nil {
		t.Error("ожидалась ошибка json секрета")
	}

	// 4) Нет данных release.
	withSecret(`{"type":"helm.sh/release.v1","data":{}}`)
	if _, err := helmReleaseYAML("ctx", "ns", "myapp"); err == nil {
		t.Error("ожидалась ошибка «no release data»")
	}

	// 5) release есть, но это не gzip.
	withSecret(`{"type":"helm.sh/release.v1","data":{"release":"aGVsbG8="}}`)
	if _, err := helmReleaseYAML("ctx", "ns", "myapp"); err == nil {
		t.Error("ожидалась ошибка gzip")
	}

	// 6) gzip есть, но внутри не JSON.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write([]byte("not-json"))
	gz.Close()
	b64 := base64.StdEncoding.EncodeToString(buf.Bytes())
	withSecret(fmt.Sprintf(`{"type":"helm.sh/release.v1","data":{"release":%q}}`, b64))
	if _, err := helmReleaseYAML("ctx", "ns", "myapp"); err == nil {
		t.Error("ожидалась ошибка json внутри gzip")
	}
}

// --- restartFollowStream: give-up и отсутствие утечки капитала ---

// failStreamSource всегда возвращает ошибку открытия потока.
func TestRestartFollowStreamGivesUp(t *testing.T) {
	calls := 0
	src := func(sel Selection, since, sinceTime string, follow bool, lines int) (io.ReadCloser, error) {
		calls++
		return nil, fmt.Errorf("boom %d", calls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := restartFollowStream(ctx, []Selection{{Context: "c", Namespace: "ns", Pod: "p"}}, "", true, 0, src)

	// Ожидается ровно 2 события ошибки: "will retry" и финальный give-up,
	// затем канал закрывается.
	var events []LogStreamEvent
	timeout := time.NewTimer(followMaxErrors*followRetryDelay + 3*time.Second)
	defer timeout.Stop()
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				goto done
			}
			events = append(events, ev)
			if ev.Err == nil {
				t.Fatalf("неожиданное событие без ошибки: %+v", ev)
			}
		case <-timeout.C:
			t.Fatalf("restartFollowStream не завершился: событий=%d", len(events))
		}
	}
done:
	if len(events) != 2 {
		t.Fatalf("событий ошибок %d, ожидали 2 (will retry + give-up)", len(events))
	}
	if !strings.Contains(events[0].Err.Error(), "will retry") {
		t.Errorf("первое событие не содержит will retry: %v", events[0].Err)
	}
	if !strings.Contains(events[1].Err.Error(), "giving up") {
		t.Errorf("финальное событие не содержит give-up: %v", events[1].Err)
	}
	if calls != followMaxErrors {
		t.Errorf("источник вызван %d раз, ожидали %d (give-up)", calls, followMaxErrors)
	}
}

// Блокирующий reader, который разблокируется при Close (как procReader):
// restartFollowStream держит его открытым, пока не отменён контекст.
type interruptibleReader struct {
	closed chan struct{}
}

func newInterruptibleReader() *interruptibleReader {
	return &interruptibleReader{closed: make(chan struct{})}
}

func (r *interruptibleReader) Read(p []byte) (int, error) {
	<-r.closed
	return 0, io.EOF
}

func (r *interruptibleReader) Close() error {
	select {
	case <-r.closed:
	default:
		close(r.closed)
	}
	return nil
}

func TestRestartFollowStreamCancelsCleanly(t *testing.T) {
	var mu sync.Mutex
	var opened int
	var readers []*interruptibleReader
	src := sourceFunc(func(sel Selection, since, sinceTime string, follow bool, lines int) (io.ReadCloser, error) {
		mu.Lock()
		opened++
		r := newInterruptibleReader()
		readers = append(readers, r)
		mu.Unlock()
		return r, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := restartFollowStream(ctx, []Selection{{Context: "c", Namespace: "ns", Pod: "p"}}, "", true, 10, src)

	// Источник открыт и ждёт данные (блокирующее чтение).
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("не ожидали события от пустого стрима: %+v", ev)
		}
	case <-time.After(200 * time.Millisecond):
		// Ок: поток открыт.
	}
	mu.Lock()
	got := opened
	mu.Unlock()
	if got < 1 {
		t.Fatalf("источник не открылся (opened=%d)", got)
	}

	cancel()

	// После отмены контекста все reader'ы закрываются (совместимо со streamClose),
	// канал обязан закрыться — значит, goroutine не утекают.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("канал должен закрыться после отмены контекста")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("канал не закрылся после отмены (утечка goroutine)")
	}
	for _, r := range readers {
		r.Close() // идиоматично для defer-закрытия: close(done)
	}
}

func TestAuthDisabledByDefault(t *testing.T) {
	// Без CB_AUTH_USERNAME/CB_AUTH_PASSWORD авторизация выключена — прямые
	// запросы к индексу и к API работают.
	old := getenv
	defer func() {
		getenv = old
		authEnabled = false
		authUser, authPass = "", ""
		authTTL = defaultAuthTTL
		authSecret = nil
	}()
	getenv = func(k string) string { return "" }
	authSetup()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handleIndex(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleIndex без авторизации: status = %d, want 200", rec.Code)
	}
	if authorize(req) != true {
		t.Error("authorize() без включённой авторизации должен возвращать true")
	}
}

func TestAuthLoginFlow(t *testing.T) {
	old := getenv
	defer func() {
		getenv = old
		// Возвращаем состояние «авторизация выключена», чтобы не задеть другие тесты.
		authEnabled = false
		authUser, authPass = "", ""
		authTTL = defaultAuthTTL
		authSecret = nil
	}()
	getenv = func(k string) string {
		switch k {
		case "CB_AUTH_USERNAME":
			return "admin"
		case "CB_AUTH_PASSWORD":
			return "secret123"
		case "CB_AUTH_CACHE":
			return "60"
		}
		return ""
	}
	authSetup()
	if !authEnabled {
		t.Fatal("авторизация должна быть включена после authSetup, когда заданы логин/пароль")
	}
	if authUser != "admin" || authPass != "secret123" {
		t.Fatalf("authSetup не прочитал креды: %q/%q", authUser, authPass)
	}
	if authTTL != 60*time.Second {
		t.Errorf("authTTL = %v, want 60s (из CB_AUTH_CACHE)", authTTL)
	}

	// GET / без сессии — форма логина с шапкой и дизайном страницы.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	authHandler(http.HandlerFunc(handleIndex)).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /: status = %d, want 200 (форма логина)", rec.Code)
	}
	page := rec.Body.String()
	for _, want := range []string{"CrossBoard", "Sign in", "username", "password", "CB"} {
		if !strings.Contains(page, want) {
			t.Errorf("форма логина не содержит %q", want)
		}
	}

	// API без сессии — 401.
	reqAPI := httptest.NewRequest(http.MethodGet, "/api/pods", nil)
	recAPI := httptest.NewRecorder()
	authHandler(http.HandlerFunc(handleAPI)).ServeHTTP(recAPI, reqAPI)
	if recAPI.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/pods без сессии: status = %d, want 401", recAPI.Code)
	}

	// Неверные креды — снова форма, 200, без cookie сессии.
	reqBad := httptest.NewRequest(http.MethodPost, "/login", nil)
	reqBad.PostForm = url.Values{"username": {"admin"}, "password": {"wrong"}}
	recBad := httptest.NewRecorder()
	authHandler(http.HandlerFunc(handleIndex)).ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusOK {
		t.Fatalf("POST /login с неверными кредами: status = %d, want 200 (форма с ошибкой)", recBad.Code)
	}
	if !strings.Contains(recBad.Body.String(), "Неверные") {
		t.Error("форма после неверного входа должна показывать сообщение об ошибке")
	}

	// Правильные креды — Set-Cookie и редирект.
	reqOK := httptest.NewRequest(http.MethodPost, "/login", nil)
	reqOK.PostForm = url.Values{"username": {"admin"}, "password": {"secret123"}}
	recOK := httptest.NewRecorder()
	authHandler(http.HandlerFunc(handleIndex)).ServeHTTP(recOK, reqOK)
	if recOK.Code != http.StatusFound {
		t.Fatalf("POST /login с верными кредами: status = %d, want 302", recOK.Code)
	}
	cookies := recOK.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("после успешного входа не установлена cookie")
	}
	var session *http.Cookie
	for _, c := range cookies {
		if c.Name == authCookie {
			session = c
		}
	}
	if session == nil {
		t.Fatalf("cookie %q не найдена (есть %v)", authCookie, cookies)
	}
	if !authValid(session.Value) {
		t.Error("выданный токен сессии не проходит authValid")
	}
	if session.MaxAge != 60 {
		t.Errorf("MaxAge = %d, want 60 (TTL из CB_AUTH_CACHE)", session.MaxAge)
	}

	// С валидной cookie GET / уже отдаёт дашборд, а не форму.
	reqDash := httptest.NewRequest(http.MethodGet, "/", nil)
	reqDash.AddCookie(session)
	recDash := httptest.NewRecorder()
	authHandler(http.HandlerFunc(handleIndex)).ServeHTTP(recDash, reqDash)
	if recDash.Code != http.StatusOK {
		t.Fatalf("GET / с сессией: status = %d, want 200", recDash.Code)
	}
	if strings.Contains(recDash.Body.String(), "Sign in") {
		t.Error("с валидной сессией не должна показываться форма логина")
	}
}

func TestAuthValidToken(t *testing.T) {
	// authValid должен отклонять поддельные и просроченные токены.
	old := getenv
	defer func() { getenv = old; authEnabled = false; authSecret = nil }()
	getenv = func(k string) string {
		switch k {
		case "CB_AUTH_USERNAME":
			return "admin"
		case "CB_AUTH_PASSWORD":
			return "secret123"
		case "CB_AUTH_CACHE":
			return "3600"
		}
		return ""
	}
	authSetup()

	good := authToken("admin", time.Hour)
	if !authValid(good) {
		t.Error("корректный токен должен проходить authValid")
	}
	if authValid("garbage") {
		t.Error("мусорный токен не должен проходить authValid")
	}
	// Подделка с тем же пользователем другим сроком — подпись не сойдётся.
	if authValid(good + "x") {
		t.Error("модифицированный токен не должен проходить authValid")
	}
	// Просроченный токен с корректной подписью.
	expired := authToken("admin", -time.Hour)
	if authValid(expired) {
		t.Error("просроченный токен не должен проходить authValid")
	}
}

func TestEventsStream(t *testing.T) {
	// Потоковый /events шлёт по SSE снапшоты событий в том же формате, что и
	// /api/overview?scope=events, периодически via ticker. Проверяем первый
	// "data:" кадр.
	old := getContextsFn
	oldRun := runKubectlFn
	defer func() {
		getContextsFn = old
		runKubectlFn = oldRun
	}()
	getContextsFn = func() ([]string, error) { return []string{"c1", "c2"}, nil }
	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		if strings.Contains(j, "get events") {
			return []byte(`{"items":[
				{"metadata":{"namespace":"ops"},"type":"Warning","reason":"BackOff","message":"back-off","involvedObject":{"kind":"Pod","name":"web-0"},"lastTimestamp":"2026-01-06T00:00:00Z"},
				{"metadata":{"namespace":"prod"},"type":"Normal","reason":"Started","message":"started","involvedObject":{"kind":"Pod","name":"web-1"},"lastTimestamp":"2026-01-06T01:00:00Z"}
			]}`), nil
		}
		return nil, fmt.Errorf("unexpected: %s", j)
	}

	// http.ResponseWriter, потокобезопасно копящий записанные байты.
	w := &sseWriter{hdr: make(http.Header)}
	w.hdr.Set("Content-Type", "text/event-stream; charset=utf-8")

	req := httptest.NewRequest(http.MethodGet, "/events?interval=1", nil)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	done := make(chan struct{})
	go func() {
		handleEvents(w, req)
		close(done)
	}()

	wait := make(chan struct{})
	go func() {
		for {
			w.mu.Lock()
			has := bytes.Contains(w.buf.Bytes(), []byte("data: "))
			w.mu.Unlock()
			if has {
				close(wait)
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	select {
	case <-wait:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("поток не отправил ни одного data-кадра")
	}
	cancel()
	<-done

	w.mu.Lock()
	body := w.buf.String()
	w.mu.Unlock()
	for _, want := range []string{"retry: 2000", `"reason":"BackOff"`, `"reason":"Started"`, `"ctx":"c1"`, `"ctx":"c2"`} {
		if !strings.Contains(body, want) {
			t.Errorf("поток не содержит %q", want)
		}
	}
}

func TestProcReaderReadClose(t *testing.T) {
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	r := &procReader{r: pr}
	if _, err := pw.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	pw.Close()
	buf := make([]byte, 16)
	n, err := r.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("Read = %q, %v", buf[:n], err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	// Повторный Close идемпотентен (sync.Once).
	if err := r.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	// Пустой procReader тоже безопасно закрывается.
	if err := (&procReader{}).Close(); err != nil {
		t.Fatalf("empty Close = %v", err)
	}
}

func TestOpenKubectlLogStreamNotFound(t *testing.T) {
	t.Setenv("PATH", "")
	_, err := openKubectlLogStream(Selection{Context: "c1", Namespace: "ns", Pod: "p"}, "", "", false, 10)
	if err == nil {
		t.Fatal("ожидалась ошибка запуска kubectl при пустом PATH")
	}
}

func TestAuthHandlerLoginGetAndLogout(t *testing.T) {
	old := getenv
	defer func() {
		getenv = old
		authEnabled = false
		authUser, authPass = "", ""
		authTTL = defaultAuthTTL
		authSecret = nil
	}()
	getenv = func(k string) string {
		switch k {
		case "CB_AUTH_USERNAME":
			return "admin"
		case "CB_AUTH_PASSWORD":
			return "secret123"
		}
		return ""
	}
	authSetup()

	h := authHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// GET /login — страница логина.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/login", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Sign in") {
		t.Errorf("GET /login: status=%d body=%q", rec.Code, rec.Body.String())
	}

	// GET /logout — редирект и сброс cookie.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/logout", nil))
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != authLoginURL {
		t.Errorf("GET /logout: status=%d loc=%q", rec.Code, rec.Header().Get("Location"))
	}

	// /logs и /events без сессии — 401 (SSE-маршруты).
	for _, p := range []string{"/logs", "/events"} {
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s без сессии: status=%d, want 401", p, rec.Code)
		}
	}
}

// sseWriter — http.ResponseWriter, потокобезопасно копящий записанные байты.
type sseWriter struct {
	mu  sync.Mutex
	hdr http.Header
	buf bytes.Buffer
}

func (w *sseWriter) Header() http.Header { return w.hdr }
func (w *sseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}
func (w *sseWriter) WriteHeader(code int) {}
func (w *sseWriter) Flush()               {}

func TestLogsHandlerStreamingUnsupported(t *testing.T) {
	// plainWriter не реализует http.Flusher → 500.
	plain := &plainWriter{}
	handleLogs(plain, httptest.NewRequest(http.MethodGet, "/logs?sel=x|y|z", nil))
	if plain.code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", plain.code)
	}
}

func TestLogsHandlerWriteError(t *testing.T) {
	oldOut := log.Writer()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(oldOut)

	old := openLogStreamFn
	openLogStreamFn = func(sel Selection, since, sinceTime string, follow bool, lines int) (io.ReadCloser, error) {
		return nil, fmt.Errorf("open boom")
	}
	defer func() { openLogStreamFn = old }()

	handleLogs(&failFlusher{}, httptest.NewRequest(http.MethodGet, "/logs?sel=x|y|z&follow=0", nil))
	if !strings.Contains(buf.String(), "sse") {
		t.Errorf("ожидался лог ошибки записи SSE, got %q", buf.String())
	}
}

// plainWriter — ResponseWriter без Flush (проверка «Streaming unsupported»).
type plainWriter struct {
	hdr  http.Header
	code int
}

func (p *plainWriter) Header() http.Header {
	if p.hdr == nil {
		p.hdr = http.Header{}
	}
	return p.hdr
}
func (p *plainWriter) Write(b []byte) (int, error) { return len(b), nil }
func (p *plainWriter) WriteHeader(code int)        { p.code = code }

// failFlusher — ResponseWriter с Flush, но падающим Write.
type failFlusher struct{ failWriter }

func (f *failFlusher) Flush() {}

func TestCollectEventsErrors(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		switch {
		case strings.Contains(j, "--context bad"):
			return []byte("Forbidden"), fmt.Errorf("exit 1")
		case strings.Contains(j, "--context broken"):
			return []byte("{not json"), nil
		default:
			return []byte(`{"items":[{"metadata":{"namespace":"prod"},"type":"Warning","reason":"BackOff","message":"m","lastTimestamp":"2026-01-01T00:00:00Z","involvedObject":{"kind":"Pod","name":"web"}}]}`), nil
		}
	}

	refs := collectEvents([]string{"bad", "broken", "ok"})
	var rbac, ev bool
	for _, r := range refs {
		if r.Reason == "RBAC/error" && r.Ctx == "bad" {
			rbac = true
		}
		if r.Kind == "event" && r.Ctx == "ok" && r.Type == "Warning" {
			ev = true
		}
	}
	if !rbac {
		t.Errorf("нет RBAC/error ref: %+v", refs)
	}
	if !ev {
		t.Errorf("нет распарсенного события: %+v", refs)
	}
	// "broken" (битый JSON) не добавляет ничего.
	for _, r := range refs {
		if r.Ctx == "broken" {
			t.Errorf("битый JSON не должен давать ref: %+v", r)
		}
	}
}

func TestEventsHandlerEdgeCases(t *testing.T) {
	oldCtx, oldRun := getContextsFn, runKubectlFn
	defer func() { getContextsFn, runKubectlFn = oldCtx, oldRun }()

	// Без Flusher — 500.
	plain := &plainWriter{}
	handleEvents(plain, httptest.NewRequest(http.MethodGet, "/events", nil))
	if plain.code != http.StatusInternalServerError {
		t.Errorf("no-flusher status = %d, want 500", plain.code)
	}

	// Ошибка получения контекстов — JSON с error.
	getContextsFn = func() ([]string, error) { return nil, fmt.Errorf("no kubeconfig") }
	rec := httptest.NewRecorder()
	handleEvents(rec, httptest.NewRequest(http.MethodGet, "/events", nil))
	if !strings.Contains(rec.Body.String(), "no kubeconfig") {
		t.Errorf("body = %q", rec.Body.String())
	}

	// Успешный снапшот с interval; контекст отменён — хендлер завершается.
	getContextsFn = func() ([]string, error) { return []string{"c1"}, nil }
	runKubectlFn = func(args ...string) ([]byte, error) {
		return []byte(`{"items":[{"metadata":{"namespace":"prod"},"type":"Warning","reason":"R","message":"m","involvedObject":{"kind":"Pod","name":"p"}}]}`), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec = httptest.NewRecorder()
	handleEvents(rec, httptest.NewRequest(http.MethodGet, "/events?interval=1", nil).WithContext(ctx))
	if !strings.Contains(rec.Body.String(), `"reason":"R"`) {
		t.Errorf("snapshot body = %q", rec.Body.String())
	}
}

func TestTrimToJSON(t *testing.T) {
	if got := trimToJSON([]byte("no json here")); string(got) != "no json here" {
		t.Errorf("trimToJSON(no brace) = %q", got)
	}
	if got := trimToJSON([]byte("warn: x\n{\"a\":1}")); string(got) != `{"a":1}` {
		t.Errorf("trimToJSON(prefix) = %q", got)
	}
	if got := trimToJSON([]byte(`{"a":1}`)); string(got) != `{"a":1}` {
		t.Errorf("trimToJSON(json) = %q", got)
	}
}

func TestWriteJSONError(t *testing.T) {
	oldOut := log.Writer()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(oldOut)

	rec := httptest.NewRecorder()
	writeJSON(rec, make(chan int)) // канал не сериализуется → ветка ошибки
	if !strings.Contains(buf.String(), "json encode response") {
		t.Errorf("ожидался лог ошибки encode, got %q", buf.String())
	}
}

func TestAuthSetupTTLFallback(t *testing.T) {
	old := getenv
	defer func() {
		getenv = old
		authEnabled = false
		authUser, authPass = "", ""
		authTTL = defaultAuthTTL
		authSecret = nil
	}()
	getenv = func(k string) string {
		switch k {
		case "CB_AUTH_USERNAME":
			return "admin"
		case "CB_AUTH_PASSWORD":
			return "pw"
		case "CB_AUTH_CACHE":
			return "0" // недопустимый TTL → fallback на default
		}
		return ""
	}
	authSetup()
	if authTTL != defaultAuthTTL {
		t.Errorf("authTTL = %v, want default %v", authTTL, defaultAuthTTL)
	}
	if !authEnabled || len(authSecret) == 0 {
		t.Errorf("auth не включён или secret пуст: enabled=%v secret=%d", authEnabled, len(authSecret))
	}
}

func TestHandleIndexAndLoginWriteError(t *testing.T) {
	oldCtx, oldRun := getContextsFn, runKubectlFn
	getContextsFn = func() ([]string, error) { return nil, nil }
	runKubectlFn = func(args ...string) ([]byte, error) { return []byte(`{"items":[]}`), nil }
	defer func() { getContextsFn, runKubectlFn = oldCtx, oldRun }()

	flushDataCache()
	handleIndex(&failWriter{}, httptest.NewRequest(http.MethodGet, "/", nil))
	flushDataCache()
	handleAPI(&failWriter{}, httptest.NewRequest(http.MethodGet, "/api/pods", nil))
	writeLoginPage(&failWriter{}, "err")
}

// failWriter — ResponseWriter, чьи Write всегда падают (проверка ветки логирования).
type failWriter struct{ hdr http.Header }

func (f *failWriter) Header() http.Header {
	if f.hdr == nil {
		f.hdr = http.Header{}
	}
	return f.hdr
}
func (f *failWriter) Write([]byte) (int, error) { return 0, fmt.Errorf("write fail") }
func (f *failWriter) WriteHeader(int)           {}

func TestObjCatAndCatRank(t *testing.T) {
	cases := map[string]string{
		"deployment": "Workload", "statefulset": "Workload", "pod": "Workload",
		"service": "Network", "ingress": "Network", "networkpolicy": "Network",
		"configmap": "Config", "secret": "Secret",
		"persistentvolumeclaim": "Storage", "storageclass": "Storage",
		"role": "RBAC", "clusterrolebinding": "RBAC",
		"namespace": "Cluster", "node": "Cluster",
		// Новые scope-типы не имеют отдельной категории — попадают в Other.
		"horizontalpodautoscaler": "Other", "verticalpodautoscaler": "Other",
		"resourcequota": "Other", "limitrange": "Other", "": "Other",
	}
	for kind, want := range cases {
		if got := objCat(kind); got != want {
			t.Errorf("objCat(%q) = %q, want %q", kind, got, want)
		}
	}
	if got := catRank("Workload"); got != 0 {
		t.Errorf("catRank(Workload) = %d, want 0", got)
	}
	if got := catRank("Other"); got != len(catOrder)-1 {
		t.Errorf("catRank(Other) = %d, want %d", got, len(catOrder)-1)
	}
	if got := catRank("Bogus"); got != len(catOrder) {
		t.Errorf("catRank(Bogus) = %d, want %d", got, len(catOrder))
	}
}

func TestPhaseStateBadges(t *testing.T) {
	phases := map[string]string{
		"Running": "ok", "Succeeded": "ok", "Pending": "warn",
		"Failed": "err", "Unknown": "err", "": "unk", "Weird": "unk",
	}
	for p, want := range phases {
		if got := phaseBadge(p); got != want {
			t.Errorf("phaseBadge(%q) = %q, want %q", p, got, want)
		}
	}
	states := map[string]string{
		"Running": "ok", "Succeeded": "ok", "Terminated:Completed": "ok",
		"Waiting:CrashLoopBackOff": "warn", "Waiting": "warn", "Pending": "warn",
		"Terminated:Error": "err", "Terminated": "err", "": "unk", "Other": "unk",
	}
	for s, want := range states {
		if got := stateBadge(s); got != want {
			t.Errorf("stateBadge(%q) = %q, want %q", s, got, want)
		}
	}
}

func TestContainerStateJSON(t *testing.T) {
	var running csiState
	running.Running = &struct {
		StartedAt string `json:"startedAt"`
	}{}
	if got := containerStateJSON(running); got != "Running" {
		t.Errorf("running = %q, want Running", got)
	}

	var waiting csiState
	waiting.Waiting = &struct {
		Reason string `json:"reason"`
	}{Reason: "CrashLoopBackOff"}
	if got := containerStateJSON(waiting); got != "Waiting:CrashLoopBackOff" {
		t.Errorf("waiting = %q", got)
	}

	var waitingNoReason csiState
	waitingNoReason.Waiting = &struct {
		Reason string `json:"reason"`
	}{}
	if got := containerStateJSON(waitingNoReason); got != "Waiting" {
		t.Errorf("waiting no reason = %q", got)
	}

	var term csiState
	term.Terminated = &struct {
		Reason string `json:"reason"`
	}{Reason: "Completed"}
	if got := containerStateJSON(term); got != "Terminated:Completed" {
		t.Errorf("terminated = %q", got)
	}

	var termNoReason csiState
	termNoReason.Terminated = &struct {
		Reason string `json:"reason"`
	}{}
	if got := containerStateJSON(termNoReason); got != "Terminated" {
		t.Errorf("terminated no reason = %q", got)
	}

	if got := containerStateJSON(csiState{}); got != "Pending" {
		t.Errorf("empty = %q, want Pending", got)
	}
}

func TestShortOutput(t *testing.T) {
	if got := shortOutput(nil); got != "no output" {
		t.Errorf("shortOutput(nil) = %q", got)
	}
	if got := shortOutput([]byte("  hi \n")); got != "hi" {
		t.Errorf("shortOutput(hi) = %q", got)
	}
	long := strings.Repeat("a", 400)
	got := shortOutput([]byte(long))
	if len(got) != 300+len("…") || !strings.HasSuffix(got, "…") {
		t.Errorf("shortOutput(long) len = %d, suffix ok = %v", len(got), strings.HasSuffix(got, "…"))
	}
}

func TestParseRFC3339AndAgeCreated(t *testing.T) {
	if got := parseRFC3339(""); !got.IsZero() {
		t.Errorf("parseRFC3339(empty) = %v, want zero", got)
	}
	if got := parseRFC3339("not-a-time"); !got.IsZero() {
		t.Errorf("parseRFC3339(bad) = %v, want zero", got)
	}
	ts := parseRFC3339("2024-01-05T10:20:30Z")
	if ts.IsZero() || ts.Year() != 2024 {
		t.Errorf("parseRFC3339(valid) = %v", ts)
	}
	if got := ageCreated(""); got != "" {
		t.Errorf("ageCreated(empty) = %q", got)
	}
	if got := ageCreated("not-a-time"); got != "" {
		t.Errorf("ageCreated(bad) = %q", got)
	}
	if got := ageCreated("2024-01-05T10:20:30Z"); got == "" {
		t.Errorf("ageCreated(valid) empty")
	}
}

func TestYamlQuote(t *testing.T) {
	if got := yamlQuote(""); got != `""` {
		t.Errorf("yamlQuote(empty) = %q", got)
	}
	if got := yamlQuote("a b"); got != `"a b"` {
		t.Errorf("yamlQuote(a b) = %q", got)
	}
	if got := yamlQuote(`a"b`); got != `"a\"b"` {
		t.Errorf("yamlQuote(quote) = %q", got)
	}
}

func TestHelmReleaseName(t *testing.T) {
	cases := map[string]string{
		"sh.helm.release.v1.myapp.v3": "myapp",
		"sh.helm.release.v1.myapp":    "myapp",
		"not-a-helm-secret":           "not-a-helm-secret",
		"":                            "",
	}
	for in, want := range cases {
		if got := helmReleaseName(in); got != want {
			t.Errorf("helmReleaseName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNetworksByContextNSAndEventsByContext(t *testing.T) {
	oldRun := runKubectlFn
	defer func() { runKubectlFn = oldRun }()

	runKubectlFn = func(args ...string) ([]byte, error) {
		j := strings.Join(args, " ")
		switch {
		case strings.Contains(j, "api-resources"):
			return []byte(""), nil
		case strings.Contains(j, " get services,ingresses,networkpolicies,endpoints,endpointslices "):
			return []byte(`{"items":[
				{"kind":"ServiceList","items":[{"metadata":{"name":"svc1","namespace":"ns1"}}]},
				{"kind":"IngressList","items":[{"metadata":{"name":"ing1","namespace":"ns2"}}]},
				{"kind":"NetworkPolicyList","items":[{"metadata":{"name":"np1","namespace":"ns1"}}]}
			]}`), nil
		case strings.Contains(j, " get events "):
			return []byte(`{"items":[
				{"metadata":{"namespace":"ns1"},"type":"Warning","reason":"R","message":"m"},
				{"metadata":{"namespace":"ns1"},"type":"Normal","reason":"R","message":"m"},
				{"metadata":{"namespace":"ns2"},"type":"Warning","reason":"R","message":"m"}
			]}`), nil
		}
		return nil, fmt.Errorf("unexpected: %s", j)
	}

	net := networksByContextNS("c1")
	if len(net) != 2 || net["ns1"] != 1 || net["ns2"] != 1 {
		t.Errorf("networksByContextNS = %v, want ns1:1 ns2:1", net)
	}
	if got := eventsByContext("c1"); got != 2 {
		t.Errorf("eventsByContext = %d, want 2 (только Warning)", got)
	}
}

func TestHandleLogout(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/logout", nil)
	handleLogout(rec, req)

	if rec.Code != http.StatusFound {
		t.Errorf("status = %d, want 302", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != authLoginURL {
		t.Errorf("Location = %q, want %q", loc, authLoginURL)
	}
	var found bool
	for _, c := range rec.Result().Cookies() {
		if c.Name == authCookie {
			found = true
			if c.Value != "" || c.MaxAge != -1 {
				t.Errorf("cookie = %+v, want empty value and MaxAge -1", c)
			}
		}
	}
	if !found {
		t.Errorf("cookie %q not cleared", authCookie)
	}
}
