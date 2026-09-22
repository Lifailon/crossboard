package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	listenAddr = ":8866"
	tmplFile   = "frontend.tmpl"
)

// ---------------------------------------------------------------------------
// Модель данных
// ---------------------------------------------------------------------------

// Pod — одна строка таблицы = одна пара «под + контейнер».
type Pod struct {
	ID           string `json:"id"` // уникальный ключ: context|namespace|pod|container
	Context      string `json:"context"`
	Namespace    string `json:"namespace"`
	Name         string `json:"name"`
	Container    string `json:"container"`
	Node         string `json:"node"`
	IP           string `json:"ip"`
	Phase        string `json:"phase"`
	PhaseCss     string `json:"phaseCss"`
	State        string `json:"state"`
	StateCss     string `json:"stateCss"`
	Ready        bool   `json:"ready"`
	RestartCount int    `json:"restarts"`
	Image        string `json:"image"`
	Labels       string `json:"labels"`
	Qos          string `json:"qos"`
	Res          string `json:"res"`
	Cpu          string `json:"cpu"`
	Mem          string `json:"mem"`
	CpuUse       string `json:"cpuuse"`
	MemUse       string `json:"memuse"`
	OwnerKind    string `json:"ownerkind"`
	Owner        string `json:"owner"`
	Created      string `json:"created"`
	Age          string `json:"age"`
}

type Stats struct {
	Contexts      int
	AvailCtx      int
	Namespaces    int
	Pods          int
	PodsRunning   int
	Containers    int
	Running       int
	Services      int
	Jobs          int // jobs + cronjobs во всех кластерах
	JobsDone      int // успешно завершённые Job (succeeded>0, failed=0)
	Configs       int // configmaps + secrets во всех кластерах
	NodeReady     int
	NodeTotal     int
	CpuMilli      int64
	MemBytes      int64
	CpuTotalMilli int64
	MemTotalBytes int64
	PVCBound      int
	PVCTotal      int
	PVCBoundBytes int64
	PVCTotalBytes int64
	PVCOk         int
}

type Card struct {
	Label string `json:"label"`
	Value string `json:"value"`
	Hint  string `json:"hint"`
	Color string `json:"color"`
	Scope string `json:"scope,omitempty"` // кликебельная карточка: ключ списка в /api/overview (или клиентский режим)
}

// PageData — данные, которые получает шаблон и /api/pods.
// Имена JSON-полей обязаны совпадать с теми, что использует клиентский JS.
type PageData struct {
	Pods       []Pod     `json:"pods"`
	Contexts   []string  `json:"contexts"`
	Namespaces []string  `json:"namespaces"`
	Nodes      []string  `json:"nodes"`
	Phases     []string  `json:"phases"`
	Stats      Stats     `json:"stats"`
	Cards      []Card    `json:"cards"`
	Count      int       `json:"count"`
	Generated  time.Time `json:"generated"`
	Error      string    `json:"error"`
}

// Selection — один источник логов для kubectl logs.
type Selection struct {
	Context   string
	Namespace string
	Pod       string
	Container string
}

// LogEvent — одна строка лога, уходит клиенту по SSE в JSON.
type LogEvent struct {
	Context   string `json:"context"`
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Timestamp string `json:"timestamp,omitempty"`
	Message   string `json:"message,omitempty"`
	Error     string `json:"error,omitempty"`
}

type LogStreamEvent struct {
	Sel  Selection
	Line LogEvent
	Err  error
}

// ---------------------------------------------------------------------------
// Docker-инфраструктура kubectl
// ---------------------------------------------------------------------------

// csiState — состояние контейнера (State внутри containerStatuses).
type csiState struct {
	Running *struct {
		StartedAt string `json:"startedAt"`
	} `json:"running"`
	Waiting *struct {
		Reason string `json:"reason"`
	} `json:"waiting"`
	Terminated *struct {
		Reason string `json:"reason"`
	} `json:"terminated"`
}

// kubeContainerSpec — контейнер из spec.containers / spec.initContainers.
type kubeContainerSpec struct {
	Name      string `json:"name"`
	Image     string `json:"image"`
	Resources struct {
		Requests map[string]string `json:"requests"`
		Limits   map[string]string `json:"limits"`
	} `json:"resources"`
}

// kubectlPodList — подмножество структуры PodList из API Kubernetes, достаточное
// для получения нужных полей.
type kubectlPodList struct {
	Items []struct {
		Metadata struct {
			Name              string            `json:"name"`
			Namespace         string            `json:"namespace"`
			CreationTimestamp time.Time         `json:"creationTimestamp"`
			Labels            map[string]string `json:"labels"`
			OwnerReferences   []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
		Spec struct {
			NodeName       string              `json:"nodeName"`
			Containers     []kubeContainerSpec `json:"containers"`
			InitContainers []kubeContainerSpec `json:"initContainers"`
			Volumes        []struct {
				PersistentVolumeClaim *struct {
					ClaimName string `json:"claimName"`
				} `json:"persistentVolumeClaim"`
			} `json:"volumes"`
		} `json:"spec"`
		Status struct {
			Phase             string `json:"phase"`
			QosClass          string `json:"qosClass"`
			PodIP             string `json:"podIP"`
			ContainerStatuses []struct {
				Name         string   `json:"name"`
				Ready        bool     `json:"ready"`
				RestartCount int      `json:"restartCount"`
				State        csiState `json:"state"`
			} `json:"containerStatuses"`
			InitContainerStatuses []struct {
				Name         string   `json:"name"`
				Ready        bool     `json:"ready"`
				RestartCount int      `json:"restartCount"`
				State        csiState `json:"state"`
			} `json:"initContainerStatuses"`
		} `json:"status"`
	} `json:"items"`
}

// ---------------------------------------------------------------------------
// Переопределяемые в тестах зависимости
// ---------------------------------------------------------------------------

var (
	getContextsFn   = getContextsReal
	fetchAllFn      = fetchAllReal
	openLogStreamFn = openKubectlLogStream
	runKubectlFn    = runKubectl
)

// Overview — результат одного прохода по всем контекстам.
type Overview struct {
	Pods          []Pod
	Error         string
	AvailCtx      int // контексты, где успешно получены поды
	TotalCtx      int
	NodeReady     int
	NodeTotal     int
	CpuMilli      int64 // суммарное использование CPU, м-ядра
	MemBytes      int64 // суммарное использование памяти, байты
	CpuTotalMilli int64 // суммарные allocatable CPU, м-ядра
	MemTotalBytes int64 // суммарные allocatable памяти, байты
	PVCBound      int   // PVC в фазе Bound (привязаны к PV)
	PVCTotal      int   // всего PVC
	PVCBoundBytes int64 // сумма ёмкости, выделенной PV из Bound PVC, байты
	PVCTotalBytes int64 // суммарная запрошенная ёмкость всех PVC, байты
	PVCOk         int   // контексты, где листинг PVC завершился успешно
	Services      int   // суммарное число Service во всех кластерах
	Jobs          int   // суммарное число Jobs + CronJobs
	JobsDone      int   // суммарное число успешно завершённых Job
	Configs       int   // суммарное число ConfigMap + Secret
}

// runKubectl выполняет kubectl с переданными аргументами и возвращает вывод.
func runKubectl(args ...string) ([]byte, error) {
	return exec.Command("kubectl", args...).CombinedOutput()
}

// ---------------------------------------------------------------------------
// Получение списка контекстов и подов
// ---------------------------------------------------------------------------

func getContextsReal() ([]string, error) {
	out, err := runKubectlFn("config", "get-contexts", "-o", "name")
	if err != nil {
		return nil, fmt.Errorf("kubectl config get-contexts: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	ctxs := splitLines(string(out))
	sort.Strings(ctxs)
	return ctxs, nil
}

func splitLines(s string) []string {
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// getDefaultNamespace возвращает ns из context.namespace текущего kubeconfig
// для заданного контекста, а если он не задан — "default".
func getDefaultNamespace(ctx string) string {
	path := fmt.Sprintf("{.contexts[?(@.name==%q)].context.namespace}", ctx)
	out, err := runKubectlFn("config", "view", "-o", "jsonpath="+path)
	if err == nil {
		if ns := strings.TrimSpace(string(out)); ns != "" {
			return ns
		}
	}
	return "default"
}

func fetchAllReal() Overview {
	ctxs, err := getContextsFn()
	if err != nil {
		return Overview{Error: err.Error()}
	}
	ov := Overview{TotalCtx: len(ctxs)}
	if len(ctxs) == 0 {
		return ov
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var pods []Pod
	var errs []string

	for _, ctx := range ctxs {
		wg.Add(1)
		go func(ctx string) {
			defer wg.Done()
			usage := topPodsUsage(ctx)
			var cpuM int64
			var memB int64
			for _, u := range usage {
				if c, ok := cpuMilli(u[0]); ok {
					cpuM += c
				}
				if m, ok := memBytes(u[1]); ok {
					memB += m
				}
			}
			ready, total, cpuT, memT := nodesContext(ctx)

			mounted := make(map[string]bool)
			out, aerr := podsForContextAll(ctx)
			if aerr != nil {
				// Все namespace недоступны — забираем данные из дефолтного
				// namespace контекста, указанного в kubeconfig.
				ns := getDefaultNamespace(ctx)
				nout, nerr := podsForContextNS(ctx, ns)
				if nerr != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("context %q: all-namespaces: %s; fallback namespace %q: %s",
						ctx, shortOutput(out), ns, shortOutput(nout)))
					mu.Unlock()
					return
				}
				out = nout
			}
			parsed, perr := parsePodList(ctx, out, usage, mounted)
			if perr != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("context %q: %v", ctx, perr))
				mu.Unlock()
				return
			}
			pvcBound, pvcTotal, pvcBoundBytes, pvcTotalBytes, pvcOK := pvcByContext(ctx, mounted)
			svcCount := servicesByContext(ctx)
			jobTotal, jobDone := countKubectlKinds(ctx, scopeKinds("jobs"))
			cfgCount, _ := countKubectlKinds(ctx, scopeKinds("configs"))
			mu.Lock()
			pods = append(pods, parsed...)
			ov.CpuMilli += cpuM
			ov.MemBytes += memB
			ov.NodeReady += ready
			ov.NodeTotal += total
			ov.CpuTotalMilli += cpuT
			ov.MemTotalBytes += memT
			ov.PVCBound += pvcBound
			ov.PVCTotal += pvcTotal
			ov.PVCBoundBytes += pvcBoundBytes
			ov.PVCTotalBytes += pvcTotalBytes
			if pvcOK {
				ov.PVCOk++
			}
			ov.Services += svcCount
			ov.Jobs += jobTotal
			ov.JobsDone += jobDone
			ov.Configs += cfgCount
			ov.AvailCtx++
			mu.Unlock()
		}(ctx)
	}
	wg.Wait()

	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Context != pods[j].Context {
			return pods[i].Context < pods[j].Context
		}
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		if pods[i].Name != pods[j].Name {
			return pods[i].Name < pods[j].Name
		}
		return pods[i].Container < pods[j].Container
	})
	ov.Pods = pods
	ov.Error = strings.Join(errs, "; ")
	return ov
}

func podsForContextAll(ctx string) ([]byte, error) {
	return runKubectlFn("--context", ctx, "get", "pods", "-A", "-o", "json")
}

func podsForContextNS(ctx, ns string) ([]byte, error) {
	return runKubectlFn("--context", ctx, "get", "pods", "-n", ns, "-o", "json")
}

// kubectlNodeList — узлы со статусом Ready и allocatable-ресурсами.
type kubectlNodeList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Status struct {
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
			Allocatable map[string]string `json:"allocatable"`
		} `json:"status"`
	} `json:"items"`
}

// nodesContext считает готовые и общее число узлов в контексте, а также
// суммарные allocatable-ресурсы (CPU в м-ядрах, память в байтах).
// Один вызов kubectl (get nodes -o json), нули при ошибке/нет прав.
func nodesContext(ctx string) (ready, total int, cpuTotal, memTotal int64) {
	out, err := runKubectlFn("--context", ctx, "get", "nodes", "-o", "json")
	if err != nil {
		return 0, 0, 0, 0
	}
	var list kubectlNodeList
	if err := json.Unmarshal(out, &list); err != nil {
		return 0, 0, 0, 0
	}
	for _, n := range list.Items {
		total++
		for _, c := range n.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				ready++
			}
		}
		if v, ok := cpuMilli(n.Status.Allocatable["cpu"]); ok {
			cpuTotal += v
		}
		if v, ok := memBytes(n.Status.Allocatable["memory"]); ok {
			memTotal += v
		}
	}
	return ready, total, cpuTotal, memTotal
}

// kubectlPVCList — PVC с запрошенной ёмкостью (spec.resources.requests.storage)
// и фазой привязки (status.phase) с реально выделенной ёмкостью (status.capacity).
type kubectlPVCList struct {
	Items []struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
		Spec struct {
			Resources struct {
				Requests map[string]string `json:"requests"`
			} `json:"resources"`
		} `json:"spec"`
		Status struct {
			Phase    string            `json:"phase"`
			Capacity map[string]string `json:"capacity"`
		} `json:"status"`
	} `json:"items"`
}

// pvcByContext перечисляет PVC в кластере: считает «Bound» (привязаны к PV,
// статус status.phase) и суммирует ёмкости. Для Bound берётся фактически
// выделенная PV ёмкость (status.capacity.storage), для остальных — запрос.
// Один вызов kubectl (get pvc -A -o json), нули при ошибке/нет прав.
// Если листинг недоступен (RBAC) — ok=false, но число смонтированных томов
// из манифестов подов всё равно отдаётся (bound = len(mounted)).
func pvcByContext(ctx string, mounted map[string]bool) (bound, total int, boundBytes, totalBytes int64, ok bool) {
	out, err := runKubectlFn("--context", ctx, "get", "pvc", "-A", "-o", "json")
	if err != nil {
		return len(mounted), 0, 0, 0, false
	}
	var list kubectlPVCList
	if err := json.Unmarshal(out, &list); err != nil {
		return len(mounted), 0, 0, 0, false
	}
	for _, p := range list.Items {
		total++
		req := p.Spec.Resources.Requests["storage"]
		if reqB, ok := memBytes(req); ok {
			totalBytes += reqB
		}
		if p.Status.Phase == "Bound" {
			bound++
			alloc := p.Status.Capacity["storage"]
			if alloc == "" {
				alloc = req
			}
			if capB, ok := memBytes(alloc); ok {
				boundBytes += capB
			}
		}
	}
	return bound, total, boundBytes, totalBytes, true
}

// servicesByContext возвращает число Service во всех namespace кластера.
// Один вызов kubectl (get svc -A -o json), ноль при ошибке/нет прав.
func servicesByContext(ctx string) int {
	out, err := runKubectlFn("--context", ctx, "get", "svc", "-A", "-o", "json")
	if err != nil {
		return 0
	}
	var list struct {
		Items []struct{} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return 0
	}
	return len(list.Items)
}

// kubeKind — тип ресурса для агрегированных списков (карточки Jobs/Configs).
type kubeKind struct {
	kind, plural, cat string
}

// scopeKinds возвращает типы ресурсов, объединяемые в одну карточку-список.
func scopeKinds(scope string) []kubeKind {
	switch scope {
	case "jobs":
		return []kubeKind{
			{"job", "jobs", "Workload"},
			{"cronjob", "cronjobs", "Workload"},
		}
	case "configs":
		return []kubeKind{
			{"configmap", "configmaps", "Config"},
			{"secret", "secrets", "Secret"},
		}
	case "workloads":
		return []kubeKind{
			{"deployment", "deployments", "Workload"},
			{"daemonset", "daemonsets", "Workload"},
			{"statefulset", "statefulsets", "Workload"},
			{"replicaset", "replicasets", "Workload"},
		}
	}
	return nil
}

// kubectlMixedAll — результат `kubectl get a,b,c -A -o json`. Ранние версии
// kubectl возвращают плоский List (объекты с собственным kind), новые — List
// из вложенных под-List'ов, поэтому элементы разбираются как raw.
type kubectlMixedAll struct {
	Items []json.RawMessage `json:"items"`
}

// mixedObject — под-List или плоский объект внутри kubectlMixedAll.
type mixedObject struct {
	Kind     string           `json:"kind"`
	Items    []overviewObject `json:"items"`
	Metadata struct {
		Name              string    `json:"name"`
		Namespace         string    `json:"namespace"`
		CreationTimestamp time.Time `json:"creationTimestamp"`
	} `json:"metadata"`
}

// countKubectlKinds считает суммарное количество объектов всех указанных типов
// во всех namespace одного кластера одним вызовом kubectl. Второе возвращаемое
// значение — число успешно завершённых Job (status.succeeded>0 и failed=0).
// Ноль при ошибке/нет прав.
func countKubectlKinds(ctx string, ks []kubeKind) (int, int) {
	if len(ks) == 0 {
		return 0, 0
	}
	plurals := make([]string, len(ks))
	for i, k := range ks {
		plurals[i] = k.plural
	}
	out, err := runKubectlFn("--context", ctx, "get", strings.Join(plurals, ","), "-A", "-o", "json")
	if err != nil {
		return 0, 0
	}
	var ml kubectlMixedAll
	if json.Unmarshal(out, &ml) != nil {
		return 0, 0
	}
	n, done := 0, 0
	for _, raw := range ml.Items {
		var e struct {
			Kind     string            `json:"kind"`
			Items    []json.RawMessage `json:"items"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				Succeeded int `json:"succeeded"`
				Failed    int `json:"failed"`
			} `json:"status"`
		}
		if json.Unmarshal(raw, &e) != nil {
			continue
		}
		if len(e.Items) > 0 {
			// Вложенный формат: каждый под-List с собственным kind.
			kind := strings.ToLower(strings.TrimSuffix(e.Kind, "List"))
			for _, iraw := range e.Items {
				n++
				if kind != "job" {
					continue
				}
				var it struct {
					Status struct {
						Succeeded int `json:"succeeded"`
						Failed    int `json:"failed"`
					} `json:"status"`
				}
				if json.Unmarshal(iraw, &it) == nil && jobSucceeded(it.Status.Succeeded, it.Status.Failed) {
					done++
				}
			}
			continue
		}
		if e.Metadata.Name == "" {
			continue
		}
		// Плоский формат: объект с собственным kind.
		n++
		if strings.ToLower(e.Kind) == "job" && jobSucceeded(e.Status.Succeeded, e.Status.Failed) {
			done++
		}
	}
	return n, done
}

func jobSucceeded(succeeded, failed int) bool {
	return succeeded > 0 && failed == 0
}

// shortOutput возвращает обрезанный первый байтовый вывод команды kubectl
// для включения в сообщение об ошибке.
func shortOutput(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	if s == "" {
		return "no output"
	}
	return s
}

func parsePodList(ctx string, data []byte, usage map[string][2]string, mounted map[string]bool) ([]Pod, error) {
	var list kubectlPodList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}

	type containerPair struct {
		name, image, cpuReq, cpuLim, memReq, memLim string
	}
	statuses := make(map[string]struct {
		ready        bool
		restartCount int
		state        string
	})

	pods := make([]Pod, 0, 16)
	for _, item := range list.Items {
		ns := item.Metadata.Namespace
		name := item.Metadata.Name
		phase := item.Status.Phase
		created := ""
		if !item.Metadata.CreationTimestamp.IsZero() {
			created = item.Metadata.CreationTimestamp.Format("2006-01-02 15:04")
		}

		for _, cs := range item.Status.ContainerStatuses {
			statuses[cs.Name] = newContainerState(cs.Ready, cs.RestartCount, containerStateJSON(cs.State))
		}
		for _, cs := range item.Status.InitContainerStatuses {
			statuses[cs.Name] = newContainerState(cs.Ready, cs.RestartCount, containerStateJSON(cs.State))
		}

		containers := make([]containerPair, 0, 4)
		collect := func(ccs []kubeContainerSpec) {
			for _, c := range ccs {
				cp := containerPair{
					name:   c.Name,
					image:  c.Image,
					cpuReq: c.Resources.Requests["cpu"],
					cpuLim: c.Resources.Limits["cpu"],
					memReq: c.Resources.Requests["memory"],
					memLim: c.Resources.Limits["memory"],
				}
				containers = append(containers, cp)
			}
		}
		collect(item.Spec.Containers)
		collect(item.Spec.InitContainers)

		if mounted != nil {
			for _, v := range item.Spec.Volumes {
				if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName != "" {
					mounted[ns+"|"+v.PersistentVolumeClaim.ClaimName] = true
				}
			}
		}

		if len(containers) == 0 {
			continue
		}

		phaseCss := phaseBadge(phase)
		labels := joinLabels(item.Metadata.Labels)
		ownerKind, owner := "", ""
		if len(item.Metadata.OwnerReferences) > 0 {
			ownerKind = item.Metadata.OwnerReferences[0].Kind
			owner = item.Metadata.OwnerReferences[0].Name
		}
		for _, c := range containers {
			st := statuses[c.name]
			use := [2]string{}
			if usage != nil {
				if u, ok := usage[ns+"|"+name]; ok {
					use = u
				}
			}
			cpu := resPart(c.cpuReq, c.cpuLim, use[0], cpuMilli)
			mem := resPart(c.memReq, c.memLim, use[1], memBytes)
			pods = append(pods, Pod{
				ID:           fmt.Sprintf("%s|%s|%s|%s", ctx, ns, name, c.name),
				Context:      ctx,
				Namespace:    ns,
				Name:         name,
				Container:    c.name,
				Node:         item.Spec.NodeName,
				IP:           item.Status.PodIP,
				Phase:        phase,
				PhaseCss:     phaseCss,
				State:        st.state,
				StateCss:     stateBadge(st.state),
				Ready:        st.ready,
				RestartCount: st.restartCount,
				Image:        c.image,
				Labels:       labels,
				Qos:          item.Status.QosClass,
				Res:          cpu + " · " + mem,
				Cpu:          cpu,
				Mem:          mem,
				CpuUse:       use[0],
				MemUse:       use[1],
				OwnerKind:    ownerKind,
				Owner:        owner,
				Created:      created,
				Age:          ageString(item.Metadata.CreationTimestamp),
			})
		}
	}
	return pods, nil
}

// joinLabels склеивает labels в отсортированную строку "k1=v1, k2=v2".
func joinLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(labels[k])
	}
	return sb.String()
}

func resPart(req, lim, use string, parseF func(string) (int64, bool)) string {
	bare := fmt.Sprintf("%s/%s", orDash(req), orDash(lim))
	if use == "" {
		return bare
	}
	if _, ok := parseF(use); !ok {
		return bare
	}
	base, label := lim, lim
	if _, ok := parseF(lim); !ok {
		base, label = req, req
	}
	if p, ok := usagePct(use, base, parseF); ok {
		return fmt.Sprintf("%s/%s (%d%%)", use, orDash(label), p)
	}
	return bare
}

func usagePct(use, base string, parseF func(string) (int64, bool)) (int, bool) {
	u, ok := parseF(use)
	if !ok {
		return 0, false
	}
	b, ok := parseF(base)
	if !ok || b <= 0 {
		return 0, false
	}
	return int(math.Round(float64(u) * 100 / float64(b))), true
}

// cpuMilli переводит CPU в милли-ядра: "250m" -> 250, "1" -> 1000.
func cpuMilli(v string) (int64, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	if strings.HasSuffix(v, "m") {
		n, err := strconv.ParseInt(strings.TrimSuffix(v, "m"), 10, 64)
		return n, err == nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		return 0, false
	}
	return int64(f * 1000), true
}

var memPower = []struct {
	suf string
	val int64
}{
	{"", 1},
	{"ki", 1 << 10}, {"mi", 1 << 20}, {"gi", 1 << 30}, {"ti", 1 << 40},
	{"k", 1000}, {"m", 1e6}, {"g", 1e9}, {"t", 1e12},
}

// memBytes переводит строку памяти в байты: "128Mi", "1Gi", "256M".
func memBytes(v string) (int64, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, false
	}
	lower := strings.ToLower(v)
	suffix, mult := "", int64(1)
	for _, p := range memPower {
		if strings.HasSuffix(lower, p.suf) && len(p.suf) > len(suffix) {
			suffix, mult = p.suf, p.val
		}
	}
	f, err := strconv.ParseFloat(v[:len(v)-len(suffix)], 64)
	if err != nil || f < 0 {
		return 0, false
	}
	return int64(f * float64(mult)), true
}

// topPodsUsage вызывает kubectl top pods -A и возвращает
// "namespace|pod" -> (cpu, mem) из metrics-server. При ошибке — nil.
func topPodsUsage(ctx string) map[string][2]string {
	out, err := runKubectlFn("--context", ctx, "top", "pods", "-A", "--no-headers")
	if err != nil {
		return nil
	}
	usage := make(map[string][2]string)
	for _, line := range splitLines(string(out)) {
		f := strings.Fields(line)
		if len(f) < 4 {
			continue
		}
		usage[f[1]+"|"+f[0]] = [2]string{f[2], f[3]}
	}
	return usage
}

func orDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func ageString(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	d := time.Since(ts)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

type containerStateInfo struct {
	ready        bool
	restartCount int
	state        string
}

func newContainerState(ready bool, restarts int, state string) containerStateInfo {
	return containerStateInfo{ready: ready, restartCount: restarts, state: state}
}

func containerStateJSON(s csiState) string {
	switch {
	case s.Running != nil:
		return "Running"
	case s.Waiting != nil:
		if s.Waiting.Reason != "" {
			return "Waiting:" + s.Waiting.Reason
		}
		return "Waiting"
	case s.Terminated != nil:
		if s.Terminated.Reason != "" {
			return "Terminated:" + s.Terminated.Reason
		}
		return "Terminated"
	}
	return "Pending"
}

func phaseBadge(phase string) string {
	switch phase {
	case "Running", "Succeeded":
		return "ok"
	case "Pending":
		return "warn"
	case "Failed", "Unknown":
		return "err"
	}
	return "unk"
}

func stateBadge(state string) string {
	switch {
	case state == "Running", state == "Succeeded":
		return "ok"
	case strings.HasPrefix(state, "Waiting"), state == "Pending":
		return "warn"
	case strings.HasPrefix(state, "Terminated"):
		return "err"
	}
	return "unk"
}

// ---------------------------------------------------------------------------
// Статистика и карточки
// ---------------------------------------------------------------------------

func collectStats(pods []Pod) Stats {
	var st Stats
	ctxSet := make(map[string]struct{})
	nsSet := make(map[string]struct{})
	podSet := make(map[string]struct{})
	podPhase := make(map[string]string)

	for _, p := range pods {
		ctxSet[p.Context] = struct{}{}
		nsSet[p.Namespace] = struct{}{}
		key := p.Context + "|" + p.Namespace + "|" + p.Name
		podSet[key] = struct{}{}
		if _, seen := podPhase[key]; !seen {
			podPhase[key] = p.Phase
		}

		// Завершённые и упавшие поды (Jobs, завершённые задачи) не считаем
		// «живыми» контейнерами — они не работают и не запускаются.
		if p.Phase == "Succeeded" || p.Phase == "Failed" {
			continue
		}
		st.Containers++
		if p.Phase == "Running" {
			st.Running++
		}
	}

	for _, phase := range podPhase {
		if phase == "Running" {
			st.PodsRunning++
		}
	}
	st.Contexts = len(ctxSet)
	st.Namespaces = len(nsSet)
	st.Pods = len(podSet)
	return st
}

func frac(a, b int) string {
	if b <= 0 && a <= 0 {
		return "0/0"
	}
	return strconv.Itoa(a) + "/" + strconv.Itoa(b)
}

func coresFrac(use, total int64) string {
	return fmt.Sprintf("%.2f / %.2f", float64(use)/1000, float64(total)/1000)
}

func gibFrac(use, total int64) string {
	return fmt.Sprintf("%.2f / %.2f", float64(use)/(1<<30), float64(total)/(1<<30))
}

// pvPvcValue формирует значение карточки PV/PVC. Если листинг PVC доступен —
// «Bound / total», иначе только число томов из манифестов подов
// (что реально видно при RBAC-отказе).
func pvPvcValue(st Stats) string {
	if st.PVCOk > 0 {
		return frac(st.PVCBound, st.PVCTotal)
	}
	return strconv.Itoa(st.PVCBound)
}

func buildCards(st Stats) []Card {
	return []Card{
		{Label: "Clusters", Value: frac(st.AvailCtx, st.Contexts), Hint: "available / total clusters", Color: "accent", Scope: "ctx"},
		{Label: "Namespaces", Value: strconv.Itoa(st.Namespaces), Hint: "namespaces across clusters", Color: "violet", Scope: "ns"},
		{Label: "Nodes", Value: frac(st.NodeReady, st.NodeTotal), Hint: "ready / total nodes", Color: "blue", Scope: "nodes"},
		{Label: "Pods", Value: frac(st.PodsRunning, st.Pods), Hint: "running / total pods", Color: "teal", Scope: "pods"},
		{Label: "Containers", Value: frac(st.Running, st.Containers), Hint: "running / active containers (excl. finished jobs)", Color: "ok", Scope: "workloads"},
		{Label: "Jobs", Value: frac(st.JobsDone, st.Jobs), Hint: "succeeded jobs / total (jobs + cronjobs)", Color: "blue", Scope: "jobs"},
		{Label: "Services", Value: strconv.Itoa(st.Services), Hint: "services across clusters", Color: "amber", Scope: "svc"},
		{Label: "Configs", Value: strconv.Itoa(st.Configs), Hint: "configmaps + secrets across clusters", Color: "violet", Scope: "configs"},
		{Label: "CPU", Value: coresFrac(st.CpuMilli, st.CpuTotalMilli), Hint: "cores in use / allocatable", Color: "amber"},
		{Label: "Memory", Value: gibFrac(st.MemBytes, st.MemTotalBytes) + " GiB", Hint: "GiB in use / allocatable", Color: "redSoft"},
		{Label: "PVC size", Value: gibFrac(st.PVCBoundBytes, st.PVCTotalBytes) + " GiB", Hint: "capacity allocated to PV / total PVC requested", Color: "ok", Scope: "pvc"},
		{Label: "PV/PVC", Value: pvPvcValue(st), Hint: "bound PV / total PVC", Color: "teal", Scope: "pvc"},
	}
}

func uniqueNamespaces(pods []Pod) []string {
	set := make(map[string]struct{})
	for _, p := range pods {
		set[p.Namespace] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for ns := range set {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}

func uniqueNodes(pods []Pod) []string {
	set := make(map[string]struct{})
	for _, p := range pods {
		if p.Node != "" {
			set[p.Node] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func uniquePhases(pods []Pod) []string {
	set := make(map[string]struct{})
	for _, p := range pods {
		if p.Phase != "" {
			set[p.Phase] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for ph := range set {
		out = append(out, ph)
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// Выборки (Selection) и разбор строк лога
// ---------------------------------------------------------------------------

func parseSelection(raw string) (Selection, bool) {
	parts := strings.Split(raw, "|")
	if len(parts) < 3 {
		return Selection{}, false
	}
	sel := Selection{Context: parts[0], Namespace: parts[1], Pod: parts[2]}
	if len(parts) > 3 {
		sel.Container = parts[3]
	}
	return sel, true
}

func parseSelections(raws []string) []Selection {
	seen := make(map[Selection]struct{})
	var out []Selection
	for _, r := range raws {
		if s, ok := parseSelection(r); ok {
			if _, dup := seen[s]; !dup {
				seen[s] = struct{}{}
				out = append(out, s)
			}
		}
	}
	return out
}

func parseLogLine(sel Selection, raw string) LogEvent {
	ts, msg := "", raw
	if i := strings.IndexByte(raw, ' '); i > 0 {
		if _, err := time.Parse(time.RFC3339Nano, raw[:i]); err == nil {
			ts, msg = raw[:i], strings.TrimLeft(raw[i+1:], " ")
		}
	}
	return LogEvent{
		Context:   sel.Context,
		Namespace: sel.Namespace,
		Pod:       sel.Pod,
		Container: sel.Container,
		Timestamp: ts,
		Message:   msg,
	}
}

// ---------------------------------------------------------------------------
// Поток логов (SSE). runLogStream мультиплексирует несколько источников.
// ---------------------------------------------------------------------------

type sourceFunc func(sel Selection, since string, sinceTime string, follow bool, lines int) (io.ReadCloser, error)

func runLogStream(ctx context.Context, sels []Selection, since string, follow bool, lines int, src sourceFunc) <-chan LogStreamEvent {
	out := make(chan LogStreamEvent, 128)
	var wg sync.WaitGroup

	for _, sel := range sels {
		wg.Add(1)
		go func(sel Selection) {
			defer wg.Done()
			rc, err := src(sel, since, "", follow, lines)
			if err != nil {
				out <- LogStreamEvent{Sel: sel, Err: fmt.Errorf("failed to open stream: %w", err)}
				return
			}
			defer rc.Close()

			// Кладём поток при отмене контекста (отключился клиент).
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				select {
				case <-ctx.Done():
					_ = rc.Close()
				case <-stop:
				}
			}()

			// Читаем построчно. ReadString сам растягивает буфер, поэтому
			// очень длинная строка (>>>>1MB) не обрывает поток.
			rd := bufio.NewReader(rc)
			for {
				chunk, err := rd.ReadString('\n')
				if chunk != "" {
					chunk = strings.TrimSuffix(chunk, "\n")
					chunk = strings.TrimSuffix(chunk, "\r")
					line := parseLogLine(sel, chunk)
					select {
					case out <- LogStreamEvent{Sel: sel, Line: line}:
					case <-ctx.Done():
						return
					}
				}
				if err != nil {
					if err != io.EOF && ctx.Err() == nil {
						out <- LogStreamEvent{Sel: sel, Err: fmt.Errorf("read failed: %v", err)}
					}
					break
				}
			}
		}(sel)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// restartFollowStream — как runLogStream, но при follow=true сам переоткрывает
// источник, когда kubectl-поток заканчивается (рубят прокси/таймауты ~15-30 c).
// SSE при этом не рвётся: канал закрывается только когда все источники
// окончательно умерли или отменился контекст.
//
// Возобновление идёт по последней прочитанной метке времени (--since-time),
// поэтому нет ни дублей, ни потери строк из «окна разрыва».
func restartFollowStream(ctx context.Context, sels []Selection, since string, follow bool, lines int, src sourceFunc) <-chan LogStreamEvent {
	out := make(chan LogStreamEvent, 128)
	var wg sync.WaitGroup

	emit := func(ev LogStreamEvent) bool {
		select {
		case out <- ev:
			return true
		case <-ctx.Done():
			return false
		}
	}
	// sleep возвращает false, если нужно выходить.
	sleep := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}

	for _, sel := range sels {
		wg.Add(1)
		go func(sel Selection) {
			defer wg.Done()
			lastTS := ""
			errs := 0

			for ctx.Err() == nil {
				sinceTime, useLines := "", lines
				if lastTS != "" {
					sinceTime = lastTS // продолжить без дублей и без дыры
					useLines = 0
				}
				rc, err := src(sel, since, sinceTime, follow, useLines)
				if err != nil {
					errs++
					if errs == 1 {
						emit(LogStreamEvent{Sel: sel, Err: fmt.Errorf("stream failed (will retry): %w", err)})
					}
					if errs >= followMaxErrors {
						emit(LogStreamEvent{Sel: sel, Err: fmt.Errorf("stream failed %d times, giving up", errs)})
						return
					}
					if !sleep(followRetryDelay) {
						return
					}
					continue
				}
				errs = 0

				// Закрыть текущий поток при отмене контекста (клиент ушёл).
				stop := make(chan struct{})
				go func(rc io.ReadCloser) {
					select {
					case <-ctx.Done():
						_ = rc.Close()
					case <-stop:
					}
				}(rc)

				rd := bufio.NewReader(rc)
			readLoop:
				for {
					chunk, err := rd.ReadString('\n')
					if chunk != "" {
						chunk = strings.TrimSuffix(chunk, "\n")
						chunk = strings.TrimSuffix(chunk, "\r")
						line := parseLogLine(sel, chunk)
						if line.Timestamp != "" {
							lastTS = line.Timestamp
						}
						if !emit(LogStreamEvent{Sel: sel, Line: line}) {
							close(stop)
							_ = rc.Close()
							return
						}
					}
					if err != nil {
						if err != io.EOF && ctx.Err() == nil {
							emit(LogStreamEvent{Sel: sel, Err: fmt.Errorf("read failed: %v", err)})
						}
						break readLoop
					}
				}
				close(stop)
				_ = rc.Close()

				if !follow {
					return // одиночный снимок истории
				}
				if !sleep(followRetryDelay) {
					return
				}
			}
		}(sel)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

const (
	followRetryDelay  = 700 * time.Millisecond // пауза перед переоткрытием стрима
	followMaxErrors   = 5                      // сколько сбоев подряд терпим
)

// procReader — объединённый (stdout+stderr) поток из kubectl-процесса,
// который при закрытии убивает процесс.
type procReader struct {
	r    *os.File
	cmd  *exec.Cmd
	once sync.Once
}

func (p *procReader) Read(b []byte) (int, error) { return p.r.Read(b) }

func (p *procReader) Close() error {
	p.once.Do(func() {
		if p.cmd != nil && p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
		if p.r != nil {
			_ = p.r.Close()
		}
	})
	return nil
}

// kubectlLogArgs собирает аргументы kubectl logs.
// Параметры взаимоисключающие: lines>0 → --tail; иначе sinceTime != "" → --since-time;
// иначе since != "" → --since. Вместе не передаём (kubectl их не сочетает).
func kubectlLogArgs(sel Selection, since string, sinceTime string, follow bool, lines int) []string {
	args := []string{"--context", sel.Context, "logs", "-n", sel.Namespace, sel.Pod}
	if sel.Container != "" {
		args = append(args, "-c", sel.Container)
	}
	if lines > 0 {
		args = append(args, "--tail", strconv.Itoa(lines))
	} else if sinceTime != "" {
		args = append(args, "--since-time", sinceTime)
	} else if since != "" {
		args = append(args, "--since", since)
	}
	if follow {
		args = append(args, "-f")
	}
	args = append(args, "--timestamps=true")
	return args
}

func openKubectlLogStream(sel Selection, since string, sinceTime string, follow bool, lines int) (io.ReadCloser, error) {
	cmd := exec.Command("kubectl", kubectlLogArgs(sel, since, sinceTime, follow, lines)...)

	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		pr.Close()
		pw.Close()
		return nil, err
	}
	pw.Close() // дочерний процесс остаётся единственным писателем
	return &procReader{r: pr, cmd: cmd}, nil
}

// ---------------------------------------------------------------------------
// HTTP-хендлеры
// ---------------------------------------------------------------------------

var tmpl = template.Must(template.ParseFiles(tmplFile))

func handleIndex(w http.ResponseWriter, r *http.Request) {
	data := gatherData()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "frontend", data); err != nil {
		log.Printf("template render: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleAPI(w http.ResponseWriter, r *http.Request) {
	data := gatherData()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("json encode: %v", err)
	}
}

func gatherData() PageData {
	ov := fetchAllFn()
	pods, errMsg := ov.Pods, ov.Error
	ctxs, err := getContextsFn()
	if errMsg == "" && err != nil {
		errMsg = err.Error()
	}
	st := collectStats(pods)
	st.Contexts = ov.TotalCtx
	st.AvailCtx = ov.AvailCtx
	st.NodeReady = ov.NodeReady
	st.NodeTotal = ov.NodeTotal
	st.CpuMilli = ov.CpuMilli
	st.MemBytes = ov.MemBytes
	st.CpuTotalMilli = ov.CpuTotalMilli
	st.MemTotalBytes = ov.MemTotalBytes
	st.PVCBound = ov.PVCBound
	st.PVCTotal = ov.PVCTotal
	st.PVCBoundBytes = ov.PVCBoundBytes
	st.PVCTotalBytes = ov.PVCTotalBytes
	st.PVCOk = ov.PVCOk
	st.Services = ov.Services
	st.Jobs = ov.Jobs
	st.JobsDone = ov.JobsDone
	st.Configs = ov.Configs
	return PageData{
		Pods:       pods,
		Contexts:   ctxs,
		Namespaces: uniqueNamespaces(pods),
		Nodes:      uniqueNodes(pods),
		Phases:     uniquePhases(pods),
		Stats:      st,
		Cards:      buildCards(st),
		Count:      len(pods),
		Generated:  time.Now(),
		Error:      errMsg,
	}
}

func handleLogs(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	sels := parseSelections(r.URL.Query()["sel"])
	if len(sels) == 0 {
		fmt.Fprint(w, "event: error\ndata: {\"error\":\"no log sources selected\"}\n\n")
		flusher.Flush()
		return
	}

	since := r.URL.Query().Get("since")
	follow := r.URL.Query().Get("follow") != "0"
	lines := 0
	if v := r.URL.Query().Get("lines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			lines = n
		}
	}

	fmt.Fprint(w, "retry: 2000\n\n")
	flusher.Flush()

	var events <-chan LogStreamEvent
	if follow {
		events = restartFollowStream(r.Context(), sels, since, follow, lines, openLogStreamFn)
	} else {
		events = runLogStream(r.Context(), sels, since, follow, lines, openLogStreamFn)
	}
	// Периодический heartbeat (: ping — комментарий SSE), чтобы прокси
	// не рвали долгую "тихую" сессию follow.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				goto streamDone
			}
			var line LogEvent
			if ev.Err != nil {
				line = LogEvent{
					Context:   ev.Sel.Context,
					Namespace: ev.Sel.Namespace,
					Pod:       ev.Sel.Pod,
					Container: ev.Sel.Container,
					Error:     ev.Err.Error(),
				}
			} else {
				line = ev.Line
			}
			b, err := json.Marshal(line)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}

streamDone:
	fmt.Fprint(w, "event: done\ndata: {}\n\n")
	flusher.Flush()
}

// ---------------------------------------------------------------------------
// Просмотр связанных ресурсов (манифестов) для пода
// ---------------------------------------------------------------------------

// ObjRef — одна строка в списке связанных ресурсов.
type ObjRef struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Cat    string `json:"cat"`
	Reason string `json:"reason"`
	Age    string `json:"age"`
	Ctx    string `json:"ctx,omitempty"`
	Ns     string `json:"ns,omitempty"`
}

// Причины связи ресурса с подом.
const (
	rOwner     = "owner"     // сам под и его цепочка контроллеров (rs -> deployment)
	rSelector  = "selector"  // Service/Ingress, подобранный по лейблам пода
	rUsed      = "used"      // SA / ConfigMap / Secret / PVC, на которые ссылается под
	rNamespace = "namespace" // просто объект в namespace пода
	rScope     = "scope"     // node / namespace
)

// relatedKinds — namespaced-типы, которые перечисляются для пода.
var relatedKinds = []string{
	"deployments", "statefulsets", "daemonsets", "replicasets", "jobs",
	"services", "ingresses",
	"configmaps", "secrets",
	"persistentvolumeclaims",
	"serviceaccounts",
}

// singularKind приводит множественное имя типа к единственному.
var singularKind = map[string]string{
	"deployments":            "deployment",
	"statefulsets":           "statefulset",
	"daemonsets":             "daemonset",
	"replicasets":            "replicaset",
	"jobs":                   "job",
	"services":               "service",
	"ingresses":              "ingress",
	"configmaps":             "configmap",
	"secrets":                "secret",
	"persistentvolumeclaims": "persistentvolumeclaim",
	"serviceaccounts":        "serviceaccount",
}

var catOrder = []string{"Workload", "Network", "Config", "Secret", "Storage", "RBAC", "Cluster", "Other"}

func catRank(cat string) int {
	for i, c := range catOrder {
		if c == cat {
			return i
		}
	}
	return len(catOrder)
}

func objCat(kind string) string {
	switch kind {
	case "deployment", "statefulset", "daemonset", "replicaset", "job", "cronjob", "pod":
		return "Workload"
	case "service", "ingress", "endpoints", "networkpolicy":
		return "Network"
	case "configmap":
		return "Config"
	case "secret":
		return "Secret"
	case "persistentvolumeclaim", "persistentvolume", "storageclass":
		return "Storage"
	case "serviceaccount", "role", "rolebinding", "clusterrole", "clusterrolebinding":
		return "RBAC"
	case "namespace", "node":
		return "Cluster"
	}
	return "Other"
}

// podObject — подмножество объектов Pod для вычисления связей ресурсов.
type podObject struct {
	Metadata struct {
		CreationTimestamp time.Time         `json:"creationTimestamp"`
		Labels            map[string]string `json:"labels"`
		OwnerReferences   []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"ownerReferences"`
	} `json:"metadata"`
	Spec struct {
		ServiceAccountName string             `json:"serviceAccountName"`
		Volumes            []podVolumeSpec    `json:"volumes"`
		Containers         []podContainerSpec `json:"containers"`
		InitContainers     []podContainerSpec `json:"initContainers"`
	} `json:"spec"`
}

type podVolumeSpec struct {
	Name                  string `json:"name"`
	PersistentVolumeClaim *struct {
		ClaimName string `json:"claimName"`
	} `json:"persistentVolumeClaim"`
	ConfigMap *struct {
		Name string `json:"name"`
	} `json:"configMap"`
	Secret *struct {
		SecretName string `json:"secretName"`
	} `json:"secret"`
}

type podContainerSpec struct {
	Env []struct {
		ValueFrom *struct {
			ConfigMapKeyRef *struct {
				Name string `json:"name"`
			} `json:"configMapKeyRef"`
			SecretKeyRef *struct {
				Name string `json:"name"`
			} `json:"secretKeyRef"`
		} `json:"valueFrom"`
	} `json:"env"`
	EnvFrom []struct {
		ConfigMapRef *struct {
			Name string `json:"name"`
		} `json:"configMapRef"`
		SecretRef *struct {
			Name string `json:"name"`
		} `json:"secretRef"`
	} `json:"envFrom"`
}

type svcList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			Selector map[string]string `json:"selector"`
		} `json:"spec"`
	} `json:"items"`
}

type ingressList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			Rules []struct {
				HTTP *struct {
					Paths []struct {
						Backend struct {
							Service *struct {
								Name string `json:"name"`
							} `json:"service"`
						} `json:"backend"`
					} `json:"paths"`
				} `json:"http"`
			} `json:"rules"`
		} `json:"spec"`
	} `json:"items"`
}

type rsList struct {
	Items []struct {
		Metadata struct {
			Name            string `json:"name"`
			OwnerReferences []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
	} `json:"items"`
}

// selectorMatches проверяет, что selector (spec.selector сервиса) целиком
// покрывается лейблами пода.
func selectorMatches(sel, labels map[string]string) bool {
	if len(sel) == 0 {
		return false
	}
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// listWithCreated перечисляет объекты kind в namespace и возвращает
// "имя" -> время создания (RFC3339). Один вызов kubectl, колонок две,
// поэтому дешевле, чем тащить полные манифесты.
func listWithCreated(ctx, ns, kind string) map[string]string {
	out, err := runKubectlFn("--context", ctx, "get", kind, "-n", ns,
		"-o", "custom-columns=NAME:.metadata.name,CREATED:.metadata.creationTimestamp")
	if err != nil {
		return nil // нет прав на чтение типа — просто пропускаем
	}
	m := make(map[string]string)
	lines := strings.Split(string(out), "\n")
	for i, line := range lines {
		f := strings.Fields(line)
		if i == 0 || len(f) < 2 {
			continue // шапка "NAME CREATED" и пустые строки
		}
		m[f[0]] = f[1]
	}
	return m
}

// ageCreated форматирует возраст по времени создания
// (пустая строка, если время недоступно/некорректно).
func ageCreated(created string) string {
	if created == "" {
		return ""
	}
	ts, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return ""
	}
	return ageString(ts)
}

func ageTime(ts time.Time) string {
	if ts.IsZero() {
		return ""
	}
	return ageString(ts)
}

// overviewObject — подмножество перечисляемого объекта (pvc/svc/ns/node/…):
// имя, namespace, время создания, storage-запрос, conditions/phase.
type overviewObject struct {
	Metadata struct {
		Name              string    `json:"name"`
		Namespace         string    `json:"namespace"`
		CreationTimestamp time.Time `json:"creationTimestamp"`
	} `json:"metadata"`
	Spec struct {
		Resources struct {
			Requests map[string]string `json:"requests"`
		} `json:"resources"`
	} `json:"spec"`
	Status struct {
		Phase      string `json:"phase"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

type overviewKindList struct {
	Items []overviewObject `json:"items"`
}

// handleOverview отдаёт список объектов для кликов по карточкам:
//
//	/api/overview?scope=ctx|ns|nodes|svc|pvc[&ctx=контекст]
//
// Формат items совпадает с /api/related (ObjRef + ctx/ns для мульти-кластерных
// списков). Ошибки RBAC тихо пропускаются — отдаётся то, что реально доступно.
func handleOverview(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	q := r.URL.Query()
	scope := q.Get("scope")
	onlyCtx := q.Get("ctx")

	resp := relatedResp{}
	if scope == "" {
		resp.Error = "missing scope"
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	ctxs, err := getContextsFn()
	if err != nil {
		resp.Error = err.Error()
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	if onlyCtx != "" {
		ctxs = []string{onlyCtx}
	}

	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		refs []ObjRef
	)
	add := func(it ObjRef) {
		mu.Lock()
		refs = append(refs, it)
		mu.Unlock()
	}

	switch scope {
	case "ctx":
		for _, c := range ctxs {
			add(ObjRef{Kind: "cluster", Name: c, Cat: "Cluster", Ctx: c})
		}
	case "ns", "nodes", "svc", "pvc":
		var kind, plural, cat string
		switch scope {
		case "ns":
			kind, plural, cat = "namespace", "namespaces", "Cluster"
		case "nodes":
			kind, plural, cat = "node", "nodes", "Cluster"
		case "svc":
			kind, plural, cat = "service", "services", "Network"
		case "pvc":
			kind, plural, cat = "persistentvolumeclaim", "persistentvolumeclaims", "Storage"
		}
		for _, c := range ctxs {
			wg.Add(1)
			go func(c, kind, plural, cat string) {
				defer wg.Done()
				out, e := runKubectlFn("--context", c, "get", plural, "-A", "-o", "json")
				if e != nil {
					return // RBAC/нет прав — пропускаем, показываем доступное
				}
				var list overviewKindList
				if json.Unmarshal(out, &list) != nil {
					return
				}
				for _, it := range list.Items {
					ref := ObjRef{
						Kind: kind, Name: it.Metadata.Name, Cat: cat,
						Ctx: c, Ns: it.Metadata.Namespace,
						Age: ageTime(it.Metadata.CreationTimestamp),
					}
					switch scope {
					case "pvc":
						if capB, ok := memBytes(it.Spec.Resources.Requests["storage"]); ok {
							ref.Reason = fmt.Sprintf("%.2f GiB", float64(capB)/(1<<30))
						}
					case "nodes":
						ref.Ns = ""
						ref.Reason = "not ready"
						for _, cd := range it.Status.Conditions {
							if cd.Type == "Ready" && cd.Status == "True" {
								ref.Reason = "ready"
							}
						}
					case "ns":
						ref.Reason = it.Status.Phase
					}
					add(ref)
				}
			}(c, kind, plural, cat)
		}
	case "jobs", "configs", "workloads":
		kinds := scopeKinds(scope)
		for _, c := range ctxs {
			wg.Add(1)
			go func(c string, kinds []kubeKind) {
				defer wg.Done()
				if len(kinds) == 0 {
					return
				}
				plurals := make([]string, len(kinds))
				for i, k := range kinds {
					plurals[i] = k.plural
				}
				out, e := runKubectlFn("--context", c, "get", strings.Join(plurals, ","), "-A", "-o", "json")
				if e != nil {
					return // RBAC/нет прав — пропускаем, показываем доступное
				}
				var ml kubectlMixedAll
				if json.Unmarshal(out, &ml) != nil {
					return
				}
				for _, raw := range ml.Items {
					var mo mixedObject
					if json.Unmarshal(raw, &mo) != nil {
						continue
					}
					if len(mo.Items) > 0 {
						// kubectl обернул каждый тип в под-List («RoleList» → «role»).
						singular := strings.ToLower(strings.TrimSuffix(mo.Kind, "List"))
						if singular == "" {
							continue
						}
						for _, it := range mo.Items {
							add(ObjRef{
								Kind: singular, Name: it.Metadata.Name, Cat: objCat(singular),
								Ctx: c, Ns: it.Metadata.Namespace,
								Age: ageTime(it.Metadata.CreationTimestamp),
							})
						}
						continue
					}
					// Плоский List: каждый объект несёт собственный kind.
					if mo.Metadata.Name == "" {
						continue
					}
					singular := strings.ToLower(mo.Kind)
					add(ObjRef{
						Kind: singular, Name: mo.Metadata.Name, Cat: objCat(singular),
						Ctx: c, Ns: mo.Metadata.Namespace,
						Age: ageTime(mo.Metadata.CreationTimestamp),
					})
				}
			}(c, kinds)
		}
	default:
		resp.Error = "unknown scope"
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	wg.Wait()

	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Ctx != refs[j].Ctx {
			return refs[i].Ctx < refs[j].Ctx
		}
		if refs[i].Ns != refs[j].Ns {
			return refs[i].Ns < refs[j].Ns
		}
		if refs[i].Name != refs[j].Name {
			return refs[i].Name < refs[j].Name
		}
		return refs[i].Kind < refs[j].Kind
	})
	resp.Items = refs
	_ = json.NewEncoder(w).Encode(resp)
}

type relatedResp struct {
	Pod    string            `json:"pod"`
	Ns     string            `json:"ns"`
	Labels map[string]string `json:"labels"`
	Items  []ObjRef          `json:"items"`
	Error  string            `json:"error,omitempty"`
}

func handleRelated(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	q := r.URL.Query()
	ctx, ns, pod, node := q.Get("ctx"), q.Get("ns"), q.Get("pod"), q.Get("node")

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		err error
	)
	var podD *podObject
	var svcD *svcList
	var ingD *ingressList
	var rsD *rsList

	// Детали пода, нужные для "связки": лейблы, владелец, ссылки на ресурсы.
	wg.Add(1)
	go func() {
		defer wg.Done()
		out, e := runKubectlFn("--context", ctx, "get", "pod", "-n", ns, pod, "-o", "json")
		if e != nil {
			mu.Lock()
			err = e
			mu.Unlock()
			return
		}
		var d podObject
		if e := json.Unmarshal(out, &d); e != nil {
			mu.Lock()
			err = e
			mu.Unlock()
			return
		}
		podD = &d
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if out, e := runKubectlFn("--context", ctx, "get", "services", "-n", ns, "-o", "json"); e == nil {
			var d svcList
			if json.Unmarshal(out, &d) == nil {
				svcD = &d
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if out, e := runKubectlFn("--context", ctx, "get", "ingresses", "-n", ns, "-o", "json"); e == nil {
			var d ingressList
			if json.Unmarshal(out, &d) == nil {
				ingD = &d
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if out, e := runKubectlFn("--context", ctx, "get", "replicasets", "-n", ns, "-o", "json"); e == nil {
			var d rsList
			if json.Unmarshal(out, &d) == nil {
				rsD = &d
			}
		}
	}()

	labels := make(map[string]string)
	ownerSet := make(map[string]bool)
	matchedSvc := make(map[string]bool)

	// Перечисление всех интересуемых типов: имя + время создания.
	refsMu := sync.Mutex{}
	seen := make(map[string]bool)
	refs := make([]ObjRef, 0, 32)
	add := func(kind, name, cat, reason, age string) {
		if kind == "" || name == "" {
			return
		}
		refsMu.Lock()
		defer refsMu.Unlock()
		key := kind + "/" + name
		if !seen[key] {
			seen[key] = true
			refs = append(refs, ObjRef{Kind: kind, Name: name, Cat: cat, Reason: reason, Age: age})
		}
	}

	for _, kind := range relatedKinds {
		wg.Add(1)
		go func(kind string) {
			defer wg.Done()
			items := listWithCreated(ctx, ns, kind)
			if items == nil {
				return // нет прав на чтение типа — просто пропускаем
			}
			for name, created := range items {
				k := singularKind[kind]
				add(k, name, objCat(k), rNamespace, ageCreated(created))
			}
		}(kind)
	}
	wg.Wait()

	if podD != nil {
		labels = podD.Metadata.Labels
		for _, o := range podD.Metadata.OwnerReferences {
			ownerSet[strings.ToLower(o.Kind)+"/"+o.Name] = true
		}
	}

	// Разворачиваем ReplicaSet пода до Deployment, чтобы пометить цепочку владения.
	ownerRS := ""
	if podD != nil {
		for _, o := range podD.Metadata.OwnerReferences {
			if strings.EqualFold(o.Kind, "ReplicaSet") {
				ownerRS = o.Name
			}
		}
	}
	if rsD != nil && ownerRS != "" {
		for _, item := range rsD.Items {
			if item.Metadata.Name != ownerRS {
				continue
			}
			for _, o := range item.Metadata.OwnerReferences {
				if strings.EqualFold(o.Kind, "Deployment") && o.Name != "" {
					ownerSet["deployment/"+o.Name] = true
				}
			}
		}
	}

	// Service, чей селектор покрывается лейблами пода.
	if svcD != nil {
		for _, s := range svcD.Items {
			if selectorMatches(s.Spec.Selector, labels) {
				matchedSvc[s.Metadata.Name] = true
			}
		}
	}
	// Ingress, чей backend указывает на подходящий Service.
	if ingD != nil {
		for _, in := range ingD.Items {
			for _, ru := range in.Spec.Rules {
				if ru.HTTP == nil {
					continue
				}
				for _, p := range ru.HTTP.Paths {
					if p.Backend.Service != nil && matchedSvc[p.Backend.Service.Name] {
						add("ingress", in.Metadata.Name, objCat("ingress"), rSelector, "")
					}
				}
			}
		}
	}

	// Ссылки пода на SA / ConfigMap / Secret / PVC.
	saName, cmSet, secretSet, pvcSet := "", map[string]bool{}, map[string]bool{}, map[string]bool{}
	if podD != nil {
		saName = podD.Spec.ServiceAccountName
		if saName == "" {
			saName = "default"
		}
		for _, v := range podD.Spec.Volumes {
			if v.PersistentVolumeClaim != nil {
				pvcSet[v.PersistentVolumeClaim.ClaimName] = true
			}
			if v.ConfigMap != nil {
				cmSet[v.ConfigMap.Name] = true
			}
			if v.Secret != nil {
				secretSet[v.Secret.SecretName] = true
			}
		}
		collect := func(ccs []podContainerSpec) {
			for _, c := range ccs {
				for _, e := range c.Env {
					if e.ValueFrom != nil && e.ValueFrom.ConfigMapKeyRef != nil {
						cmSet[e.ValueFrom.ConfigMapKeyRef.Name] = true
					}
					if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
						secretSet[e.ValueFrom.SecretKeyRef.Name] = true
					}
				}
				for _, f := range c.EnvFrom {
					if f.ConfigMapRef != nil {
						cmSet[f.ConfigMapRef.Name] = true
					}
					if f.SecretRef != nil {
						secretSet[f.SecretRef.Name] = true
					}
				}
			}
		}
		collect(podD.Spec.Containers)
		collect(podD.Spec.InitContainers)
	}

	// Проставляем причины связки и добавляем базовые объекты.
	final := make([]ObjRef, 0, len(refs)+3)
	var podAge string
	if podD != nil {
		podAge = ageTime(podD.Metadata.CreationTimestamp)
	}
	final = append(final,
		ObjRef{Kind: "pod", Name: pod, Cat: "Workload", Reason: rOwner, Age: podAge},
		ObjRef{Kind: "namespace", Name: ns, Cat: "Cluster", Reason: rScope},
	)
	if node != "" {
		final = append(final, ObjRef{Kind: "node", Name: node, Cat: "Cluster", Reason: rScope})
	}
	for _, it := range refs {
		key := it.Kind + "/" + it.Name
		switch {
		case ownerSet[key]:
			it.Reason = rOwner
		case it.Kind == "service" && matchedSvc[it.Name]:
			it.Reason = rSelector
		case it.Kind == "ingress" && it.Reason == rSelector:
			// уже помечено через backend
		case it.Kind == "serviceaccount" && it.Name == saName:
			it.Reason = rUsed
		case it.Kind == "configmap" && cmSet[it.Name]:
			it.Reason = rUsed
		case it.Kind == "secret" && secretSet[it.Name]:
			it.Reason = rUsed
		case it.Kind == "persistentvolumeclaim" && pvcSet[it.Name]:
			it.Reason = rUsed
		}
		if it.Reason == rNamespace {
			continue // объект связан только тем, что живёт в namespace пода — не нужен
		}
		final = append(final, it)
	}

	sort.Slice(final, func(i, j int) bool {
		ri, rj := final[i], final[j]
		if ri.Cat != rj.Cat {
			return catRank(ri.Cat) < catRank(rj.Cat)
		}
		if ri.Kind != rj.Kind {
			return ri.Kind < rj.Kind
		}
		return ri.Name < rj.Name
	})

	resp := relatedResp{Pod: pod, Ns: ns, Labels: labels, Items: final}
	if err != nil {
		resp.Error = err.Error()
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// kubectlConfig — подмножество kubeconfig для вырезки одной записи контекста.
type kubectlConfig struct {
	Contexts []struct {
		Name    string `json:"name"`
		Context struct {
			Cluster   string `json:"cluster"`
			User      string `json:"user"`
			Namespace string `json:"namespace"`
		} `json:"context"`
	} `json:"contexts"`
	Clusters []struct {
		Name    string `json:"name"`
		Cluster struct {
			Server                   string `json:"server"`
			CertificateAuthorityData string `json:"certificate-authority-data"`
			InsecureSkipTLSVerify    bool   `json:"insecure-skip-tls-verify"`
		} `json:"cluster"`
	} `json:"clusters"`
	Users []struct {
		Name string `json:"name"`
		User struct {
			Token                 string `json:"token"`
			Username              string `json:"username"`
			Password              string `json:"password"`
			ClientCertificateData string `json:"client-certificate-data"`
			ClientKeyData         string `json:"client-key-data"`
		} `json:"user"`
	} `json:"users"`
}

// yamlQuote оборачивает значение в двойные кавычки YAML — безопасно для любых
// строк kubeconfig (base64, URL, пути). Экранирование совместимо с YAML.
func yamlQuote(s string) string {
	if s == "" {
		return `""`
	}
	return strconv.Quote(s)
}

// clusterSnippetYAML вырезает из kubeconfig запись контекста ctx (cluster/user)
// и возвращает её как минимальный манифест Config. Один вызов kubectl config view.
func clusterSnippetYAML(ctx string) (string, error) {
	out, err := runKubectlFn("config", "view", "-o", "json")
	if err != nil {
		return "", fmt.Errorf("kubectl config view: %s", shortOutput(out))
	}
	var cfg kubectlConfig
	if err := json.Unmarshal(out, &cfg); err != nil {
		return "", fmt.Errorf("parse kubeconfig: %v", err)
	}

	var clusterName, userName, ns string
	found := false
	for i := range cfg.Contexts {
		if cfg.Contexts[i].Name == ctx {
			clusterName = cfg.Contexts[i].Context.Cluster
			userName = cfg.Contexts[i].Context.User
			ns = cfg.Contexts[i].Context.Namespace
			found = true
			break
		}
	}
	if !found || clusterName == "" {
		return "", fmt.Errorf("context %q not found in kubeconfig", ctx)
	}

	var b strings.Builder
	b.WriteString("apiVersion: v1\n")
	b.WriteString("kind: Config\n")
	b.WriteString("current-context: " + yamlQuote(ctx) + "\n")
	b.WriteString("contexts:\n")
	b.WriteString("- name: " + yamlQuote(ctx) + "\n")
	b.WriteString("  context:\n")
	b.WriteString("    cluster: " + yamlQuote(clusterName) + "\n")
	b.WriteString("    user: " + yamlQuote(userName) + "\n")
	if ns != "" {
		b.WriteString("    namespace: " + yamlQuote(ns) + "\n")
	}

	for i := range cfg.Clusters {
		if cfg.Clusters[i].Name != clusterName {
			continue
		}
		cl := cfg.Clusters[i].Cluster
		b.WriteString("clusters:\n")
		b.WriteString("- name: " + yamlQuote(clusterName) + "\n")
		b.WriteString("  cluster:\n")
		if cl.Server != "" {
			b.WriteString("    server: " + yamlQuote(cl.Server) + "\n")
		}
		if cl.CertificateAuthorityData != "" {
			b.WriteString("    certificate-authority-data: " + yamlQuote(cl.CertificateAuthorityData) + "\n")
		}
		if cl.InsecureSkipTLSVerify {
			b.WriteString("    insecure-skip-tls-verify: true\n")
		}
	}

	for i := range cfg.Users {
		if cfg.Users[i].Name != userName {
			continue
		}
		u := cfg.Users[i].User
		b.WriteString("users:\n")
		b.WriteString("- name: " + yamlQuote(userName) + "\n")
		b.WriteString("  user:\n")
		if u.Token != "" {
			b.WriteString("    token: " + yamlQuote(u.Token) + "\n")
		}
		if u.Username != "" {
			b.WriteString("    username: " + yamlQuote(u.Username) + "\n")
		}
		if u.Password != "" {
			b.WriteString("    password: " + yamlQuote(u.Password) + "\n")
		}
		if u.ClientCertificateData != "" {
			b.WriteString("    client-certificate-data: " + yamlQuote(u.ClientCertificateData) + "\n")
		}
		if u.ClientKeyData != "" {
			b.WriteString("    client-key-data: " + yamlQuote(u.ClientKeyData) + "\n")
		}
	}
	return b.String(), nil
}

type objectResp struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Yaml  string `json:"yaml"`
	Error string `json:"error,omitempty"`
}

func handleObject(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	q := r.URL.Query()
	ctx, ns, kind, name := q.Get("ctx"), q.Get("ns"), q.Get("kind"), q.Get("name")

	resp := objectResp{Kind: kind, Name: name}
	if name == "" {
		resp.Error = "no object name provided"
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	if kind == "cluster" {
		y, e := clusterSnippetYAML(ctx)
		if e != nil {
			resp.Error = e.Error()
		} else {
			resp.Yaml = y
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	args := []string{"--context", ctx, "get", kind, name, "-o", "yaml"}
	if ns != "" {
		args = append(args, "-n", ns)
	}
	out, err := runKubectlFn(args...)
	if err != nil {
		resp.Error = shortOutput(out)
		if strings.TrimSpace(string(out)) == "" {
			resp.Error = err.Error()
		}
	} else {
		resp.Yaml = string(out)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/api/pods", handleAPI)
	mux.HandleFunc("/api/related", handleRelated)
	mux.HandleFunc("/api/object", handleObject)
	mux.HandleFunc("/api/overview", handleOverview)
	mux.HandleFunc("/logs", handleLogs)

	log.Printf("KubeLogs: http://localhost%s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
