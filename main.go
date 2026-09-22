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
	"regexp"
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
	Contexts   int
	Namespaces int
	Nodes      int
	Pods       int
	Containers int
	Running    int
	NotReady   int
	Restarts   int
	Issues     int
}

type Card struct {
	Label string `json:"label"`
	Value int    `json:"value"`
	Hint  string `json:"hint"`
	Color string `json:"color"`
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
	fetchPodsFn     = fetchPodsReal
	fetchNodesFn    = fetchNodesReal
	openLogStreamFn = openKubectlLogStream
	runKubectlFn    = runKubectl
)

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

func fetchPodsReal() ([]Pod, string) {
	ctxs, err := getContextsFn()
	if err != nil {
		return nil, err.Error()
	}
	if len(ctxs) == 0 {
		return nil, ""
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
			parsed, perr := parsePodList(ctx, out, usage)
			if perr != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("context %q: %v", ctx, perr))
				mu.Unlock()
				return
			}
			mu.Lock()
			pods = append(pods, parsed...)
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
	return pods, strings.Join(errs, "; ")
}

func podsForContextAll(ctx string) ([]byte, error) {
	return runKubectlFn("--context", ctx, "get", "pods", "-A", "-o", "json")
}

func podsForContextNS(ctx, ns string) ([]byte, error) {
	return runKubectlFn("--context", ctx, "get", "pods", "-n", ns, "-o", "json")
}

// countNodesContext считает узлы в одном контексте (0 при ошибке/нет прав).
func countNodesContext(ctx string) int {
	out, err := runKubectlFn("--context", ctx, "get", "nodes", "--no-headers")
	if err != nil {
		return 0
	}
	return len(splitLines(string(out)))
}

// fetchNodesReal — суммарное количество узлов по всем контекстам.
func fetchNodesReal() int {
	ctxs, err := getContextsFn()
	if err != nil {
		return 0
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	total := 0
	for _, ctx := range ctxs {
		wg.Add(1)
		go func(ctx string) {
			defer wg.Done()
			n := countNodesContext(ctx)
			mu.Lock()
			total += n
			mu.Unlock()
		}(ctx)
	}
	wg.Wait()
	return total
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

func parsePodList(ctx string, data []byte, usage map[string][2]string) ([]Pod, error) {
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

// resString форматирует запрошенные ресурсы: "CPU req/lim · Mem req/lim".
func resString(cpuReq, cpuLim, memReq, memLim string) string {
	return fmt.Sprintf("%s/%s · %s/%s", orDash(cpuReq), orDash(cpuLim), orDash(memReq), orDash(memLim))
}

// resLive форматирует ресурсы с живым потреблением, если оно известно
// (metrics-server / kubectl top). Формат: "CPU use/lim (%%) · Mem use/lim (%%)",
// в знаменателе — лимит, а если лимита нет — запрошенные ресурсы.
func resLive(cpuReq, cpuLim, memReq, memLim, cpuUse, memUse string) string {
	cpu := resPart(cpuReq, cpuLim, cpuUse, cpuMilli)
	mem := resPart(memReq, memLim, memUse, memBytes)
	return cpu + " · " + mem
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
		podPhase[key] = p.Phase

		st.Containers++
		st.Restarts += p.RestartCount
		if p.Phase == "Running" {
			st.Running++
		}
		if !p.Ready {
			st.NotReady++
		}
	}

	for _, phase := range podPhase {
		if phase != "Running" && phase != "Succeeded" {
			st.Issues++
		}
	}
	st.Contexts = len(ctxSet)
	st.Namespaces = len(nsSet)
	st.Pods = len(podSet)
	return st
}

func buildCards(st Stats) []Card {
	return []Card{
		{Label: "Contexts", Value: st.Contexts, Hint: "clusters from kubeconfig", Color: "accent"},
		{Label: "Namespaces", Value: st.Namespaces, Hint: "namespaces across contexts", Color: "violet"},
		{Label: "Nodes", Value: st.Nodes, Hint: "nodes across contexts", Color: "blue"},
		{Label: "Pods", Value: st.Pods, Hint: "unique pods", Color: "teal"},
		{Label: "Containers", Value: st.Containers, Hint: "container instances", Color: "teal"},
		{Label: "Running", Value: st.Running, Hint: "containers with phase Running", Color: "ok"},
		{Label: "Restarts", Value: st.Restarts, Hint: "total restart count", Color: "amber"},
		{Label: "Not ready", Value: st.NotReady, Hint: "containers with ready != true", Color: "red"},
		{Label: "Problem pods", Value: st.Issues, Hint: "pods with phase not Running/Succeeded", Color: "redSoft"},
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

type sourceFunc func(sel Selection, since string, follow bool) (io.ReadCloser, error)

func runLogStream(ctx context.Context, sels []Selection, since string, follow bool, src sourceFunc) <-chan LogStreamEvent {
	out := make(chan LogStreamEvent, 128)
	var wg sync.WaitGroup

	for _, sel := range sels {
		wg.Add(1)
		go func(sel Selection) {
			defer wg.Done()
			rc, err := src(sel, since, follow)
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

			sc := bufio.NewScanner(rc)
			sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
			for sc.Scan() {
				line := parseLogLine(sel, sc.Text())
				select {
				case out <- LogStreamEvent{Sel: sel, Line: line}:
				case <-ctx.Done():
					return
				}
			}
			if err := sc.Err(); err != nil && ctx.Err() == nil {
				out <- LogStreamEvent{Sel: sel, Err: fmt.Errorf("read failed: %v", err)}
			}
		}(sel)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

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

func openKubectlLogStream(sel Selection, since string, follow bool) (io.ReadCloser, error) {
	args := []string{"--context", sel.Context, "logs", "-n", sel.Namespace, sel.Pod}
	if sel.Container != "" {
		args = append(args, "-c", sel.Container)
	}
	if since != "" {
		args = append(args, "--since", since)
	}
	if follow {
		args = append(args, "-f")
	}
	args = append(args, "--timestamps=true")
	cmd := exec.Command("kubectl", args...)

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
	pods, errMsg := fetchPodsFn()
	ctxs, err := getContextsFn()
	if errMsg == "" && err != nil {
		errMsg = err.Error()
	}
	st := collectStats(pods)
	st.Contexts = len(ctxs)
	st.Nodes = fetchNodesFn()
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

	fmt.Fprint(w, "retry: 2000\n\n")
	flusher.Flush()

	events := runLogStream(r.Context(), sels, since, follow, openLogStreamFn)
	for ev := range events {
		line := ev.Line
		if ev.Err != nil {
			line = LogEvent{
				Context:   ev.Sel.Context,
				Namespace: ev.Sel.Namespace,
				Pod:       ev.Sel.Pod,
				Container: ev.Sel.Container,
				Error:     ev.Err.Error(),
			}
		}
		b, err := json.Marshal(line)
		if err != nil {
			continue
		}
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}
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

var catOrder = []string{"Workload", "Network", "Config", "Storage", "Access", "Cluster", "Other"}

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
	case "configmap", "secret":
		return "Config"
	case "persistentvolumeclaim", "persistentvolume", "storageclass":
		return "Storage"
	case "serviceaccount", "role", "rolebinding", "clusterrole", "clusterrolebinding":
		return "Access"
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
	out, err := runKubectlFn("--context", ctx, "get", kind, "-n", ns, name, "-o", "yaml")
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

type searchItem struct {
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Cat     string `json:"cat"`
	Snippet string `json:"snippet"`
}

type searchResp struct {
	Query string       `json:"query"`
	Items []searchItem `json:"items"`
}

var (
	itemSplitRE = regexp.MustCompile(`(?m)^\s*- apiVersion:`)
	yamlKindRE  = regexp.MustCompile(`(?m)^\s*kind:\s*(\S+)`)
	yamlNameRE  = regexp.MustCompile(`(?m)^\s*name:\s*(\S+)`)
)

// snippetLine вырезает строку с совпадением и обрезает до max символов.
func snippetLine(doc, needle string, max int) string {
	for _, l := range strings.Split(doc, "\n") {
		if idx := strings.Index(strings.ToLower(l), needle); idx >= 0 {
			l = strings.TrimSpace(l)
			if len(l) <= max {
				return l
			}
			start := idx - 20
			if start < 0 {
				start = 0
			}
			end := start + max
			if end > len(l) {
				end = len(l)
			}
			out := l[start:end]
			if start > 0 {
				out = "…" + out
			}
			if end < len(l) {
				out += "…"
			}
			return out
		}
	}
	return ""
}

// searchManifests ищет строку внутри манифестов всех интересующих типов в
// namespace. Для каждого типа — один вызов kubectl get -o yaml, затем вывод
// разбирается как List и грепается по блокам отдельных объектов.
func searchManifests(ctx, ns, q string) []searchItem {
	ql := strings.ToLower(q)
	var mu sync.Mutex
	var wg sync.WaitGroup
	seen := make(map[string]bool)
	items := make([]searchItem, 0, 8)

	for _, kind := range relatedKinds {
		wg.Add(1)
		go func(kind string) {
			defer wg.Done()
			out, err := runKubectlFn("--context", ctx, "get", kind, "-n", ns, "-o", "yaml")
			if err != nil {
				return
			}
			doc := string(out)
			if !strings.Contains(strings.ToLower(doc), ql) {
				return
			}
			parts := itemSplitRE.Split(doc, -1)
			for _, part := range parts[1:] {
				block := "- apiVersion:" + part
				if !strings.Contains(strings.ToLower(block), ql) {
					continue
				}
				mk := yamlKindRE.FindStringSubmatch(block)
				mn := yamlNameRE.FindStringSubmatch(block)
				if len(mk) < 2 || len(mn) < 2 || mk[1] == "List" || mk[1] == "" {
					continue
				}
				kind := strings.ToLower(mk[1])
				key := kind + "/" + mn[1]
				mu.Lock()
				if !seen[key] {
					seen[key] = true
					items = append(items, searchItem{
						Kind:    kind,
						Name:    mn[1],
						Cat:     objCat(kind),
						Snippet: snippetLine(block, ql, 140),
					})
				}
				mu.Unlock()
			}
		}(kind)
	}
	wg.Wait()

	sort.Slice(items, func(i, j int) bool {
		a, b := catIndexOf(items[i].Cat), catIndexOf(items[j].Cat)
		if a != b {
			return a < b
		}
		return items[i].Kind+"/"+items[i].Name < items[j].Kind+"/"+items[j].Name
	})
	return items
}

func catIndexOf(cat string) int {
	for i, c := range catOrder {
		if c == cat {
			return i
		}
	}
	return len(catOrder)
}

func handleSpecSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	q := r.URL.Query()
	ctx, ns := q.Get("ctx"), q.Get("ns")
	query := strings.TrimSpace(q.Get("q"))
	resp := searchResp{Query: query}
	if len(query) >= 2 {
		resp.Items = searchManifests(ctx, ns, query)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/api/pods", handleAPI)
	mux.HandleFunc("/api/related", handleRelated)
	mux.HandleFunc("/api/object", handleObject)
	mux.HandleFunc("/api/search", handleSpecSearch)
	mux.HandleFunc("/logs", handleLogs)

	log.Printf("KubeLogs: http://localhost%s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
