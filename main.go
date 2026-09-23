package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
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
	defaultPort = ":8866"
	// defaultCacheTTL — TTL кеша агрегированных данных по умолчанию (можно
	// переопределить переменной окружения CB_DATA_CACHE, значение в секундах).
	defaultCacheTTL = 10 * time.Second
	tmplFile        = "frontend.tmpl"
	// defaultAuthTTL — время жизни сессии авторизации по умолчанию (CB_AUTH_CACHE).
	defaultAuthTTL = 1 * time.Hour
)

// getenv — переопределяемая в тестах обёртка над os.Getenv.
var getenv = os.Getenv

// envOr возвращает значение переменной окружения key, обрезая пробелы;
// если переменная пустая/не задана — возвращает def.
func envOr(key, def string) string {
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return def
}

// envDurationSeconds возвращает количество секунд из переменной окружения key;
// при пустом значении или ошибке парсинга возвращает def. Значение 0 допускается
// (например, CACHE=0 — кеш агрегированных данных отключён).
func envDurationSeconds(key string, def time.Duration) time.Duration {
	if raw := envOr(key, ""); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}

// Kubectl-вызовы ограничены: maxParallelKubectl одновременных kubectl-процессов
// и maxCtxParallel параллельных проходов по кластерам. Предотвращает всплеск
// нагрузки при большом числе контекстов.
const (
	maxParallelKubectl = 8
	maxCtxParallel     = 8
)

// kubectlTimeout — таймаут на один kubectl-вызов при сборе агрегированных
// данных. Зависший kubectl не должен блокировать снапшот навсегда.
var kubectlTimeout = 30 * time.Second

var (
	// listenAddr — адрес HTTP-сервера, по умолчанию :8866, задаётся через CB_PORT.
	listenAddr = ":" + envOr("CB_PORT", defaultPort[1:])
	// dataTTL — время жизни кеша агрегированных данных. 0 — кеш отключён.
	dataTTL = envDurationSeconds("CB_DATA_CACHE", defaultCacheTTL)
	// authEnabled — авторизация включена только при заданных CB_AUTH_USERNAME
	// и CB_AUTH_PASSWORD.
	authEnabled  bool
	authUser     string
	authPass     string
	authSecret   []byte // ключ подписи токена сессии, генерируется при старте
	authTTL      = envDurationSeconds("CB_AUTH_CACHE", defaultAuthTTL)
	authCookie   = "cb_session"
	authLoginURL = "/login"
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
	CpuLim       string `json:"cpulim"`
	MemLim       string `json:"memlim"`
	OwnerKind    string `json:"ownerkind"`
	Owner        string `json:"owner"`
	Workload     string `json:"workload"` // верхнеуровневый владелец: Deployment/StatefulSet/DaemonSet/Job/CronJob или "" (bare pod)
	Created      string `json:"created"`
	Age          string `json:"age"`
}

type Stats struct {
	Contexts      int    `json:"total"`
	AvailCtx      int    `json:"avail"`
	Namespaces    int    `json:"namespaces"`
	Pods          int    `json:"pods"`
	PodsRunning   int    `json:"podsRunning"`
	Containers    int    `json:"containers"`
	Running       int    `json:"running"`
	Networks      int    `json:"networks"` // network-ресурсы во всех кластерах (Service, Ingress, NetworkPolicy, Endpoints)
	Services      int    `json:"services"` // только Service во всех кластерах (карточка Service)
	Jobs          int    `json:"jobs"`     // jobs + cronjobs во всех кластерах
	JobsDone      int    `json:"jobsDone"` // успешно завершённые Job (succeeded>0, failed=0)
	Configs       int    `json:"configs"`  // configmaps + secrets во всех кластерах
	Charts        int    `json:"charts"`   // helm-релизы во всех кластерах
	NodeReady     int    `json:"nodeReady"`
	NodeTotal     int    `json:"nodeTotal"`
	CpuMilli      int64  `json:"cpuMilli"`
	MemBytes      int64  `json:"memBytes"`
	CpuLimitMilli int64  `json:"cpuLimitMilli"` // суммарные hard-лимиты CPU всех контейнеров, м-ядра
	MemLimitBytes int64  `json:"memLimitBytes"` // суммарные hard-лимиты памяти всех контейнеров, байты
	CpuTotalMilli int64  `json:"cpuTotal"`
	MemTotalBytes int64  `json:"memTotal"`
	PVCBound      int
	PVCTotal      int
	PVCBoundBytes int64
	PVCTotalBytes int64
	PVTotalBytes  int64 // суммарная ёмкость всех PV во всех кластерах, байты
	PVCOk         int
	StorageClasses int // объекты StorageClass во всех кластерах (cluster-scoped)
	EventsWarning int // события type=Warning во всех кластерах
}

// NSStats — те же счётчики ресурсов, но по (ctx, namespace). Ключ — "ctx|ns".
// Нужны клиенту, чтобы сворачивать карточки Jobs/Networks/Configs/PV-PVC/Events
// при клиентском фильтре по ноде/пространству.
type NSStats struct {
	Networks      int
	Services      int
	Jobs          int
	JobsDone      int
	Configs       int
	Charts        int
	EventsWarning int
	PVCBound      int
	PVCTotal      int
PVCBoundBytes int64
	PVCTotalBytes int64
	PVTotalBytes  int64 // суммарная ёмкость всех PV в кластере, байты (кладётся на "ctx|cluster")
	PVCOk         int    // 1, если в этом ctx листинг PVC реально удался
	StorageClasses int // StorageClass (cluster-scoped): кладётся на ключ "ctx|cluster"
	NodeReady     int
	NodeTotal     int
	NodeOk        int // 1, если листинг Node в этом ctx реально удался
}

type Card struct {
	Label string `json:"label"`
	Value string `json:"value"`
	Hint  string `json:"hint"`
	Color string `json:"color"`
	Scope string `json:"scope,omitempty"` // кликебельная карточка: ключ списка в /api/overview (или клиентский режим)
}

// PageData — данные, которые получает шаблон и /api/pods.
// Имена JSON-полей обязаны совпадать с тем, что использует клиентский JS.
type PageData struct {
	Pods       []Pod             `json:"pods"`
	Contexts   []string          `json:"contexts"`
	Namespaces []string          `json:"namespaces"`
	Nodes      []string          `json:"nodes"`
	Phases     []string          `json:"phases"`
	States     []string          `json:"states"`
	Stats      Stats             `json:"stats"`
	NS         map[string]NSStats `json:"ns"`
	NSJSON     template.JS       `json:"-"` // NS, подготовленный для вставки в <script>
	Cards      []Card            `json:"cards"`
	Count      int               `json:"count"`
	Generated  time.Time         `json:"generated"`
	Error      string            `json:"error"`
	// AuthRequired — требуется показать форму логина вместо дашборда.
	AuthRequired bool `json:"-"`
	// AuthError — текст ошибки авторизации (выводится в форме логина).
	AuthError string `json:"-"`
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

// wlObject — объект workload-ресурса (deploy/rs/sts/ds/job/cronjob) с его
// собственным владельцем, чтобы резолвить цепочку ReplicaSet -> Deployment.
type wlObject struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name            string `json:"name"`
		OwnerReferences []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"ownerReferences"`
	} `json:"metadata"`
}

// workloadOwnerMap по одному `kubectl get workloads -A` строит карту
// "name|kind" -> [ownerKind, ownerName] для подов, чьим владельцем является
// ReplicaSet (=> Deployment) или Job (=> CronJob). Разбирает оба формата
// kubectl: плоский List и вложенные под-List'ы.
func workloadOwnerMap(ctx string) map[string][2]string {
	out, err := runKubectlFn("--context", ctx, "get", "deployments,statefulsets,daemonsets,replicasets,jobs,cronjobs", "-A", "-o", "json")
	if err != nil {
		return nil
	}
	var ml kubectlMixedAll
	if json.Unmarshal(trimToJSON(out), &ml) != nil {
		return nil
	}
	m := make(map[string][2]string)
	scan := func(kind string, raws []json.RawMessage) {
		for _, raw := range raws {
			var o wlObject
			if json.Unmarshal(raw, &o) != nil {
				continue
			}
			if o.Kind == "" {
				o.Kind = kind
			}
			if len(o.Metadata.OwnerReferences) == 0 {
				continue
			}
			ref := o.Metadata.OwnerReferences[0]
			m[o.Metadata.Name+"|"+strings.ToLower(o.Kind)] = [2]string{ref.Kind, ref.Name}
		}
	}
	for _, raw := range ml.Items {
		var w struct {
			Kind  string            `json:"kind"`
			Items []json.RawMessage `json:"items"`
		}
		if json.Unmarshal(raw, &w) != nil {
			continue
		}
		if len(w.Items) > 0 {
			scan(strings.ToLower(strings.TrimSuffix(w.Kind, "List")), w.Items)
			continue
		}
		var o wlObject
		if json.Unmarshal(raw, &o) != nil || o.Metadata.Name == "" {
			continue
		}
		if len(o.Metadata.OwnerReferences) == 0 {
			continue
		}
		ref := o.Metadata.OwnerReferences[0]
		m[o.Metadata.Name+"|"+strings.ToLower(o.Kind)] = [2]string{ref.Kind, ref.Name}
	}
	return m
}

// resolveWorkload поднимает владельца пода до верхнего уровня: ReplicaSet ->
// Deployment, Job -> CronJob. Без резолва оставляет исходный ownerKind.
// Резолв применяется только если владелец — известный workload (Deployment,
// CronJob, StatefulSet, DaemonSet). Прочие владельцы (например HelmChart от
// helm-controller) не подменяются, чтобы фильтр Workloads не показывал
// случайные типы.
func resolveWorkload(p *Pod, wl map[string][2]string) string {
	if p.OwnerKind == "" {
		return ""
	}
	if wl != nil {
		if ref, ok := wl[p.Owner+"|"+strings.ToLower(p.OwnerKind)]; ok && isWorkloadKind(ref[0]) {
			p.OwnerKind = ref[0]
			p.Owner = ref[1]
			return ref[0]
		}
	}
	return p.OwnerKind
}

func isWorkloadKind(kind string) bool {
	switch strings.ToLower(kind) {
	case "deployment", "statefulset", "daemonset", "replicaset", "job", "cronjob":
		return true
	}
	return false
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
	PVTotalBytes  int64 // суммарная ёмкость всех PV, байты
	PVCOk         int   // контексты, где листинг PVC завершился успешно
	StorageClasses int  // суммарное число StorageClass по кластерам
	Networks      int   // суммарное число сетевых ресурсов (Service/Ingress/NetworkPolicy/Endpoints/EndpointSlice)
	Services      int   // суммарное число Service
	Jobs          int   // суммарное число Jobs + CronJobs
	JobsDone      int   // суммарное число успешно завершённых Job
	Configs       int   // суммарное число ConfigMap + Secret
	Charts        int   // суммарное число helm-релизов
	EventsWarning int   // события type=Warning во всех кластерах
	NS            map[string]NSStats // разбивка по "ctx|ns" для клиентских фильтров
}

// kubectlSem ограничивает число одновременно работающих kubectl-процессов.
var kubectlSem = make(chan struct{}, maxParallelKubectl)

// runKubectl выполняет kubectl с переданными аргументами и возвращает вывод.
// Вызов ограничен семафором (максимум maxParallelKubectl одновременных) и
// таймаутом kubectlTimeout.
func runKubectl(args ...string) ([]byte, error) {
	kubectlSem <- struct{}{}
	defer func() { <-kubectlSem }()

	ctx, cancel := context.WithTimeout(context.Background(), kubectlTimeout)
	defer cancel()
	return exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
}

// trimToJSON отбрасывает всё до первой '{' — kubectl может печатать предупреждения
// (например, "Warning: v1 Endpoints is deprecated") в stderr, которые CombinedOutput
// склеивает с JSON, ломая парсинг.
func trimToJSON(out []byte) []byte {
	if i := bytes.IndexByte(out, '{'); i >= 0 {
		return out[i:]
	}
	return out
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

	ctxSem := make(chan struct{}, maxCtxParallel)
	for _, ctx := range ctxs {
		wg.Add(1)
		go func(ctx string) {
			defer wg.Done()
			ctxSem <- struct{}{}
			defer func() { <-ctxSem }()
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
			ready, total, cpuT, memT, nodeOk := nodesContext(ctx)
			var wl map[string][2]string
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
			// Резолвим верхнеуровневый Workload (ReplicaSet -> Deployment и
			// Job -> CronJob) одним запросом workloads по контексту.
			wl = workloadOwnerMap(ctx)
			for i := range parsed {
				parsed[i].Workload = resolveWorkload(&parsed[i], wl)
			}
			pvcNS := pvcByContextNS(ctx, mounted)
			scNS := scByContext(ctx)
			pvCap := pvCapacityByContext(ctx)
			netNS, svcNS := networksAndServicesByContextNS(ctx)
			chartNS := chartsByContextNS(ctx)
			// Один вызов kubectl на Jobs+CronJobs+ConfigMaps+Secrets,
			// затем разбивка по категориям внутри.
			byKind := countKindsByKindNS(ctx, append(scopeKinds("jobs"), scopeKinds("configs")...))
			jobNS := mergeKindsNS(filterKinds(byKind, "job", "cronjob"))
			cfgNS := mergeKindsNS(filterKinds(byKind, "configmap", "secret"))
			evNS := eventsByContextNS(ctx)
			nsStats := make(map[string]NSStats)
			addNS := func(ns string, fn func(*NSStats)) {
				if ns == "" {
					ns = "cluster"
				}
				v := nsStats[ns]
				fn(&v)
				nsStats[ns] = v
			}
			addNS("", func(s *NSStats) {
				s.NodeReady += ready
				s.NodeTotal += total
				if nodeOk {
					s.NodeOk = 1
				}
			})
			for ns, v := range pvcNS {
				addNS(ns, func(s *NSStats) {
					s.PVCBound += v.bound
					s.PVCTotal += v.total
					s.PVCBoundBytes += v.boundBytes
					s.PVCTotalBytes += v.totalBytes
					if v.listed {
						s.PVCOk = 1
					}
				})
			}
			if scNS >= 0 {
				addNS("", func(s *NSStats) { s.StorageClasses += scNS })
			}
			if pvCap > 0 {
				addNS("", func(s *NSStats) { s.PVTotalBytes += pvCap })
			}
			for ns, v := range netNS {
				addNS(ns, func(s *NSStats) { s.Networks += v })
			}
			for ns, v := range svcNS {
				addNS(ns, func(s *NSStats) { s.Services += v })
			}
			for ns, v := range chartNS {
				addNS(ns, func(s *NSStats) { s.Charts += v })
			}
			for ns, v := range jobNS {
				addNS(ns, func(s *NSStats) { s.Jobs += v.total; s.JobsDone += v.done })
			}
			for ns, v := range cfgNS {
				addNS(ns, func(s *NSStats) { s.Configs += v.total })
			}
			for ns, v := range evNS {
				addNS(ns, func(s *NSStats) { s.EventsWarning += v })
			}
			mu.Lock()
			pods = append(pods, parsed...)
			ov.CpuMilli += cpuM
			ov.MemBytes += memB
			ov.NodeReady += ready
			ov.NodeTotal += total
			ov.CpuTotalMilli += cpuT
			ov.MemTotalBytes += memT
			for _, v := range pvcNS {
				ov.PVCBound += v.bound
				ov.PVCTotal += v.total
				ov.PVCBoundBytes += v.boundBytes
				ov.PVCTotalBytes += v.totalBytes
				if v.listed {
					ov.PVCOk++
				}
			}
			if scNS >= 0 {
				ov.StorageClasses += scNS
			}
			ov.PVTotalBytes += pvCap
			for _, v := range netNS {
				ov.Networks += v
			}
			for _, v := range svcNS {
				ov.Services += v
			}
			for _, v := range chartNS {
				ov.Charts += v
			}
			for _, v := range jobNS {
				ov.Jobs += v.total
				ov.JobsDone += v.done
			}
			for _, v := range cfgNS {
				ov.Configs += v.total
			}
			for _, v := range evNS {
				ov.EventsWarning += v
			}
			ov.AvailCtx++
			if ov.NS == nil {
				ov.NS = make(map[string]NSStats)
			}
			for ns, v := range nsStats {
				key := ctx + "|" + ns
				cur := ov.NS[key]
				cur.Networks += v.Networks
				cur.Services += v.Services
				cur.Jobs += v.Jobs
				cur.JobsDone += v.JobsDone
				cur.Configs += v.Configs
				cur.Charts += v.Charts
				cur.EventsWarning += v.EventsWarning
				cur.PVCBound += v.PVCBound
				cur.PVCTotal += v.PVCTotal
				cur.PVCBoundBytes += v.PVCBoundBytes
				cur.PVCTotalBytes += v.PVCTotalBytes
				cur.PVTotalBytes += v.PVTotalBytes
				if v.PVCOk != 0 {
					cur.PVCOk = 1
				}
				cur.StorageClasses += v.StorageClasses
				cur.NodeReady += v.NodeReady
				cur.NodeTotal += v.NodeTotal
				if v.NodeOk != 0 {
					cur.NodeOk = 1
				}
				ov.NS[key] = cur
			}
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
func nodesContext(ctx string) (ready, total int, cpuTotal, memTotal int64, ok bool) {
	out, err := runKubectlFn("--context", ctx, "get", "nodes", "-o", "json")
	if err != nil {
		return 0, 0, 0, 0, false
	}
	var list kubectlNodeList
	if err := json.Unmarshal(trimToJSON(out), &list); err != nil {
		return 0, 0, 0, 0, false
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
	return ready, total, cpuTotal, memTotal, true
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

type nsPVC struct {
	bound, total    int
	boundBytes      int64
	totalBytes      int64
	listed          bool
}

// pvcByContextNS перечисляет PVC в кластере по namespace. Для Bound берётся
// фактически выделенная PV ёмкость (status.capacity.storage), для остальных —
// запрос. При RBAC-отказе (listed=false) в карте остаются только смонтированные
// в поды тома (число томов без ёмкостей).
func pvcByContextNS(ctx string, mounted map[string]bool) map[string]nsPVC {
	out, err := runKubectlFn("--context", ctx, "get", "pvc", "-A", "-o", "json")
	if err != nil {
		byNS := make(map[string]nsPVC)
		for k := range mounted {
			ns := "cluster"
			if i := strings.IndexByte(k, '|'); i > 0 {
				ns = k[:i]
			}
			v := byNS[ns]
			v.bound++
			byNS[ns] = v
		}
		return byNS
	}
	var list kubectlPVCList
	if err := json.Unmarshal(out, &list); err != nil {
		return nil
	}
	byNS := make(map[string]nsPVC)
	for _, p := range list.Items {
		ns := p.Metadata.Namespace
		if ns == "" {
			ns = "cluster"
		}
		v := byNS[ns]
		v.total++
		v.listed = true
		req := p.Spec.Resources.Requests["storage"]
		if reqB, ok := memBytes(req); ok {
			v.totalBytes += reqB
		}
		if p.Status.Phase == "Bound" {
			v.bound++
			alloc := p.Status.Capacity["storage"]
			if alloc == "" {
				alloc = req
			}
			if capB, ok := memBytes(alloc); ok {
				v.boundBytes += capB
			}
		}
		byNS[ns] = v
	}
	return byNS
}

// pvcByContext — агрегатная версия pvcByContextNS (сумма по всем namespace).
func pvcByContext(ctx string, mounted map[string]bool) (bound, total int, boundBytes, totalBytes int64, ok bool) {
	byNS := pvcByContextNS(ctx, mounted)
	listed := false
	for _, v := range byNS {
		bound += v.bound
		total += v.total
		boundBytes += v.boundBytes
		totalBytes += v.totalBytes
		if v.listed {
			listed = true
		}
	}
	return bound, total, boundBytes, totalBytes, listed
}

// kubectlPVList — PV с дисковым volume (spec.capacity.storage) и выделенной
// ёмкостью (status.capacity.storage).
type kubectlPVList struct {
	Items []struct {
		Spec struct {
			Capacity map[string]string `json:"capacity"`
		} `json:"spec"`
		Status struct {
			Capacity map[string]string `json:"capacity"`
		} `json:"status"`
	} `json:"items"`
}

// pvCapacityByContext возвращает суммарную ёмкость всех PV в кластере в байтах.
// PV — cluster-scoped ресурс, поэтому значение одно на весь контекст.
// 0 при ошибке / нет прав.
func pvCapacityByContext(ctx string) int64 {
	out, err := runKubectlFn("--context", ctx, "get", "pv", "-o", "json")
	if err != nil {
		return 0
	}
	var list kubectlPVList
	if err := json.Unmarshal(trimToJSON(out), &list); err != nil {
		return 0
	}
	var sum int64
	for _, p := range list.Items {
		capStr := p.Status.Capacity["storage"]
		if capStr == "" {
			capStr = p.Spec.Capacity["storage"]
		}
		if b, ok := memBytes(capStr); ok {
			sum += b
		}
	}
	return sum
}

// scByContext возвращает количество StorageClass в кластере. StorageClass —
// cluster-scoped ресурс, поэтому число одно на весь контекст. -1 при ошибке/нет прав.
func scByContext(ctx string) int {
	out, err := runKubectlFn("--context", ctx, "get", "storageclass", "-o", "json")
	if err != nil {
		return -1
	}
	var list struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return -1
	}
	return len(list.Items)
}

// servicesByContextNS возвращает число Service по namespace кластера.
// Ключ — namespace. Пустая карта при ошибке/нет прав.
func servicesByContextNS(ctx string) map[string]int {
	out, err := runKubectlFn("--context", ctx, "get", "svc", "-A", "-o", "json")
	if err != nil {
		return nil
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(trimToJSON(out), &list); err != nil {
		return nil
	}
	byNS := make(map[string]int)
	for _, it := range list.Items {
		ns := it.Metadata.Namespace
		if ns == "" {
			ns = "cluster"
		}
		byNS[ns]++
	}
	return byNS
}

// servicesByContext возвращает суммарное число Service во всех namespace.
func servicesByContext(ctx string) int {
	n := 0
	for _, v := range servicesByContextNS(ctx) {
		n += v
	}
	return n
}

// networksAndServicesByContextNS возвращает число сетевых ресурсов и отдельно
// Service по namespace кластера одним вызовом kubectl. Service включаются в
// Networks карточки вместе с Ingress/NetworkPolicy/Endpoints/EndpointSlice, но
// карточка Service требует отдельного числа. Ключи — namespace; Cluster-scoped
// ресурсы (egress-политики) попадают под "cluster".
func networksAndServicesByContextNS(ctx string) (netByNS map[string]int, svcByNS map[string]int) {
	kinds := scopeKinds("svc")
	kinds = append(kinds, egressKinds(ctx)...)
	byKind := countKindsByKindNS(ctx, kinds)
	netByNS = make(map[string]int)
	svcByNS = make(map[string]int)
	for kind, byNS := range byKind {
		dst := netByNS
		if kind == "service" {
			dst = svcByNS
		}
		for ns, v := range byNS {
			if ns == "" {
				ns = "cluster"
			}
			dst[ns] += v.total
		}
	}
	return netByNS, svcByNS
}

// networksByContextNS возвращает число сетевых ресурсов кластера по namespace:
// Service, Ingress, NetworkPolicy, Endpoints, EndpointSlice (и egress-политики,
// если они установлены отдельными CRD).
func networksByContextNS(ctx string) map[string]int {
	netByNS, _ := networksAndServicesByContextNS(ctx)
	return netByNS
}

// chartsByContextNS возвращает число helm-релизов кластера по namespace.
// Helm v3 хранит релизы как Secret с типом helm.sh/release.v1 (label owner=helm).
func chartsByContextNS(ctx string) map[string]int {
	out, err := runKubectlFn("--context", ctx, "get", "secrets", "-A", "-l", "owner=helm", "-o", "json")
	if err != nil {
		return nil
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(trimToJSON(out), &list); err != nil {
		return nil
	}
	byNS := make(map[string]int)
	for _, it := range list.Items {
		ns := it.Metadata.Namespace
		if ns == "" {
			ns = "cluster"
		}
		byNS[ns]++
	}
	return byNS
}

// kubectlEvent — событие кластера (get events -A -o json).
type kubectlEvent struct {
	Metadata struct {
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Type           string `json:"type"`
	Reason         string `json:"reason"`
	Message        string `json:"message"`
	Count          int    `json:"count"`
	LastTimestamp  string `json:"lastTimestamp"`
	InvolvedObject struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"involvedObject"`
}

// eventsByContextNS возвращает число событий с type=Warning по namespace
// кластера. Пустая карта при ошибке/нет прав.
func eventsByContextNS(ctx string) map[string]int {
	out, err := runKubectlFn("--context", ctx, "get", "events", "-A", "-o", "json")
	if err != nil {
		return nil
	}
	var list struct {
		Items []kubectlEvent `json:"items"`
	}
	if err := json.Unmarshal(trimToJSON(out), &list); err != nil {
		return nil
	}
	byNS := make(map[string]int)
	for _, ev := range list.Items {
		if ev.Type != "Warning" {
			continue
		}
		ns := ev.Metadata.Namespace
		if ns == "" {
			ns = "cluster"
		}
		byNS[ns]++
	}
	return byNS
}

// eventsByContext возвращает суммарное число событий с type=Warning.
func eventsByContext(ctx string) int {
	n := 0
	for _, v := range eventsByContextNS(ctx) {
		n += v
	}
	return n
}

// helmReleaseName извлекает имя релиза из имени helm-Secret вида
// sh.helm.release.v1.<release>.<revision>. Если формат не соответствует —
// возвращается исходное имя без изменений.
func helmReleaseName(secretName string) string {
	const prefix = "sh.helm.release.v1."
	if !strings.HasPrefix(secretName, prefix) {
		return secretName
	}
	rest := strings.TrimPrefix(secretName, prefix)
	if i := strings.LastIndex(rest, ".v"); i >= 0 {
		rest = rest[:i]
	}
	return rest
}

// releaseSecretForName возвращает действительное имя helm-Secret (поищем
// префикс sh.helm.release.v1.<release>.v<rev> и выберем самую свежую ревизию).
func releaseSecretForName(ctx, ns, release string) (string, error) {
	prefix := "sh.helm.release.v1." + release + ".v"
	out, err := runKubectlFn("--context", ctx, "get", "secrets", "-n", ns, "-l", "owner=helm", "-o", "json")
	if err != nil {
		return "", err
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(trimToJSON(out), &list); err != nil {
		return "", err
	}
	best, bestRev := "", -1
	for _, it := range list.Items {
		if !strings.HasPrefix(it.Metadata.Name, prefix) {
			continue
		}
		revStr := strings.TrimPrefix(it.Metadata.Name, prefix)
		rev, _ := strconv.Atoi(revStr)
		if rev > bestRev {
			best, bestRev = it.Metadata.Name, rev
		}
	}
	if best == "" {
		return "", fmt.Errorf("helm release %q not found in namespace %s", release, ns)
	}
	return best, nil
}

// helmReleaseYAML декодирует содержимое helm-Secret (data.release — base64 +
// gzip + JSON) в читаемый человеко-понятный YAML, чтобы модалка ресурса
// показывала полезную информацию вместо ошибки "no resource type chart".
func helmReleaseYAML(ctx, ns, release string) (string, error) {
	secret, err := releaseSecretForName(ctx, ns, release)
	if err != nil {
		return "", err
	}
	out, err := runKubectlFn("--context", ctx, "get", "secret", secret, "-n", ns, "-o", "json")
	if err != nil {
		return "", err
	}
	var sec struct {
		Type string `json:"type"`
		Data struct {
			Release string `json:"release"`
		} `json:"data"`
	}
	if err := json.Unmarshal(trimToJSON(out), &sec); err != nil {
		return "", err
	}
	if sec.Data.Release == "" {
		return "", fmt.Errorf("secret %s has no release data", secret)
	}
	raw, err := base64.StdEncoding.DecodeString(sec.Data.Release)
	if err != nil {
		return "", err
	}
	// helm v3 хранит релиз как base64(gzip(json)). kubectl в JSON секрета
	// добавляет ещё один слой base64: разворачиваем оба.
	if lvl := strings.TrimSpace(string(raw)); strings.HasPrefix(lvl, "H4sI") {
		if dec, e := base64.StdEncoding.DecodeString(lvl); e == nil {
			raw = dec
		}
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	defer gz.Close()
	dec, err := io.ReadAll(gz)
	if err != nil {
		return "", err
	}
	var rel map[string]interface{}
	if err := json.Unmarshal(dec, &rel); err != nil {
		return "", err
	}
	lines := []string{
		"# crossboard: helm release decoded from secret " + secret,
	}
	str := func(m map[string]interface{}, ks ...string) string {
		for _, k := range ks {
			if cur, ok := m[k].(map[string]interface{}); ok {
				m = cur
				continue
			}
			if v, ok := m[k].(string); ok {
				return v
			}
			if v, ok := m[k].(float64); ok {
				return strconv.FormatFloat(v, 'f', -1, 64)
			}
			return fmt.Sprintf("%v", m[k])
		}
		return ""
	}
	chart := map[string]interface{}{}
	if cm, ok := rel["chart"].(map[string]interface{}); ok {
		if mm, ok := cm["metadata"].(map[string]interface{}); ok {
			chart = mm
		}
	}
	info := map[string]interface{}{}
	if im, ok := rel["info"].(map[string]interface{}); ok {
		info = im
	}
	lines = append(lines,
		"name: "+str(rel, "name"),
		"namespace: "+str(rel, "namespace"),
		"status: "+str(info, "status"),
		"description: "+str(info, "description"),
		"revision: "+str(rel, "version"),
		"chart: "+str(chart, "name"),
		"chartVersion: "+str(chart, "version"),
		"appVersion: "+str(chart, "appVersion"),
		"values:",
	)
	if cfg, ok := rel["config"].(map[string]interface{}); ok && len(cfg) > 0 {
		var b strings.Builder
		enc := json.NewEncoder(&b)
		enc.SetIndent("", "  ")
		_ = enc.Encode(cfg)
		for _, ln := range strings.Split(strings.TrimRight(b.String(), "\n"), "\n") {
			lines = append(lines, "  "+ln)
		}
	} else {
		lines = append(lines, "  {}")
	}
	return strings.Join(lines, "\n"), nil
}

// kubeKind — тип ресурса для агрегированных списков (карточки Jobs/Configs).
type kubeKind struct {
	kind, plural, cat string
}

// configsScopeKinds — конфиги + RBAC для модалки «Configs and RBAC».
// Card-счётчик (Configs) остаётся только configmap+secret (см. scopeKinds).
func configsScopeKinds() []kubeKind {
	return append(scopeKinds("configs"), []kubeKind{
		{"serviceaccount", "serviceaccounts", "RBAC"},
		{"role", "roles", "RBAC"},
		{"rolebinding", "rolebindings", "RBAC"},
		{"clusterrole", "clusterroles", "RBAC"},
		{"clusterrolebinding", "clusterrolebindings", "RBAC"},
	}...)
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
	case "svc":
		return []kubeKind{
			{"service", "services", "Network"},
			{"ingress", "ingresses", "Network"},
			{"networkpolicy", "networkpolicies", "Network"},
			{"endpoints", "endpoints", "Network"},
			{"endpointslice", "endpointslices", "Network"},
		}
	case "pvc":
		return []kubeKind{
			{"persistentvolumeclaim", "persistentvolumeclaims", "Storage"},
			{"persistentvolume", "persistentvolumes", "Storage"},
			{"storageclass", "storageclasses", "Storage"},
		}
	}
	return nil
}

// egressKinds находит egress-политики (EgressFirewall, EgressNetworkPolicy и
// т.п.): любой ресурс, в названии которого есть "egress". Если CRD нет —
// возвращается nil и список Network не меняется.
func egressKinds(ctx string) []kubeKind {
	out, err := runKubectlFn("--context", ctx, "api-resources", "-o", "name")
	if err != nil {
		return nil
	}
	var kinds []kubeKind
	for _, line := range splitLines(string(out)) {
		name := strings.TrimSpace(line)
		if name == "" || !strings.Contains(strings.ToLower(name), "egress") {
			continue
		}
		kinds = append(kinds, kubeKind{kind: name, plural: name, cat: "Network"})
	}
	return kinds
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
	Spec struct {
		Resources struct {
			Requests map[string]string `json:"requests"`
		} `json:"resources"`
	} `json:"spec"`
}

// nsKinds — количество объектов и успешно завершённых Job в одном namespace.
type nsKinds struct {
	total, done int
}

// countKindsByKindNS считает количество объектов каждого типа по namespace
// кластера одним вызовом kubectl. Ключ первого уровня — kind (нижний регистр,
// без суффикса "list"), второго — namespace. done — число успешно завершённых
// Job. Пустая карта при ошибке/нет прав.
func countKindsByKindNS(ctx string, ks []kubeKind) map[string]map[string]nsKinds {
	if len(ks) == 0 {
		return nil
	}
	plurals := make([]string, len(ks))
	for i, k := range ks {
		plurals[i] = k.plural
	}
	out, err := runKubectlFn("--context", ctx, "get", strings.Join(plurals, ","), "-A", "-o", "json")
	if err != nil {
		return nil
	}
	var ml kubectlMixedAll
	if json.Unmarshal(trimToJSON(out), &ml) != nil {
		return nil
	}
	byKind := make(map[string]map[string]nsKinds)
	acc := func(ns, rawKind string, succeeded, failed int) {
		kind := strings.ToLower(rawKind)
		m := byKind[kind]
		if m == nil {
			m = make(map[string]nsKinds)
			byKind[kind] = m
		}
		k := m[ns]
		k.total++
		if kind == "job" && jobSucceeded(succeeded, failed) {
			k.done++
		}
		m[ns] = k
	}
	parseObj := func(raw json.RawMessage) {
		var e struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Status struct {
				Succeeded int `json:"succeeded"`
				Failed    int `json:"failed"`
			} `json:"status"`
		}
		if json.Unmarshal(raw, &e) != nil {
			return
		}
		if e.Metadata.Name == "" {
			return
		}
		acc(e.Metadata.Namespace, e.Kind, e.Status.Succeeded, e.Status.Failed)
	}
	for _, raw := range ml.Items {
		var sub struct {
			Kind  string             `json:"kind"`
			Items []json.RawMessage  `json:"items"`
		}
		if json.Unmarshal(raw, &sub) != nil {
			continue
		}
		if len(sub.Items) > 0 {
			// Вложенный формат: под-List с собственным kind.
			kind := strings.ToLower(strings.TrimSuffix(sub.Kind, "List"))
			for _, iraw := range sub.Items {
				var it struct {
					Metadata struct {
						Namespace string `json:"namespace"`
					} `json:"metadata"`
					Status struct {
						Succeeded int `json:"succeeded"`
						Failed    int `json:"failed"`
					} `json:"status"`
				}
				ns := ""
				if json.Unmarshal(iraw, &it) == nil {
					ns = it.Metadata.Namespace
				}
				m := byKind[kind]
				if m == nil {
					m = make(map[string]nsKinds)
					byKind[kind] = m
				}
				k := m[ns]
				k.total++
				if kind == "job" && jobSucceeded(it.Status.Succeeded, it.Status.Failed) {
					k.done++
				}
				m[ns] = k
			}
			continue
		}
		if sub.Kind == "" {
			continue
		}
		// Плоский объект с собственным kind.
		parseObj(raw)
	}
	return byKind
}

// mergeKindsNS сливает несколько карт kind→ns→counts в одну ns→counts,
// суммируя total и done по всем kinds.
func mergeKindsNS(maps map[string]map[string]nsKinds) map[string]nsKinds {
	byNS := make(map[string]nsKinds)
	for _, m := range maps {
		for ns, v := range m {
			k := byNS[ns]
			k.total += v.total
			k.done += v.done
			byNS[ns] = k
		}
	}
	return byNS
}

// filterKinds оставляет в карте kind→ns→counts только указанные kinds.
func filterKinds(byKind map[string]map[string]nsKinds, kinds ...string) map[string]map[string]nsKinds {
	out := make(map[string]map[string]nsKinds, len(kinds))
	for _, k := range kinds {
		if m, ok := byKind[k]; ok {
			out[k] = m
		}
	}
	return out
}

// countKubectlKindsNS считает количество объектов всех указанных типов по
// namespace одного кластера одним вызовом kubectl. Ключ — namespace ("cluster"
// для Cluster-scoped ресурсов). done — число успешно завершённых Job.
// Пустая карта при ошибке/нет прав.
func countKubectlKindsNS(ctx string, ks []kubeKind) map[string]nsKinds {
	return mergeKindsNS(countKindsByKindNS(ctx, ks))
}

// countKubectlKinds — суммарное количество всех указанных типов во всех
// namespace кластера (агрегат по countKubectlKindsNS).
func countKubectlKinds(ctx string, ks []kubeKind) (int, int) {
	n, done := 0, 0
	for _, v := range countKubectlKindsNS(ctx, ks) {
		n += v.total
		done += v.done
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
				CpuLim:       c.cpuLim,
				MemLim:       c.memLim,
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
	// Нет метрик использования — показываем только hard limit ("-/500m"),
	// чтобы не вводить в заблуждение, будто request — это нагрузка.
	if use == "" {
		return fmt.Sprintf("-/%s", orDash(lim))
	}
	if _, ok := parseF(use); !ok {
		return fmt.Sprintf("-/%s", orDash(lim))
	}
	bare := fmt.Sprintf("%s/%s", orDash(req), orDash(lim))
	base, label := lim, lim
	if _, ok := parseF(lim); !ok {
		base, label = req, req
	}
	if p, ok := usagePct(use, base, parseF); ok {
		return fmt.Sprintf("%d%% (%s/%s)", p, use, orDash(label))
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
		usage[f[0]+"|"+f[1]] = [2]string{f[2], f[3]}
	}
	return usage
}

func orDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

// parseRFC3339 разбирает timestamp события ("2026-01-01T12:00:00Z").
func parseRFC3339(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return ts
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
	case state == "Running", state == "Succeeded", state == "Terminated:Completed":
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
		if c, ok := cpuMilli(p.CpuLim); ok {
			st.CpuLimitMilli += c
		}
		if m, ok := memBytes(p.MemLim); ok {
			st.MemLimitBytes += m
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
	return fmt.Sprintf("%.2f/%.2f", float64(use)/1000, float64(total)/1000)
}

func gibFrac(use, total int64) string {
	return fmt.Sprintf("%.2f/%.2f", float64(use)/(1<<30), float64(total)/(1<<30))
}

// pvPvcValue формирует значение карточки PVC/PV/SC. Если листинг PVC доступен —
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
		{Label: "Clusters", Value: frac(st.AvailCtx, st.Contexts), Hint: "available / total clusters", Color: "violet", Scope: "ctx"},
		{Label: "Namespaces", Value: strconv.Itoa(st.Namespaces), Hint: "namespaces across clusters", Color: "violet", Scope: "ns"},
		{Label: "Nodes", Value: frac(st.NodeReady, st.NodeTotal), Hint: "ready / total nodes", Color: "violet", Scope: "nodes"},
		{Label: "Pods", Value: frac(st.PodsRunning, st.Pods), Hint: "running / total pods", Color: "ok", Scope: "pods"},
		{Label: "Containers", Value: frac(st.Running, st.Containers), Hint: "running / active containers (excl. finished jobs)", Color: "ok", Scope: "workloads"},
		{Label: "Jobs", Value: frac(st.JobsDone, st.Jobs), Hint: "succeeded jobs / total (jobs + cronjobs)", Color: "ok", Scope: "jobs"},
		{Label: "Charts", Value: strconv.Itoa(st.Charts), Hint: "helm releases across clusters", Color: "blue", Scope: "charts"},
		{Label: "Service", Value: strconv.Itoa(st.Services), Hint: "services across clusters (click for full list including Ingress, NetworkPolicy, Endpoints)", Color: "blue", Scope: "svc"},
		{Label: "Configs", Value: strconv.Itoa(st.Configs), Hint: "configmaps + secrets across clusters", Color: "blue", Scope: "configs"},
		{Label: "Events", Value: strconv.Itoa(st.EventsWarning), Hint: "events with type=Warning across clusters", Color: "amber", Scope: "events"},
		{Label: "CPU", Value: coresFrac(st.CpuMilli, st.CpuTotalMilli), Hint: "cores in use / allocatable", Color: "teal"},
		{Label: "Memory", Value: gibFrac(st.MemBytes, st.MemTotalBytes) + " GiB", Hint: "GiB in use / allocatable", Color: "teal"},
		{Label: "Limits", Value: fmt.Sprintf("%.2f/%.2f GiB", float64(st.CpuLimitMilli)/1000, float64(st.MemLimitBytes)/(1<<30)), Hint: "sum of hard limits across containers: CPU cores / MEMORY GiB (click to list manifests)", Color: "amber", Scope: "limits"},
		{Label: "PVC/PV size", Value: gibFrac(st.PVCTotalBytes, st.PVTotalBytes) + " GiB", Hint: "sum of PVC requests / total PV capacity", Color: "sky", Scope: "pvc"},
		{Label: "PVC/PV Count", Value: pvPvcValue(st), Hint: "bound PVC / total PVC (PV, PersistentVolumeClaim, StorageClass)", Color: "sky", Scope: "pvc"},
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

func uniqueStates(pods []Pod) []string {
	set := make(map[string]struct{})
	for _, p := range pods {
		if p.State != "" {
			set[p.State] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for st := range set {
		out = append(out, st)
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
	followRetryDelay = 700 * time.Millisecond // пауза перед переоткрытием стрима
	followMaxErrors  = 5                      // сколько сбоев подряд терпим
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

// flight — один выполняющийся (или выполненный) запрос к fetchAllFn.
type flight struct {
	done      chan struct{}
	data      *PageData
	htmlBytes []byte
	jsonBytes []byte
}

// dataCache — потокобезопасный кеш агрегированных данных с single-flight:
// параллельные вызовы get() не запускают второй сбор данных, а ждут первый.
// Помимо самих данных кешируются отрендеренные HTML и JSON (обновляются
// только при cache miss), чтобы интервал автообновления не пересылал
// клиенту одну и ту же разметку.
type dataCache struct {
	now func() time.Time // часы для тестов
	ttl time.Duration
	mu  sync.Mutex
	// последний успешный снимок
	data      *PageData
	htmlBytes []byte
	jsonBytes []byte
	stamp     time.Time
	// текущий выполняющийся запрос (single-flight)
	inFlight *flight
}

// get возвращает данные из кеша, если они свежие относительно ttl; иначе —
// запускает fetch и кеширует результат. При ttl <= 0 кеш отключён (запрос
// каждый раз выполняется заново), но single-flight для одновременных вызовов
// сохраняется.
func (c *dataCache) get(fetch func() *PageData) (data *PageData) {
	data, _, _ = c.view(fetch, nil)
	return data
}

// view аналогичен get, но дополнительно кеширует отрендеренные html/json байты.
// render вызывается только при cache miss (или ttl<=0) и хранится до следующего
// обновления данных; при render==nil кешируются только сами данные.
func (c *dataCache) view(fetch func() *PageData, render func(*PageData) ([]byte, []byte)) (data *PageData, htmlBytes, jsonBytes []byte) {
	c.mu.Lock()
	if c.ttl > 0 && c.data != nil && c.now().Sub(c.stamp) < c.ttl {
		// данные могли прийти из get() без отрисовки — тогда байтов нет.
		stale := render != nil && c.htmlBytes == nil
		if !stale {
			data = c.data
			htmlBytes = c.htmlBytes
			jsonBytes = c.jsonBytes
			c.mu.Unlock()
			return data, htmlBytes, jsonBytes
		}
		data = c.data
		c.mu.Unlock()
		htmlBytes, jsonBytes = render(data)
		c.mu.Lock()
		c.htmlBytes, c.jsonBytes = htmlBytes, jsonBytes
		c.mu.Unlock()
		return data, htmlBytes, jsonBytes
	}
	if f := c.inFlight; f != nil {
		c.mu.Unlock()
		<-f.done
		return f.data, f.htmlBytes, f.jsonBytes
	}
	f := &flight{done: make(chan struct{})}
	c.inFlight = f
	c.mu.Unlock()

	// Паника в fetch/render не должна вешать single-flight: закрываем done
	// и сбрасываем inFlight, иначе все последующие запросы навсегда
	// заблокируются на <-f.done.
	defer func() {
		if r := recover(); r != nil {
			data = &PageData{Error: fmt.Sprintf("internal error while gathering data: %v", r)}
			f.data = data
			c.mu.Lock()
			if c.inFlight == f {
				close(f.done)
				c.inFlight = nil
			}
			c.mu.Unlock()
		}
	}()

	data = fetch()
	if render != nil {
		htmlBytes, jsonBytes = render(data)
	}

	c.mu.Lock()
	f.data = data
	f.htmlBytes = htmlBytes
	f.jsonBytes = jsonBytes
	close(f.done)
	c.inFlight = nil
	if c.ttl > 0 {
		c.data = data
		c.htmlBytes = htmlBytes
		c.jsonBytes = jsonBytes
		c.stamp = c.now()
	}
	c.mu.Unlock()
	return data, htmlBytes, jsonBytes
}

// flushDataCache сбрасывает кеш агрегированных данных.
func flushDataCache() {
	cacheMu.Lock()
	overviewCache = newDataCache(dataTTL)
	cacheMu.Unlock()
}

// overviewCache — глобальный кеш /api/pods и главной страницы.
var (
	overviewCache = newDataCache(dataTTL)
	cacheMu       sync.Mutex
)

func newDataCache(ttl time.Duration) *dataCache {
	return &dataCache{now: time.Now, ttl: ttl}
}

// renderCacheView кеширует отрендеренные html+json байты рядом с данными;
// fetch выполняется только при cache miss, рендер — один раз на снимок.
func renderCacheView(fetch func() *PageData, render func(*PageData) ([]byte, []byte)) (html, json []byte) {
	cacheMu.Lock()
	c := overviewCache
	cacheMu.Unlock()
	_, html, json = c.view(fetch, render)
	return html, json
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	html, _ := renderCacheView(func() *PageData {
		data := gatherData()
		return &data
	}, renderPage)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if len(html) == 0 {
		// renderPage не вернул байтов — вряд ли, но не отдаём пустую страницу.
		http.Error(w, "template render produced no output", http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(html); err != nil {
		log.Printf("index write: %v", err)
	}
}

func handleAPI(w http.ResponseWriter, r *http.Request) {
	_, json := renderCacheView(func() *PageData {
		data := gatherData()
		return &data
	}, renderPage)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(json); err != nil {
		log.Printf("api write: %v", err)
	}
}

// renderPage рендерит HTML-страницу и JSON-представление PageData.
// Оба результата кешируются как байты до обновления данных.
func renderPage(data *PageData) (html, jsonBuf []byte) {
	var b bytes.Buffer
	if err := tmpl.ExecuteTemplate(&b, "frontend", data); err != nil {
		log.Printf("template render: %v", err)
		return nil, nil
	}
	var jb bytes.Buffer
	if err := json.NewEncoder(&jb).Encode(data); err != nil {
		log.Printf("json encode: %v", err)
		return nil, nil
	}
	return b.Bytes(), jb.Bytes()
}

// ---------------------------------------------------------------------------
// Базовая авторизация (собственная, без внешних зависимостей).
// Работает только если заданы CB_AUTH_USERNAME и CB_AUTH_PASSWORD.
// Форма логина рендерится с тем же дизайном и шапкой, сессия хранится в cookie
// CB_AUTH_CACHE (секунды, по умолчанию 1 час).
// ---------------------------------------------------------------------------

// authSetup инициализирует конфигурацию авторизации из окружения.
func authSetup() {
	authUser = envOr("CB_AUTH_USERNAME", "")
	authPass = envOr("CB_AUTH_PASSWORD", "")
	authEnabled = authUser != "" && authPass != ""
	t := envDurationSeconds("CB_AUTH_CACHE", defaultAuthTTL)
	if t <= 0 {
		t = defaultAuthTTL // 0 не допускаем: сессия должна жить весь срок
	}
	authTTL = t
	if authEnabled {
		authSecret = make([]byte, 32)
		if _, err := rand.Read(authSecret); err != nil {
			authSecret = []byte(fmt.Sprintf("cb-secret-%d", time.Now().UnixNano()))
			log.Printf("rand: %v (fallback)", err)
		}
	}
}

// authToken подписывает строку HMAC-SHA256 и возвращает base64url(user|now|exp|sig).
// Зависимости времени нет: достаточно сверить подпись и срок действия.
func authToken(u string, ttl time.Duration) string {
	now := time.Now().Unix()
	exp := now + int64(ttl.Seconds())
	payload := fmt.Sprintf("%s|%d|%d", u, now, exp)
	mac := hmac.New(sha256.New, authSecret)
	mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload+"|"+sig))
}

// authValid проверяет подпись и срок действия токена сессии.
func authValid(tok string) bool {
	b, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return false
	}
	parts := strings.Split(string(b), "|")
	if len(parts) != 4 {
		return false
	}
	u, nowS, expS, sig := parts[0], parts[1], parts[2], parts[3]
	if u != authUser {
		return false
	}
	now, err1 := strconv.ParseInt(nowS, 10, 64)
	exp, err2 := strconv.ParseInt(expS, 10, 64)
	if err1 != nil || err2 != nil {
		return false
	}
	if time.Now().Unix() > exp {
		return false
	}
	mac := hmac.New(sha256.New, authSecret)
	mac.Write([]byte(strings.Join(parts[:3], "|")))
	want := fmt.Sprintf("%x", mac.Sum(nil))
	if len(want) != len(sig) || subtle.ConstantTimeCompare([]byte(want), []byte(sig)) != 1 {
		return false
	}
	_ = now
	return true
}

// authorize проверяет cookie сессии запроса.
func authorize(r *http.Request) bool {
	if !authEnabled {
		return true
	}
	if c, err := r.Cookie(authCookie); err == nil {
		return authValid(c.Value)
	}
	return false
}

// writeLoginPage рендерит форму логина с сохранением шапки и дизайна.
// Шаблон переиспользует "frontend", но с AuthRequired=true вместо контента.
func writeLoginPage(w http.ResponseWriter, errMsg string) {
	data := PageData{AuthRequired: true, AuthError: errMsg, Generated: time.Now()}
	var b bytes.Buffer
	if err := tmpl.ExecuteTemplate(&b, "frontend", &data); err != nil {
		log.Printf("login template render: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(b.Bytes()); err != nil {
		log.Printf("login page write: %v", err)
	}
}

// writeJSON сериализует v в w и логирует ошибку записи/кодирования.
func writeJSON(w http.ResponseWriter, v interface{}) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("json encode response: %v", err)
	}
}

// handleLogin обрабатывает POST /login: проверяет учётные данные и ставит cookie.
func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeLoginPage(w, "")
		return
	}
	u := r.PostFormValue("username")
	p := r.PostFormValue("password")
	ok := subtle.ConstantTimeCompare([]byte(u), []byte(authUser)) == 1 &&
		subtle.ConstantTimeCompare([]byte(p), []byte(authPass)) == 1
	if !ok {
		writeLoginPage(w, "Неверные имя пользователя или пароль")
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     authCookie,
		Value:    authToken(u, authTTL),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(authTTL.Seconds()),
	})
	log.Printf("auth: user %q logged in", u)
	http.Redirect(w, r, "/", http.StatusFound)
}

// handleLogout сбрасывает cookie сессии.
func handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     authCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})
	http.Redirect(w, r, authLoginURL, http.StatusFound)
}

// authHandler оборачивает роутер: если авторизация включена, требует сессию
// для всех маршрутов, кроме /login и /logout.
func authHandler(next http.Handler) http.Handler {
	if !authEnabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case authLoginURL:
			if r.Method == http.MethodPost {
				handleLogin(w, r)
				return
			}
			writeLoginPage(w, "")
			return
		case "/logout":
			handleLogout(w, r)
			return
		}
		if authorize(r) {
			next.ServeHTTP(w, r)
			return
		}
		// API без авторизации — 401, HTML — форма логина.
		if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/logs" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		writeLoginPage(w, "")
	})
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
	st.PVTotalBytes = ov.PVTotalBytes
	st.PVCOk = ov.PVCOk
	st.StorageClasses = ov.StorageClasses
	st.Networks = ov.Networks
	st.Services = ov.Services
	st.Jobs = ov.Jobs
	st.JobsDone = ov.JobsDone
	st.Configs = ov.Configs
	st.Charts = ov.Charts
	st.EventsWarning = ov.EventsWarning
	nsJSON := template.JS("{}")
	if ov.NS != nil {
		if b, err := json.Marshal(ov.NS); err == nil {
			nsJSON = template.JS(b)
		}
	}
	return PageData{
		Pods:       pods,
		Contexts:   ctxs,
		Namespaces: uniqueNamespaces(pods),
		Nodes:      uniqueNodes(pods),
		Phases:     uniquePhases(pods),
		States:     uniqueStates(pods),
		Stats:      st,
		NS:         ov.NS,
		NSJSON:     nsJSON,
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
		if _, err := fmt.Fprint(w, "event: error\ndata: {\"error\":\"no log sources selected\"}\n\n"); err != nil {
			log.Printf("sse error write: %v", err)
		}
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

	if _, err := fmt.Fprint(w, "retry: 2000\n\n"); err != nil {
		log.Printf("sse retry write: %v", err)
	}
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
				log.Printf("sse marshal: %v", err)
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				log.Printf("sse data write: %v", err)
				return
			}
			flusher.Flush()
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				log.Printf("sse ping write: %v", err)
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}

streamDone:
	if _, err := fmt.Fprint(w, "event: done\ndata: {}\n\n"); err != nil {
		log.Printf("sse done write: %v", err)
	}
	flusher.Flush()
}

// ---------------------------------------------------------------------------
// Просмотр связанных ресурсов (манифестов) для пода
// ---------------------------------------------------------------------------

// ObjRef — одна строка в списке связанных ресурсов.
type ObjRef struct {
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Cat     string `json:"cat"`
	Reason  string `json:"reason"`
	Age     string `json:"age"`
	Ctx     string `json:"ctx,omitempty"`
	Ns      string `json:"ns,omitempty"`
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
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
		writeJSON(w, resp)
		return
	}
	ctxs, err := getContextsFn()
	if err != nil {
		resp.Error = err.Error()
		writeJSON(w, resp)
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
	case "ns", "nodes":
		var kind, plural, cat string
		switch scope {
		case "ns":
			kind, plural, cat = "namespace", "namespaces", "Cluster"
		case "nodes":
			kind, plural, cat = "node", "nodes", "Cluster"
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
				if json.Unmarshal(trimToJSON(out), &list) != nil {
					return
				}
				for _, it := range list.Items {
					ref := ObjRef{
						Kind: kind, Name: it.Metadata.Name, Cat: cat,
						Ctx: c, Ns: it.Metadata.Namespace,
						Age: ageTime(it.Metadata.CreationTimestamp),
					}
					if scope == "nodes" {
						ref.Ns = ""
						ref.Reason = "not ready"
						for _, cd := range it.Status.Conditions {
							if cd.Type == "Ready" && cd.Status == "True" {
								ref.Reason = "ready"
							}
						}
					} else {
						ref.Reason = it.Status.Phase
					}
					add(ref)
				}
			}(c, kind, plural, cat)
		}
	case "events":
		var kind, cat = "event", "Event"
		for _, c := range ctxs {
			wg.Add(1)
			go func(c string) {
				defer wg.Done()
				out, e := runKubectlFn("--context", c, "get", "events", "-A", "-o", "json")
				if e != nil {
					mu.Lock()
					refs = append(refs, ObjRef{Kind: kind, Ctx: c, Reason: "RBAC/error"})
					mu.Unlock()
					return
				}
				var list struct {
					Items []kubectlEvent `json:"items"`
				}
				if json.Unmarshal(trimToJSON(out), &list) != nil {
					return
				}
				for _, ev := range list.Items {
					add(ObjRef{
						Kind: kind, Name: ev.Metadata.Namespace + "/" + ev.InvolvedObject.Kind + "/" + ev.InvolvedObject.Name,
						Cat: cat, Reason: ev.Reason, Type: ev.Type, Message: ev.Message,
						Ctx: c, Ns: ev.Metadata.Namespace,
						Age: ageTime(parseRFC3339(ev.LastTimestamp)),
					})
				}
			}(c)
		}
	case "jobs", "configs", "workloads", "svc", "pvc":
		for _, c := range ctxs {
			wg.Add(1)
			go func(c string) {
				defer wg.Done()
				kinds := scopeKinds(scope)
				if scope == "configs" {
					kinds = configsScopeKinds()
				}
				if scope == "svc" {
					kinds = append(kinds, egressKinds(c)...)
				}
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
				if json.Unmarshal(trimToJSON(out), &ml) != nil {
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
							ref := ObjRef{
								Kind: singular, Name: it.Metadata.Name, Cat: objCat(singular),
								Ctx: c, Ns: it.Metadata.Namespace,
								Age: ageTime(it.Metadata.CreationTimestamp),
							}
							if scope == "pvc" && singular == "persistentvolumeclaim" {
								if capB, ok := memBytes(it.Spec.Resources.Requests["storage"]); ok {
									ref.Reason = fmt.Sprintf("%.2f GiB", float64(capB)/(1<<30))
								}
							}
							add(ref)
						}
						continue
					}
					// Плоский List: каждый объект несёт собственный kind.
					if mo.Metadata.Name == "" {
						continue
					}
					singular := strings.ToLower(mo.Kind)
					ref := ObjRef{
						Kind: singular, Name: mo.Metadata.Name, Cat: objCat(singular),
						Ctx: c, Ns: mo.Metadata.Namespace,
						Age: ageTime(mo.Metadata.CreationTimestamp),
					}
					if scope == "pvc" && singular == "persistentvolumeclaim" {
						if capB, ok := memBytes(mo.Spec.Resources.Requests["storage"]); ok {
							ref.Reason = fmt.Sprintf("%.2f GiB", float64(capB)/(1<<30))
						}
					}
					add(ref)
				}
			}(c)
		}
	case "charts":
		// Helm-релизы хранятся в виде Secret с label owner=helm.
		for _, c := range ctxs {
			wg.Add(1)
			go func(c string) {
				defer wg.Done()
				out, e := runKubectlFn("--context", c, "get", "secrets", "-A", "-l", "owner=helm", "-o", "json")
				if e != nil {
					return // RBAC/нет прав — пропускаем, показываем доступное
				}
				var list struct {
					Items []struct {
						Metadata struct {
							Name              string    `json:"name"`
							Namespace         string    `json:"namespace"`
							CreationTimestamp time.Time `json:"creationTimestamp"`
						} `json:"metadata"`
					} `json:"items"`
				}
				if json.Unmarshal(trimToJSON(out), &list) != nil {
					return
				}
				for _, it := range list.Items {
					ref := ObjRef{
						Kind: "chart", Name: helmReleaseName(it.Metadata.Name), Cat: "Chart",
						Ctx: c, Ns: it.Metadata.Namespace,
						Age: ageTime(it.Metadata.CreationTimestamp),
					}
					mu.Lock()
					refs = append(refs, ref)
					mu.Unlock()
				}
			}(c)
		}
	default:
		resp.Error = "unknown scope"
		writeJSON(w, resp)
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
	writeJSON(w, resp)
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
		if e := json.Unmarshal(trimToJSON(out), &d); e != nil {
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
			if json.Unmarshal(trimToJSON(out), &d) == nil {
				svcD = &d
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if out, e := runKubectlFn("--context", ctx, "get", "ingresses", "-n", ns, "-o", "json"); e == nil {
			var d ingressList
			if json.Unmarshal(trimToJSON(out), &d) == nil {
				ingD = &d
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		if out, e := runKubectlFn("--context", ctx, "get", "replicasets", "-n", ns, "-o", "json"); e == nil {
			var d rsList
			if json.Unmarshal(trimToJSON(out), &d) == nil {
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
	writeJSON(w, resp)
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
	if err := json.Unmarshal(trimToJSON(out), &cfg); err != nil {
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
		writeJSON(w, resp)
		return
	}
	if kind == "cluster" {
		y, e := clusterSnippetYAML(ctx)
		if e != nil {
			resp.Error = e.Error()
		} else {
			resp.Yaml = y
		}
		writeJSON(w, resp)
		return
	}
	if kind == "chart" {
		// Helm-релизы не являются ресурсом K8s («chart» не существует как тип).
		// Показываем содержимое helm-Secret: имя, версия, values, статус.
		y, e := helmReleaseYAML(ctx, ns, name)
		if e != nil {
			resp.Error = e.Error()
		} else {
			resp.Yaml = y
		}
		writeJSON(w, resp)
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
	writeJSON(w, resp)
}

func main() {
	authSetup()

	if authEnabled {
		log.Printf("CrossBoard: basic auth enabled (user %q, session TTL %v)", authUser, authTTL)
	} else {
		log.Printf("CrossBoard: basic auth disabled (set CB_AUTH_USERNAME/CB_AUTH_PASSWORD to enable)")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/login", handleLogin)
	mux.HandleFunc("/logout", handleLogout)
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/api/pods", handleAPI)
	mux.HandleFunc("/api/related", handleRelated)
	mux.HandleFunc("/api/object", handleObject)
	mux.HandleFunc("/api/overview", handleOverview)
	mux.HandleFunc("/logs", handleLogs)

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           authHandler(mux),
		ReadHeaderTimeout: 5 * time.Second, // защита от медленного заголовка (slowloris)
	}
	// WriteTimeout сознательно не задаём: долгоживущее SSE-соединение /logs.
	log.Printf("CrossBoard: http://localhost%s", listenAddr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("listen: %v", err)
	}
}
