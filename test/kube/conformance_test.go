package kube

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/clock"
)

var (
	scheme    = runtime.NewScheme()
	codecs    = serializer.NewCodecFactory(scheme)
	endpoints []string
)

func init() {
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(examplev1.AddToScheme(scheme))
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// TestMain starts two duet nodes on one disk image for the whole run.
func TestMain(m *testing.M) {
	os.Exit(func() int {
		dir, err := os.MkdirTemp("", "duet-kube-")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(dir)
		bin, img := filepath.Join(dir, "duet"), filepath.Join(dir, "disk.img")
		build := exec.Command("go", "build", "-o", bin, "./cmd/duet")
		build.Dir = "../.."
		if out, err := build.CombinedOutput(); err != nil {
			panic(fmt.Sprintf("build: %v\n%s", err, out))
		}
		if err := os.WriteFile(img, nil, 0o600); err != nil {
			panic(err)
		}
		if err := os.Truncate(img, 1<<30); err != nil {
			panic(err)
		}
		direct := "-direct=true"
		format := func() ([]byte, error) {
			return exec.Command(bin, "format", "-device", img, "-nodes", "2", "-ttl", "1s",
				"-epoch-slots", "64", "-max-batch", "2097152", direct).CombinedOutput()
		}
		if _, err := format(); err != nil {
			direct = "-direct=false"
			if out, err := format(); err != nil {
				panic(fmt.Sprintf("format: %v\n%s", err, out))
			}
		}
		for i := 0; i < 2; i++ {
			ep := freeAddr()
			endpoints = append(endpoints, ep)
			state := filepath.Join(dir, fmt.Sprint("state", i))
			os.Mkdir(state, 0o700)
			cmd := exec.Command(bin, "run", "-device", img, "-node", fmt.Sprint(i), "-listen", freeAddr(),
				"-etcd", ep, "-state-dir", state, "-start-delay", "200ms", "-watch-progress-interval", "1s", direct)
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				panic(err)
			}
			defer func() { cmd.Process.Kill(); cmd.Wait() }()
		}
		return m.Run()
	}())
}

// setup builds the store kube-apiserver would build for one resource, under
// a prefix unique to the test so tests do not see each other's objects.
func setup(t *testing.T) (context.Context, storage.Interface, *kubernetes.Client) {
	t.Helper()
	client, err := kubernetes.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	codec := apitesting.TestCodec(codecs, examplev1.SchemeGroupVersion)
	versioner := storage.APIObjectVersioner{}
	compactor := etcd3.NewCompactor(client.Client, 0, clock.RealClock{}, nil)
	t.Cleanup(compactor.Stop)
	prefix := "/" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	store, err := etcd3.New(client, compactor, codec,
		func() runtime.Object { return &example.Pod{} },
		func() runtime.Object { return &example.PodList{} },
		prefix, "/pods/", schema.GroupResource{Resource: "pods"},
		storagetesting.NewPrefixTransformer([]byte("test!"), false),
		etcd3.NewDefaultLeaseManagerConfig(),
		etcd3.NewDefaultDecoder(codec, versioner), versioner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return context.Background(), store, client
}

func increaseRV(c *kubernetes.Client) storagetesting.IncreaseRVFunc {
	return func(ctx context.Context, t *testing.T) int64 {
		resp, err := c.KV.Put(ctx, "increaseRV", "ok")
		if err != nil {
			t.Fatalf("could not raise the revision: %v", err)
		}
		return resp.Header.Revision
	}
}

// compact compacts the way kube-apiserver's compactor does, including the
// bookkeeping key the store watches to learn the compacted revision.
func compact(c *kubernetes.Client) storagetesting.Compaction {
	return func(ctx context.Context, t *testing.T, resourceVersion string) {
		rv, err := storage.APIObjectVersioner{}.ParseResourceVersion(resourceVersion)
		if err != nil {
			t.Fatal(err)
		}
		// The first call learns the bookkeeping key's version when another
		// test has compacted before; the second then succeeds.
		var current, done int64
		for i := 0; i < 3 && err == nil && done != int64(rv); i++ {
			current, _, done, err = etcd3.Compact(ctx, c.Client, current, int64(rv))
		}
		if err == nil && done != int64(rv) {
			err = fmt.Errorf("compacted revision is %d", done)
		}
		if err != nil {
			t.Fatalf("compact to %d: %v", rv, err)
		}
	}
}

func simple(run func(context.Context, *testing.T, storage.Interface)) func(*testing.T) {
	return func(t *testing.T) {
		ctx, s, _ := setup(t)
		run(ctx, t, s)
	}
}

func TestStorage(t *testing.T) {
	noKeyCheck := func(context.Context, *testing.T, string) {}
	for name, run := range map[string]func(context.Context, *testing.T, storage.Interface){
		"Create": func(c context.Context, t *testing.T, s storage.Interface) {
			storagetesting.RunTestCreate(c, t, s, noKeyCheck)
		},
		"CreateWithTTL":                             storagetesting.RunTestCreateWithTTL,
		"CreateWithKeyExist":                        storagetesting.RunTestCreateWithKeyExist,
		"Get":                                       storagetesting.RunTestGet,
		"UnconditionalDelete":                       storagetesting.RunTestUnconditionalDelete,
		"ConditionalDelete":                         storagetesting.RunTestConditionalDelete,
		"DeleteWithSuggestion":                      storagetesting.RunTestDeleteWithSuggestion,
		"DeleteWithSuggestionAndConflict":           storagetesting.RunTestDeleteWithSuggestionAndConflict,
		"DeleteWithConflict":                        storagetesting.RunTestDeleteWithConflict,
		"DeleteWithSuggestionOfDeletedObject":       storagetesting.RunTestDeleteWithSuggestionOfDeletedObject,
		"ValidateDeletionWithSuggestion":            storagetesting.RunTestValidateDeletionWithSuggestion,
		"PreconditionalDeleteWithSuggestion":        storagetesting.RunTestPreconditionalDeleteWithSuggestion,
		"GetListRecursivePrefix":                    storagetesting.RunTestGetListRecursivePrefix,
		"GuaranteedUpdateWithTTL":                   storagetesting.RunTestGuaranteedUpdateWithTTL,
		"GuaranteedUpdateWithConflict":              storagetesting.RunTestGuaranteedUpdateWithConflict,
		"GuaranteedUpdateWithSuggestionAndConflict": storagetesting.RunTestGuaranteedUpdateWithSuggestionAndConflict,
		"ListPaging":                                storagetesting.RunTestListPaging,
		"NamespaceScopedList":                       storagetesting.RunTestNamespaceScopedList,
		"KeySchema":                                 storagetesting.RunTestKeySchema,
	} {
		t.Run(name, simple(run))
	}
	t.Run("ListContinuation", func(t *testing.T) {
		ctx, s, _ := setup(t)
		storagetesting.RunTestListContinuation(ctx, t, s, func(*testing.T, uint64, uint64) {})
	})
	t.Run("ListInconsistentContinuation", func(t *testing.T) {
		ctx, s, c := setup(t)
		storagetesting.RunTestListInconsistentContinuation(ctx, t, s, compact(c))
	})
	t.Run("ConsistentList", func(t *testing.T) {
		ctx, s, c := setup(t)
		storagetesting.RunTestConsistentList(ctx, t, s, increaseRV(c), false, true, false)
	})
	t.Run("GetListNonRecursive", func(t *testing.T) {
		ctx, s, c := setup(t)
		storagetesting.RunTestGetListNonRecursive(ctx, t, increaseRV(c), s)
	})
	t.Run("CompactRevision", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, true)
		ctx, s, c := setup(t)
		// The store learns the compacted revision from its watch on the
		// bookkeeping key; wait for that, as Kubernetes' own test does.
		do := compact(c)
		storagetesting.RunTestCompactRevision(ctx, t, s, increaseRV(c), func(ctx context.Context, t *testing.T, rv string) {
			do(ctx, t, rv)
			for deadline := time.Now().Add(5 * time.Second); fmt.Sprint(s.CompactRevision()) != rv && time.Now().Before(deadline); {
				time.Sleep(20 * time.Millisecond)
			}
		})
	})
}

func TestWatch(t *testing.T) {
	for name, run := range map[string]func(context.Context, *testing.T, storage.Interface){
		"Watch":                              storagetesting.RunTestWatch,
		"DeleteTriggerWatch":                 storagetesting.RunTestDeleteTriggerWatch,
		"WatchFromNonZero":                   storagetesting.RunTestWatchFromNonZero,
		"DelayedWatchDelivery":               storagetesting.RunTestDelayedWatchDelivery,
		"WatchContextCancel":                 storagetesting.RunTestWatchContextCancel,
		"WatcherTimeout":                     storagetesting.RunTestWatcherTimeout,
		"WatchDeleteEventObjectHaveLatestRV": storagetesting.RunTestWatchDeleteEventObjectHaveLatestRV,
		"WatchInitializationSignal":          storagetesting.RunTestWatchInitializationSignal,
		"ClusterScopedWatch":                 storagetesting.RunTestClusterScopedWatch,
		"NamespaceScopedWatch":               storagetesting.RunTestNamespaceScopedWatch,
	} {
		t.Run(name, simple(run))
	}
	t.Run("WatchFromZero", func(t *testing.T) {
		ctx, s, c := setup(t)
		storagetesting.RunTestWatchFromZero(ctx, t, s, compact(c))
	})
	t.Run("WatchDispatchBookmarkEvents", func(t *testing.T) {
		ctx, s, _ := setup(t)
		storagetesting.RunTestWatchDispatchBookmarkEvents(ctx, t, s, false)
	})
}
