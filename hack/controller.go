package hack

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// ====================================================================
// 4. CONTROLLER RECONCILER IMPLEMENTATION
// ====================================================================

type MySampleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *MySampleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.Log.WithValues("namespace", req.Namespace, "name", req.Name)

	// Fetch target object from our sharded memory cache layer
	deployment := &appsv1.Deployment{}
	if err := r.Get(ctx, req.NamespacedName, deployment); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("🚀 Reconciling deployment assigned to my shard segment!")
	return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
}

// ====================================================================
// 5. PARSING SYSTEM ORCHESTRATION & HELPER BOOTSTRAP
// ====================================================================

func getShardConfigOrDie() (int, int) {
	podName := os.Getenv("POD_NAME")
	totalShardsStr := os.Getenv("TOTAL_SHARDS_COUNT")

	if podName == "" || totalShardsStr == "" {
		fmt.Println("Warning: Missing sharding variables, resetting defaults to standalone mode.")
		return 0, 1
	}

	totalShards, err := strconv.Atoi(totalShardsStr)
	if err != nil || totalShards < 1 {
		panic("Invalid TOTAL_SHARDS_COUNT configuration string.")
	}

	// Parsing trailing index off a StatefulSet ordinal token (e.g. operator-pod-3)
	re := regexp.MustCompile(`-(\d+)$`)
	matches := re.FindStringSubmatch(podName)
	if len(matches) < 2 {
		panic(fmt.Sprintf("Failed to identify a valid stateful ordinal context from: %s", podName))
	}

	shardID, _ := strconv.Atoi(matches[1])
	return shardID, totalShards
}

func run() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	setupLog := ctrl.Log.WithName("bootstrap")

	// 1. Calculate active shard assignments
	shardID, totalShards := getShardConfigOrDie()
	setupLog.Info("Configuring Cluster Operator Instance", "ShardID", shardID, "TotalShards", totalShards)

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	// 2. Configure Manager options with overridden cache layer injection
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:   scheme,
		NewCache: NewShardedCacheBuilder(shardID, totalShards),
		// Disabling classic single-leader election since shards run simultaneously in parallel
		LeaderElection: false,
	})
	if err != nil {
		setupLog.Error(err, "Unable to boot up the cluster manager infrastructure")
		os.Exit(1)
	}

	// 3. Bind runtime event pipeline filters
	err = ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{}, builder.WithPredicates(FilterToMyShard(shardID, totalShards))).
		Owns(&corev1.Pod{}, builder.WithPredicates(FilterToMyShard(shardID, totalShards))).
		Complete(&MySampleReconciler{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		})

	if err != nil {
		setupLog.Error(err, "Unable to successfully assemble controller layout parameters")
		os.Exit(1)
	}

	setupLog.Info("Starting Sharded Manager Loop...")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Problem running manager loop engine")
		os.Exit(1)
	}
}
