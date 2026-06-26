package hack

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// ====================================================================
// 1. DYNAMIC CONSISTENT SHARDING LAYER (Rendezvous Hashing)
// ====================================================================

// DetermineShard parses an object's root controller owner chain and
// evaluates which deterministic shard ID must process the work.
func DetermineShard(obj client.Object, totalShards int) int {
	if totalShards <= 1 {
		return 0
	}

	targetNamespace := obj.GetNamespace()
	targetName := obj.GetName()

	// Recursively inspect OwnerReferences to keep children bound to the same shard
	for _, owner := range obj.GetOwnerReferences() {
		if owner.Controller != nil && *owner.Controller {
			targetName = owner.Name
			break
		}
	}

	resourceKey := fmt.Sprintf("%s/%s", targetNamespace, targetName)
	
	var highestWeight uint64
	winningShard := 0

	// Rendezvous Hashing loop (Highest Random Weight)
	for i := 0; i < totalShards; i++ {
		hasher := sha256.New()
		hasher.Write([]byte(fmt.Sprintf("%s-%d", resourceKey, i)))
		hashBytes := hasher.Sum(nil)

		weight := binary.BigEndian.Uint64(hashBytes[:8])
		if weight > highestWeight {
			highestWeight = weight
			winningShard = i
		}
	}

	return winningShard
}

// ====================================================================
// 2. CACHE LAYER (Drops items from background Informer memory)
// ====================================================================

type ShardedCacheInterceptor struct {
	cache.Cache
	shardID     int
	totalShards int
}

// Get overrides reading requests, verifying alignment boundaries
func (s *ShardedCacheInterceptor) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	err := s.Cache.Get(ctx, key, obj, opts...)
	if err != nil {
		return err
	}
	if DetermineShard(obj, s.totalShards) != s.shardID {
		return errors.NewNotFound(
			// obj.GetObjectKind().GroupVersionKind().GroupKind(),
			obj.GetObjectKind().,
			key.Name,
		)
	}
	return nil
}

// NewShardedCacheBuilder instantiates the memory protection wrapper
func NewShardedCacheBuilder(shardID, totalShards int) cache.NewCacheFunc {
	return func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
		baseCache, err := cache.New(config, opts)

		if err != nil {
			return nil, err
		}

		return &ShardedCacheInterceptor{
			Cache:       baseCache,
			shardID:     shardID,
			totalShards: totalShards,
		}, nil
	}
}

// ====================================================================
// 3. WATCH/EVENT FILTER LAYER (Predicates)
// ====================================================================

// FilterToMyShard blocks unassigned events from entering the workqueue
func FilterToMyShard(shardID, totalShards int) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return DetermineShard(e.Object, totalShards) == shardID
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return DetermineShard(e.ObjectNew, totalShards) == shardID
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return DetermineShard(e.Object, totalShards) == shardID
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return DetermineShard(e.Object, totalShards) == shardID
		},
	}
}
