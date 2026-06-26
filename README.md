# k8s-sharded-controller

Functions to help scaling Kubernetes Controller via sharding (stable hashing) across multiple active instances.

## Goal

Provide a mininally evasive way to allow Custom Kubernetes Controllers to scale horizintally and perform reconciliation in parallel, instead of relying on a global lock (leader election).

This is achieved by leveraging consistent hashing to discard reconcile events (and caching) on each Controller instance.

The current implementation relies on a stable naming for each Controller instance (so then the hashing works as expected). This can be achieved by instead of running the Controller as a `Deployment`, as a `Statefulset`.

## Statefulset

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: my-sharded-operator
  namespace: operator-system
spec:
  serviceName: "my-sharded-operator"
  replicas: 3 # This matches your TOTAL_SHARDS count
  selector:
    matchLabels:
      app: my-sharded-operator
  template:
    metadata:
      labels:
        app: my-sharded-operator
    spec:
      containers:
      - name: manager
        image: my-sharded-operator:latest
        env:
        # 1. Inject the deterministic Pod Name (e.g., my-sharded-operator-0)
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        # 2. Hardcode or inject the total expected replicas
        - name: TOTAL_SHARDS_COUNT
          value: "3" # Must match the replica count!

```

## Interesting links

- https://github.com/kubernetes-sigs/controller-runtime/issues/2576
- https://github.com/timebertt/kubernetes-controller-sharding/tree/main
