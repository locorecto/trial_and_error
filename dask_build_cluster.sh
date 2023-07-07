#!/bin/bash

# Set the OpenShift project and namespace
PROJECT_NAME="your-project"
NAMESPACE="your-namespace"

# Set the Dask scheduler and worker image names
SCHEDULER_IMAGE="dask/scheduler"
WORKER_IMAGE="dask/worker"

# Set the number of workers and resources for each worker
NUM_WORKERS=4
WORKER_CPU_LIMIT="1"
WORKER_MEMORY_LIMIT="1Gi"

# Deploy the Dask scheduler
oc new-app ${SCHEDULER_IMAGE} --name=dask-scheduler -n ${NAMESPACE}

# Get the route for the Dask scheduler
SCHEDULER_ROUTE=$(oc get route dask-scheduler -n ${NAMESPACE} -o jsonpath='{.spec.host}')
echo "Dask scheduler is running at: http://${SCHEDULER_ROUTE}"

# Deploy Dask workers
for i in $(seq 1 ${NUM_WORKERS}); do
    oc run dask-worker-${i} --image=${WORKER_IMAGE} --env="DASK_SCHEDULER=${SCHEDULER_ROUTE}" --limits="cpu=${WORKER_CPU_LIMIT},memory=${WORKER_MEMORY_LIMIT}" -n ${NAMESPACE}
done

# Wait for the workers to start
sleep 10

# Check the status of the workers
oc get pods -l run=dask-worker -n ${NAMESPACE}
