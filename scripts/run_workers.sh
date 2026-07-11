#!/bin/bash

# Arguments
N=${1:-4}

export DATABASE_URL=${DATABASE_URL:-"sqlite:///asyncflow.db"}
export QUEUE_HOST=${QUEUE_HOST:-"127.0.0.1"}
export QUEUE_PORT=${QUEUE_PORT:-"9000"}

declare -a WORKER_PIDS

cleanup() {
    echo "Received signal, shutting down workers..."
    for PID in "${WORKER_PIDS[@]}"; do
        if kill -0 "$PID" 2>/dev/null; then
            kill -TERM "$PID"
        fi
    done

    # Wait up to 10 seconds for graceful shutdown
    for i in {1..10}; do
        all_dead=true
        for PID in "${WORKER_PIDS[@]}"; do
            if kill -0 "$PID" 2>/dev/null; then
                all_dead=false
                break
            fi
        done
        if $all_dead; then
            echo "All workers shut down gracefully."
            exit 0
        fi
        sleep 1
    done

    echo "Timeout reached, force killing remaining workers..."
    for PID in "${WORKER_PIDS[@]}"; do
        if kill -0 "$PID" 2>/dev/null; then
            kill -KILL "$PID"
        fi
    done
    exit 1
}

trap cleanup SIGINT SIGTERM

echo "Starting $N workers..."
for i in $(seq 1 $N); do
    export WORKER_ID="worker_${i}_$(python -c 'import uuid; print(uuid.uuid4())')"
    python -m worker &
    PID=$!
    WORKER_PIDS+=($PID)
    echo "Launched worker $WORKER_ID with PID $PID"
done

wait
