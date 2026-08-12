#!/bin/bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  set -- "jobmanager"
fi

case "$1" in
  jobmanager)
    exec /opt/flink/bin/jobmanager.sh start-foreground
    ;;
  taskmanager)
    exec /opt/flink/bin/taskmanager.sh start-foreground
    ;;
  bash|sh)
    exec "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
