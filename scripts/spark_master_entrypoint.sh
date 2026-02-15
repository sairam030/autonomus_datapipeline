#!/bin/bash
set -e

echo "============================================"
echo "Starting Spark Master Container"
echo "============================================"

# Start Spark Master
echo "[$(date)] Starting Spark Master..."
/opt/spark/sbin/start-master.sh -h spark-master

# Keep container alive
echo "[$(date)] Spark Master initialization complete"
echo "============================================"
sleep infinity
