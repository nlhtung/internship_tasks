export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=4g

$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077 &
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077 &
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077 &