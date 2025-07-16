echo "debugging joins"
spark-submit --master spark://localhost:7077 src/optimize_job.py > log/optimize.log 2>&1