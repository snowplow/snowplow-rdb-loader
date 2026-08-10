#!/bin/bash
# Route executor/history-server to Spark's own entrypoint unchanged.
#
# For driver mode the caller passes spark-submit options and application args
# as a single flat list, separated by a "--" sentinel:
#
#   [spark-submit options] -- [application args]
#
# This script splits on "--" so that spark-submit options land before --class
# and the JAR (where spark-submit expects them) and application args land after
# (where the application's own CLI parser expects them).
case "$1" in
  executor | history-server)
    exec /opt/entrypoint.sh "$@"
    ;;
  *)
    spark_args=()
    app_args=()
    past_separator=false
    for arg in "$@"; do
      if [[ "${arg}" == "--" ]]; then
        past_separator=true
        continue
      fi
      if "${past_separator}"; then
        app_args+=("${arg}")
      else
        spark_args+=("${arg}")
      fi
    done
    exec /opt/entrypoint.sh driver \
      --conf "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
      --conf "spark.hadoop.fs.AbstractFileSystem.s3.impl=org.apache.hadoop.fs.s3a.S3A" \
      --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.snowplowanalytics.snowplow.rdbloader.transformer.batch.aws.S3ADefaultCredentialsProvider" \
      --conf "spark.hadoop.fs.s3.aws.credentials.provider=com.snowplowanalytics.snowplow.rdbloader.transformer.batch.aws.S3ADefaultCredentialsProvider" \
      "${spark_args[@]}" \
      --class com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Main \
      local:///opt/snowplow/snowplow-transformer-batch.jar \
      "${app_args[@]}"
    ;;
esac
