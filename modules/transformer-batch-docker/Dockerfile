# Dockerfile for the batch transformer Spark-on-Kubernetes image.
# Other Docker images in this repo are built via sbt-native-packager (see build.sbt);
# this one is built separately because it must be based on the Spark base image.

ARG SPARK_IMAGE_TAG
FROM apache/spark:${SPARK_IMAGE_TAG}

ARG VERSION
COPY SNOWPLOW-LICENSE.md /SNOWPLOW-LICENSE.md
COPY snowplow-transformer-batch-docker-${VERSION}.jar /opt/snowplow/snowplow-transformer-batch.jar
COPY --chmod=755 docker-entrypoint.sh /opt/snowplow-entrypoint.sh

# Inherited from the base image; declared explicitly so security tooling sees a numeric UID.
USER 185:185

ENTRYPOINT ["/opt/snowplow-entrypoint.sh"]
