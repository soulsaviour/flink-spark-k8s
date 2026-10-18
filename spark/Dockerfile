# syntax=docker/dockerfile:1
# Spark image with the jobs and the S3A connector baked in, so jobs don't
# download jars at start-up. hadoop-aws must match Spark's Hadoop (3.5.0 for
# Spark 4.2) and the AWS SDK bundle must match hadoop-aws.
FROM spark:4.2.0-python3

ARG HADOOP_AWS_VERSION=3.5.0
ARG AWS_SDK_VERSION=2.35.4

USER 0
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar /opt/spark/jars/
# 185 is the spark user in the official image.
USER 185

COPY jobs/ /opt/spark/jobs/
