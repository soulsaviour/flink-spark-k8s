#!/usr/bin/env bash
# Installs cert-manager (needed by the Flink operator's webhook), the Flink
# Kubernetes Operator and the Spark operator, at pinned versions.
set -euo pipefail
cd "$(dirname "$0")"

CERT_MANAGER_VERSION=v1.19.1
FLINK_OPERATOR_VERSION=1.16.1
SPARK_OPERATOR_VERSION=2.5.2

kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
kubectl -n cert-manager rollout status deployment/cert-manager-webhook --timeout 180s

kubectl create namespace flink --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace spark-jobs --dry-run=client -o yaml | kubectl apply -f -

helm repo add flink-operator-repo \
  "https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/" --force-update
helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update

helm upgrade --install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --version "$FLINK_OPERATOR_VERSION" --namespace flink-operator --create-namespace \
  -f flink-operator-values.yaml --wait

helm upgrade --install spark-operator spark-operator/spark-operator \
  --version "$SPARK_OPERATOR_VERSION" --namespace spark-operator --create-namespace \
  -f spark-operator-values.yaml --wait

kubectl get pods -n flink-operator
kubectl get pods -n spark-operator
