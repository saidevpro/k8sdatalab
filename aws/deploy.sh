#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Deploy the datalab EMR Serverless batch platform in 2 phases:
#   phase 1: infra (ECR, network, storage, catalog, IAM, params/secrets)
#   build  : build & push the aws-spark-emr image to the created ECR repo
#   phase 2: EMR Serverless app + Step Functions + EventBridge schedules
#
# Usage:
#   PRIM_TOKEN=xxxxx ./aws/deploy.sh            # full run (all phases)
#   ./aws/deploy.sh phase1 | build | phase2     # run a single phase
# ---------------------------------------------------------------------------
set -euo pipefail

PREFIX="${PREFIX:-datalab}"
REGION="${AWS_REGION:-eu-west-3}"
STACK="${STACK:-${PREFIX}-emr-serverless}"
TAG="${TAG:-3.5.8}"
TEMPLATE="$(dirname "$0")/templates/datalab-emr-serverless.yaml"
DOCKERFILE="$(dirname "$0")/../docker-images/aws-spark-emr/3.5.8/Dockerfile"
CONTEXT="$(dirname "$0")/.."
PRIM_TOKEN="${PRIM_TOKEN:-REPLACE_ME}"

phase1() {
  echo ">> Phase 1: base infrastructure (no image)"
  aws cloudformation deploy \
    --region "$REGION" \
    --stack-name "$STACK" \
    --template-file "$TEMPLATE" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides "Prefix=$PREFIX" "PrimDatasetToken=$PRIM_TOKEN"
}

build() {
  echo ">> Build & push image"
  local repo
  repo="$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" --output text)"
  aws ecr get-login-password --region "$REGION" \
    | docker login --username AWS --password-stdin "${repo%/*}"
  docker buildx build --platform linux/amd64 \
    -f "$DOCKERFILE" -t "$repo:$TAG" --push "$CONTEXT"
  echo "$repo:$TAG"
}

phase2() {
  echo ">> Phase 2: EMR Serverless app + orchestration"
  local repo
  repo="$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" --output text)"
  aws cloudformation deploy \
    --region "$REGION" \
    --stack-name "$STACK" \
    --template-file "$TEMPLATE" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides "Prefix=$PREFIX" "PrimDatasetToken=$PRIM_TOKEN" "SparkImageUri=$repo:$TAG"
}

case "${1:-all}" in
  phase1) phase1 ;;
  build)  build ;;
  phase2) phase2 ;;
  all)    phase1; build; phase2 ;;
  *) echo "usage: $0 [phase1|build|phase2|all]"; exit 1 ;;
esac
