#!/bin/bash
docker build -t us-central1-docker.pkg.dev/pyrokv/pyrokv/pyrokv-server:${IMAGE_TAG} \
              -t us-central1-docker.pkg.dev/pyrokv/pyrokv/pyrokv-server:${IMAGE_SHA} \
              -t us-central1-docker.pkg.dev/pyrokv/pyrokv/pyrokv-server:latest \
              -t pyrokv:${IMAGE_TAG} \
              -t pyrokv:${IMAGE_SHA} \
              -t pyrokv:latest \
              -f crates/pyrokv-server/Dockerfile .