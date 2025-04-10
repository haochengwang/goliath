#!/bin/bash

IMAGE_NAME=$1

docker build --no-cache --build-arg BUILD_ARG=build -t ${IMAGE_NAME} .
