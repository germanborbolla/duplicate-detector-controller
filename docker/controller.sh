#!/usr/bin/env bash

exec java -cp "/controller/libs/*" $JAVA_OPTS com.sumologic.duplicate.detector.controller.Controller
