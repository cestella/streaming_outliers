#!/bin/sh
STREAMING_ANALYTICS=/root/streaming_analytics-0.1
storm jar $STREAMING_ANALYTICS/lib/storm-0.1.jar com.caseystella.analytics.outlier.Topology "$@"
