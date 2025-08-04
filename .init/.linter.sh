#!/bin/bash
cd /home/kavia/workspace/code-generation/poll-engagement-analytics-dashboard-20158/fanengage_analytics_backend
source venv/bin/activate
flake8 .
LINT_EXIT_CODE=$?
if [ $LINT_EXIT_CODE -ne 0 ]; then
  exit 1
fi

