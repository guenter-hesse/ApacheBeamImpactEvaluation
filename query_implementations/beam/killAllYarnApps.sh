#!/usr/bin/env bash

echo "killing apps"
for app in `yarn application -list | awk '$6 == "RUNNING" { print $1 }'`; do yarn application -kill  "$app";  done
for app in `yarn application -list | awk '$6 == "ACCEPTED" { print $1 }'`; do yarn application -kill  "$app";  done
