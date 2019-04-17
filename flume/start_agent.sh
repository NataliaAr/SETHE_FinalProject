#!/bin/sh
set -e

# EXAMPLE CALL
## sh start_agent.sh /flume_conf Agent1

FLUME_CONF_DIR = $1
FLUME_AGENT_NAME = $2

flume-ng agent --conf ${FLUME_CONF_DIR} --conf-file ${FLUME_CONF_DIR}/flume.conf --name ${FLUME_AGENT_NAME} -Dflume.root.logger=INFO,console