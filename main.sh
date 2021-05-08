#!/usr/bin/env bash

# export $(egrep -v '^#' .env | xargs)

# gunicorn --bind 0.0.0.0:4868 -w 4 -k uvicorn.workers.UvicornWorker -t 600 src.server:APP

uvicorn --host 0.0.0.0 --port 4868 --workers 4 --root-path /1.1 src.server:APP