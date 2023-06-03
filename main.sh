#!/usr/bin/env bash

# export $(egrep -v '^#' .env | xargs)

python src/process_db.py
gunicorn --bind 0.0.0.0:4868 -w 4 -k uvicorn.workers.UvicornWorker -t 60000 src.server:APP
