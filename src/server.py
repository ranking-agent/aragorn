"""wrapper for aragorn and robokop."""
import os
import logging, warnings

from fastapi import Body, FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from src.aragorn_app import ARAGORN_APP
from src.robokop_app import ROBOKOP_APP
from src.openapi_constructor import construct_open_api_schema

#The app version is now going to be set in ../openapi-config.yaml
#if you want to bump it, do it there.
#Note that it's set once and applies to both robokop and aragorn (b/c they share much of their implementation)
#APP_VERSION = '2.0.24'

APP = FastAPI(title="ARAGORN/ROBOKOP")

# Mount aragorn app at /aragorn
APP.mount('/aragorn',  ARAGORN_APP, 'ARAGORN')
# Mount robokop app at /robokop
APP.mount('/robokop', ROBOKOP_APP, 'ROBOKOP')
# Add all routes of each app for open api generation at /openapi.json
# This will create an aggregate openapi spec.
APP.include_router(ARAGORN_APP.router, prefix='/aragorn')
APP.include_router(ROBOKOP_APP.router, prefix='/robokop')
APP.openapi_schema = construct_open_api_schema(APP, description="ARAGORN/ROBOKOP workflow engine")

# declare app access details
APP.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

