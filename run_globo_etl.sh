#!/bin/bash
set -e

# Assuming running on script server under folder called 'oia-la-etls'
WORKING_DIRECTORY=/scripts/sharepoint-etls

echo "Working directory is $WORKING_DIRECTORY"

source $WORKING_DIRECTORY/venv/bin/activate

python -u globo_etl.py
