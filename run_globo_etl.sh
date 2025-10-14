#!/bin/bash
set -e

# Assuming running on script server under folder called 'oia-la-etls'
cd /scripts/oia-la-etls
source venv/bin/activate
python -u globo_etl.py
