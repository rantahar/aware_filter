#!/usr/bin/bash
cd /home/rantahj1/aware_filter
source /home/rantahj1/aware_filter/venv/bin/activate
exec /home/rantahj1/aware_filter/venv/bin/gunicorn --config gunicorn.conf.py wsgi:app