#!/usr/bin/env bash
# Render build step for the Django service.
set -o errexit
pip install -r requirements.txt
python manage.py collectstatic --no-input
python manage.py makemigrations api --no-input
python manage.py migrate --no-input
