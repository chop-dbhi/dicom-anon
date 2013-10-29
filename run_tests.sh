#!/bin/bash
PYTHONPATH=PYTHONPATH:`pwd` coverage run --include=./dicom_anon.py  tests/tests.py
