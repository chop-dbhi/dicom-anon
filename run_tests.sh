#!/bin/bash
PYTHONPATH=PYTHONPATH:`pwd` coverage2 run --include=./dicom_anon.py  tests/tests.py
