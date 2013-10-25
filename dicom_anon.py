#!/usr/bin/env python
#Copyright (c) 2013, The Children's Hospital of Philadelphia
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
#following conditions are met:
#
#1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#   disclaimer.
#
#2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
#   following disclaimer in the documentation and/or other materials provided with the distribution.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
#INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
#WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
#USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sqlite3
import dicom
import difflib
import sys
import os
import shutil
import json
import re
import logging
from datetime import datetime
from optparse import OptionParser
from functools import partial
from dicom.sequence import Sequence 
from dicom.dataset import Dataset

logger = logging.getLogger('anon')
logger.setLevel(logging.INFO)

MEDIA_STORAGE_SOP_INSTANCE_UID = (0x2, 0x3)
STUDY_INSTANCE_UID = (0x20,0xD)
STUDY_DESCR = (0x8, 0x1030)
SERIES_INSTANCE_UID = (0x20,0xE)
SERIES_DESCR = (0x8,0x103E)
SOP_CLASS_UID = (0x8,0x16)
SOP_INSTANCE_UID = (0x8,0x18)
PIXEL_SPACING = (0x28,0x30)
IMAGER_PIXEL_SPACING = (0x18,0x1164)
WINDOW_CENTER = (0x28,0x1050)
WINDOW_WIDTH = (0x28, 0x1051)
CALIBRATION_TYPE =  (0x28,0x402)
CALIBRATION_DESCR = (0x28,0x404)
BURNT_IN = (0x28,0x301)
MODALITY = (0x8,0x60)
IMAGE_TYPE = (0x8,0x8)
MANUFACTURER = (0x8,0x70)
MANUFACTURER_MODEL_NAME = (0x8,0x1090)

audit = None
db = None

TABLE_EXISTS = 'SELECT name FROM sqlite_master WHERE name=?'
CREATE_REGULAR_TABLE = 'CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, original, cleaned)'
CREATE_LINKED_TABLE = 'CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, original, cleaned, study INTEGER, FOREIGN KEY(study) REFERENCES studyinstanceuid(id))'
INSERT_OTHER = 'INSERT INTO %s (original, cleaned) VALUES (?, ?)'
INSERT_LINKED = 'INSERT INTO %s (original, cleaned, study) VALUES (?, ?, ?)'
GET_OTHER = 'SELECT cleaned FROM %s WHERE original = ?'
GET_DATE = 'SELECT cleaned FROM %s WHERE original = ? AND study = ?'
STUDY_PK = 'SELECT id FROM studyinstanceuid WHERE cleaned = ?'
NEXT_ID = 'SELECT max(id) FROM %s'

CLEANED_DATE = '20000101'
CLEANED_TIME = '000000.00'

ALLOWED_FILE_META = {
  (0x2, 0x0):1, # File Meta Information Group Length
  (0x2, 0x1):1, # Version
  (0x2, 0x2):1, # Media Storage SOP Class UID
  (0x2, 0x3):1, # Media Storage SOP Instance UID
  (0x2, 0x10):1,# Transfer Syntax UID
  (0x2, 0x12):1,# Implementation Class UID
  (0x2, 0x13):1 # Implementation Version Name
}
# Attributes taken from https://github.com/dicom/ruby-dicom

AUDIT = { 
    STUDY_INSTANCE_UID:1,
    SERIES_INSTANCE_UID:1,
    SOP_INSTANCE_UID:1,
    (0x8,0x20): 1, # Study Date
    (0x8,0x50):1, # Accession Number - Z BALC
    (0x8,0x80):1, # Institution name
    (0x8,0x81):1, # Institution Address
    (0x8,0x90):1, # Referring Physician's name
    (0x8,0x92):1, # Referring Physician's address
    (0x8,0x94):1, # Referring Physician's Phone
    (0x8,0x1048):1, # Physician(s) of Record
    (0x8,0x1049):1, # Physician(s) of Record Identification
    (0x8,0x1050):1, # Performing Physician's Name
    (0x8,0x1060):1, # Reading Physicians Name
    (0x8,0x1070):1, # Operator's Name
    (0x8,0x1010):1, # Station name
    (0x10,0x10):1, # Patient's name
    (0x10,0x1005):1, # Patient's Birth Name
    (0x10,0x20):1, # Patient's ID
}

ANNEX_E = {
    (0x0008,0x0050): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Accession Number
    (0x0018,0x4000): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Acquisition Comments
    (0x0040,0x0555): ['N', 'Y', 'X', '', '', '', '', '', '', '', 'C'], # Acquisition Context Sequence
    (0x0008,0x0022): ['N', 'Y', 'X/Z', '', '', '', '', 'K', 'C', '', ''], # Acquisition Date
    (0x0008,0x002A): ['N', 'Y', 'X/D', '', '', '', '', 'K', 'C', '', ''], # Acquisition DateTime
    (0x0018,0x1400): ['N', 'Y', 'X/D', '', '', '', '', '', '', 'C', ''], # Acquisition Device Processing Description
    (0x0018,0x9424): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Acquisition Protocol Description
    (0x0008,0x0032): ['N', 'Y', 'X/Z', '', '', '', '', 'K', 'C', '', ''], # Acquisition Time
    (0x0040,0x4035): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Actual Human Performers Sequence
    (0x0010,0x21B0): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Additional Patient's History
    (0x0038,0x0010): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Admission ID
    (0x0038,0x0020): ['N', 'N', 'X', '', '', '', '', 'K', 'C', '', ''], # Admitting Date
    (0x0008,0x1084): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Admitting Diagnoses Code Sequence
    (0x0008,0x1080): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Admitting Diagnoses Description
    (0x0038,0x0021): ['N', 'N', 'X', '', '', '', '', 'K', 'C', '', ''], # Admitting Time
    (0x0000,0x1000): ['N', 'N', 'X', '', 'K', '', '', '', '', '', ''], # Affected SOP Instance UID
    (0x0010,0x2110): ['N', 'N', 'X', '', '', '', 'C', '', '', 'C', ''], # Allergies
    (0x4000,0x0010): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Arbitrary
    (0x0040,0xA078): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Author Observer Sequence
    (0x0010,0x1081): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Branch of Service
    (0x0018,0x1007): ['N', 'Y', 'X', '', '', 'K', '', '', '', '', ''], # Cassette ID
    (0x0040,0x0280): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Comments on Performed Procedure Step
    (0x0020,0x9161): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Concatenation UID
    (0x0040,0x3001): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Confidentiality Constraint on Patient Data Description
    (0x0070,0x0084): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Content Creator's Name
    (0x0070,0x0086): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Content Creator's Identification Code Sequence
    (0x0008,0x0023): ['N', 'Y', 'Z/D', '', '', '', '', 'K', 'C', '', ''], # Content Date
    (0x0040,0xA730): ['N', 'Y', 'X', '', '', '', '', '', '', '', 'C'], # Content Sequence
    (0x0008,0x0033): ['N', 'Y', 'Z/D', '', '', '', '', 'K', 'C', '', ''], # Content Time
    (0x0008,0x010D): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Context Group Extension Creator UID
    (0x0018,0x0010): ['N', 'Y', 'Z/D', '', '', '', '', '', '', 'C', ''], # Contrast Bolus Agent
    (0x0018,0xA003): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Contribution Description
    (0x0010,0x2150): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Country of Residence
    (0x0008,0x9123): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Creator Version UID
    (0x0038,0x0300): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Current Patient Location
#    (0x50xx,0xxxxx): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Curve Data
    (0x0008,0x0025): ['Y', 'Y', 'X', '', '', '', '', 'K', 'C', '', ''], # Curve Date
    (0x0008,0x0035): ['Y', 'Y', 'X', '', '', '', '', 'K', 'C', '', ''], # Curve Time
    (0x0040,0xA07C): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Custodial Organization Sequence
    (0xFFFC,0xFFFC): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Data Set Trailing Padding
    (0x0008,0x2111): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Derivation Description
    (0x0018,0x700A): ['N', 'Y', 'X', '', '', 'K', '', '', '', '', ''], # Detector ID
    (0x0018,0x1000): ['N', 'Y', 'X/Z/D', '', '', 'K', '', '', '', '', ''], # Device Serial Number
    (0x0018,0x1002): ['N', 'Y', 'U', '', 'K', 'K', '', '', '', '', ''], # Device UID
    (0x0400,0x0100): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Digital Signature UID
    (0xFFFA,0xFFFA): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Digital Signatures Sequence
    (0x0020,0x9164): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Dimension Organization UID
    (0x0038,0x0040): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Discharge Diagnosis Description
    (0x4008,0x011A): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Distribution Address
    (0x4008,0x0119): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Distribution Name
    (0x300A,0x0013): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Dose Reference UID
    (0x0010,0x2160): ['N', 'Y', 'X', '', '', '', 'K', '', '', '', ''], # Ethnic Group
    (0x0008,0x0058): ['N', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Failed SOP Instance UID List
    (0x0070,0x031A): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Fiducial UID
    (0x0040,0x2017): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Filler Order Number of Imaging Service Request
    (0x0020,0x9158): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Frame Comments
    (0x0020,0x0052): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Frame of Reference UID
    (0x0018,0x1008): ['N', 'Y', 'X', '', '', 'K', '', '', '', '', ''], # Gantry ID
    (0x0018,0x1005): ['N', 'Y', 'X', '', '', 'K', '', '', '', '', ''], # Generator ID
    (0x0070,0x0001): ['N', 'Y', 'D', '', '', '', '', '', '', '', ''], # Graphic Annotation Sequence
    (0x0040,0x4037): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Human Performers Name
    (0x0040,0x4036): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Human Performers Organization
    (0x0088,0x0200): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Icon Image Sequence
    (0x0008,0x4000): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Identifying Comments
    (0x0020,0x4000): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Image Comments
    (0x0028,0x4000): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Image Presentation Comments
    (0x0040,0x2400): ['N', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Imaging Service Request Comments
    (0x4008,0x0300): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Impressions
    (0x0008,0x0014): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Instance Creator UID
    (0x0008,0x0081): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Institution Address
    (0x0008,0x0082): ['N', 'Y', 'X/Z/D', '', '', '', '', '', '', '', ''], # Institution Code Sequence
    (0x0008,0x0080): ['N', 'Y', 'X/Z/D', '', '', '', '', '', '', '', ''], # Institution Name
    (0x0008,0x1040): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Institutional Department Name
    (0x0010,0x1050): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Insurance Plan Identification
    (0x0040,0x1011): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Intended Recipients of Results Identification Sequence
    (0x4008,0x0111): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Interpretation Approver Sequence
    (0x4008,0x010C): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Interpretation Author
    (0x4008,0x0115): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Interpretation Diagnosis Description
    (0x4008,0x0202): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Interpretation ID Issuer
    (0x4008,0x0102): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Interpretation Recorder
    (0x4008,0x010B): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Interpretation Text
    (0x4008,0x010A): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Interpretation Transcriber
    (0x0008,0x3010): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Irradiation Event UID
    (0x0038,0x0011): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Issuer of Admission ID
    (0x0010,0x0021): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Issuer of Patient ID
    (0x0038,0x0061): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Issuer of Service Episode ID
    (0x0028,0x1214): ['Y', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Large Palette Color Lookup Table UID
    (0x0010,0x21D0): ['N', 'N', 'X', '', '', '', '', 'K', 'C', '', ''], # Last Menstrual Date
    (0x0400,0x0404): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # MAC
    (0x0002,0x0003): ['N', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Media Storage SOP Instance UID
    (0x0010,0x2000): ['N', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Medical Alerts
    (0x0010,0x1090): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Medical Record Locator
    (0x0010,0x1080): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Military Rank
    (0x0400,0x0550): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Modified Attributes Sequence
    (0x0020,0x3406): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Modified Image Description
    (0x0020,0x3401): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Modifying Device ID
    (0x0020,0x3404): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Modifying Device Manufacturer
    (0x0008,0x1060): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Name of Physician(s) Reading Study
    (0x0040,0x1010): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Names of Intended Recipient of Results
    (0x0010,0x2180): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Occupation
    (0x0008,0x1072): ['N', 'Y', 'X/D', '', '', '', '', '', '', '', ''], # Operators' Identification Sequence
    (0x0008,0x1070): ['N', 'Y', 'X/Z/D', '', '', '', '', '', '', '', ''], # Operators' Name
    (0x0400,0x0561): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Original Attributes Sequence
    (0x0040,0x2010): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Order Callback Phone Number
    (0x0040,0x2008): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Order Entered By
    (0x0040,0x2009): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Order Enterer Location
    (0x0010,0x1000): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Other Patient IDs
    (0x0010,0x1002): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Other Patient IDs Sequence
    (0x0010,0x1001): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Other Patient Names
#    (0x60xx,0x4000): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Overlay Comments
#    (0x60xx,0x3000): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Overlay Data
    (0x0008,0x0024): ['Y', 'Y', 'X', '', '', '', '', 'K', 'C', '', ''], # Overlay Date
    (0x0008,0x0034): ['Y', 'Y', 'X', '', '', '', '', 'K', 'C', '', ''], # Overlay Time
    (0x0028,0x1199): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Palette Color Lookup Table UID
    (0x0040,0xA07A): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Participant Sequence
    (0x0010,0x1040): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Patient Address
    (0x0010,0x4000): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Patient Comments
    (0x0010,0x0020): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Patient ID
    (0x0010,0x2203): ['N', 'Y', 'X/Z', '', '', '', 'K', '', '', '', ''], # Patient Sex Neutered
    (0x0038,0x0500): ['N', 'N', 'X', '', '', '', 'C', '', '', 'C', ''], # Patient State
    (0x0040,0x1004): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Patient Transport Arrangements
    (0x0010,0x1010): ['N', 'Y', 'X', '', '', '', 'K', '', '', '', ''], # Patient's Age
    (0x0010,0x0030): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Patient's Birth Date
    (0x0010,0x1005): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Patient's Birth Name
    (0x0010,0x0032): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Patient's Birth Time
    (0x0038,0x0400): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Patient's Institution Residence
    (0x0010,0x0050): ['', '', 'X', '', '', '', '', '', '', '', ''], # Patient's Insurance Plan Code Sequence
    (0x0010,0x1060): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Patient's Mother's Birth Name
    (0x0010,0x0010): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Patient's Name
    (0x0010,0x0101): ['', '', 'X', '', '', '', '', '', '', '', ''], # Patient's Primary Language Code Sequence
    (0x0010,0x0102): ['', '', 'X', '', '', '', '', '', '', '', ''], # Patient's Primary Language Modifier Code Sequence
    (0x0010,0x21F0): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Patient's Religious Preference
    (0x0010,0x0040): ['N', 'Y', 'Z', '', '', '', 'K', '', '', '', ''], # Patient's Sex
    (0x0010,0x1020): ['N', 'Y', 'X', '', '', '', 'K', '', '', '', ''], # Patient's Size
    (0x0010,0x2154): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Patient's Telephone Number
    (0x0010,0x1030): ['N', 'Y', 'X', '', '', '', 'K', '', '', '', ''], # Patient's Weight
    (0x0040,0x0243): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Performed Location
    (0x0040,0x0254): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Performed Procedure Step Description
    (0x0040,0x0253): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Performed Procedure Step ID
    (0x0040,0x0244): ['N', 'Y', 'X', '', '', '', '', 'K', 'C', '', ''], # Performed Procedure Step Start Date
    (0x0040,0x0245): ['N', 'Y', 'X', '', '', '', '', 'K', 'C', '', ''], # Performed Procedure Step Start Time
    (0x0040,0x0241): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Performed Station AE Title
    (0x0040,0x4030): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Performed Station Geographic Location Code Sequence
    (0x0040,0x0242): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Performed Station Name
    (0x0040,0x0248): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Performed Station Name Code Sequence
    (0x0008,0x1052): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Performing Physicians' Identification Sequence
    (0x0008,0x1050): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Performing Physicians' Name
    (0x0040,0x1102): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Person Address
    (0x0040,0x1101): ['N', 'Y', 'D', '', '', '', '', '', '', '', ''], # Person Identification Code Sequence
    (0x0040,0xA123): ['N', 'Y', 'D', '', '', '', '', '', '', '', ''], # Person Name
    (0x0040,0x1103): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Person Telephone Numbers
    (0x4008,0x0114): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Physician Approving Interpretation
    (0x0008,0x1062): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Physician Reading Study Identification Sequence
    (0x0008,0x1048): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Physician(s) of Record
    (0x0008,0x1049): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Physician(s) of Record Identification Sequence
    (0x0040,0x2016): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Placer Order Number of Imaging Service Request
    (0x0018,0x1004): ['N', 'Y', 'X', '', '', 'K', '', '', '', '', ''], # Plate ID
    (0x0040,0x0012): ['N', 'N', 'X', '', '', '', 'C', '', '', '', ''], # Pre-Medication
    (0x0010,0x21C0): ['N', 'N', 'X', '', '', '', 'K', '', '', '', ''], # Pregnancy Status
    (0x0018,0x1030): ['N', 'Y', 'X/D', '', '', '', '', '', '', 'C', ''], # Protocol Name
    (0x0040,0x2001): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Reason for Imaging Service Request
    (0x0032,0x1030): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Reason for Study
    (0x0400,0x0402): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Referenced Digital Signature Sequence
    (0x3006,0x0024): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Referenced Frame of Reference UID
    (0x0040,0x4023): ['N', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Referenced General Purpose Scheduled Procedure Step Transaction UID
    (0x0008,0x1140): ['N', 'Y', 'X/Z/U*', '', 'K', '', '', '', '', '', ''], # Referenced Image Sequence
    (0x0038,0x1234): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Referenced Patient Alias Sequence
    (0x0008,0x1120): ['N', 'Y', 'X', '', 'X', '', '', '', '', '', ''], # Referenced Patient Sequence
    (0x0008,0x1111): ['N', 'Y', 'X/Z/D', '', 'K', '', '', '', '', '', ''], # Referenced Performed Procedure Step Sequence
    (0x0400,0x0403): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Referenced SOP Instance MAC Sequence
    (0x0008,0x1155): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Referenced SOP Instance UID
    (0x0004,0x1511): ['N', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Referenced SOP Instance UID in File
    (0x0008,0x1110): ['N', 'Y', 'X/Z', '', 'K', '', '', '', '', '', ''], # Referenced Study Sequence
    (0x0008,0x0092): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Referring Physician's Address
    (0x0008,0x0096): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Referring Physician's Identification Sequence
    (0x0008,0x0090): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Referring Physician's Name
    (0x0008,0x0094): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Referring Physician's Telephone Numbers
    (0x0010,0x2152): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Region of Residence
    (0x3006,0x00C2): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Related Frame of Reference UID
    (0x0040,0x0275): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Request Attributes Sequence
    (0x0032,0x1070): ['N', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Requested Contrast Agent
    (0x0040,0x1400): ['N', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Requested Procedure Comments
    (0x0032,0x1060): ['N', 'Y', 'X/Z', '', '', '', '', '', '', 'C', ''], # Requested Procedure Description
    (0x0040,0x1001): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Requested Procedure ID
    (0x0040,0x1005): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Requested Procedure Location
    (0x0000,0x1001): ['N', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Requested SOP Instance UID
    (0x0032,0x1032): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Requesting Physician
    (0x0032,0x1033): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Requesting Service
    (0x0010,0x2299): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Responsible Organization
    (0x0010,0x2297): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Responsible Person
    (0x4008,0x4000): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Results Comments
    (0x4008,0x0118): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Results Distribution List Sequence
    (0x4008,0x0042): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Results ID Issuer
    (0x300E,0x0008): ['N', 'Y', 'X/Z', '', '', '', '', '', '', '', ''], # Reviewer Name
    (0x0040,0x4034): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Scheduled Human Performers Sequence
    (0x0038,0x001E): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Scheduled Patient Institution Residence
    (0x0040,0x000B): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Scheduled Performing Physician Identification Sequence
    (0x0040,0x0006): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Scheduled Performing Physician Name
    (0x0040,0x0004): ['N', 'N', 'X', '', '', '', '', 'K', 'C', '', ''], # Scheduled Procedure Step End Date
    (0x0040,0x0005): ['N', 'N', 'X', '', '', '', '', 'K', 'C', '', ''], # Scheduled Procedure Step End Time
    (0x0040,0x0007): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Scheduled Procedure Step Description
    (0x0040,0x0011): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Scheduled Procedure Step Location
    (0x0040,0x0002): ['N', 'N', 'X', '', '', '', '', 'K', 'C', '', ''], # Scheduled Procedure Step Start Date
    (0x0040,0x0003): ['N', 'N', 'X', '', '', '', '', 'K', 'C', '', ''], # Scheduled Procedure Step Start Time
    (0x0040,0x0001): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Scheduled Station AE Title
    (0x0040,0x4027): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Scheduled Station Geographic Location Code Sequence
    (0x0040,0x0010): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Scheduled Station Name
    (0x0040,0x4025): ['N', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Scheduled Station Name Code Sequence
    (0x0032,0x1020): ['Y', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Scheduled Study Location
    (0x0032,0x1021): ['Y', 'N', 'X', '', '', 'K', '', '', '', '', ''], # Scheduled Study Location AE Title
    (0x0008,0x0021): ['N', 'Y', 'X/D', '', '', '', '', 'K', 'C', '', ''], # Series Date
    (0x0008,0x103E): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Series Description
    (0x0020,0x000E): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Series Instance UID
    (0x0008,0x0031): ['N', 'Y', 'X/D', '', '', '', '', 'K', 'C', '', ''], # Series Time
    (0x0038,0x0062): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Service Episode Description
    (0x0038,0x0060): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Service Episode ID
    (0x0010,0x21A0): ['N', 'N', 'X', '', '', '', 'K', '', '', '', ''], # Smoking Status
    (0x0008,0x0018): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # SOP Instance UID
    (0x0008,0x2112): ['N', 'Y', 'X/Z/U*', '', 'K', '', '', '', '', '', ''], # Source Image Sequence
    (0x0038,0x0050): ['N', 'N', 'X', '', '', '', 'C', '', '', '', ''], # Special Needs
    (0x0008,0x1010): ['N', 'Y', 'X/Z/D', '', '', 'K', '', '', '', '', ''], # Station Name
    (0x0088,0x0140): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Storage Media File-set UID
    (0x0032,0x4000): ['Y', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Study Comments
    (0x0008,0x0020): ['N', 'Y', 'Z', '', '', '', '', 'K', 'C', '', ''], # Study Date
    (0x0008,0x1030): ['N', 'Y', 'X', '', '', '', '', '', '', 'C', ''], # Study Description
    (0x0020,0x0010): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Study ID
    (0x0032,0x0012): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Study ID Issuer
    (0x0020,0x000D): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Study Instance UID
    (0x0008,0x0030): ['N', 'Y', 'Z', '', '', '', '', 'K', 'C', '', ''], # Study Time
    (0x0020,0x0200): ['N', 'Y', 'U', '', 'K', '', '', '', '', '', ''], # Synchronization Frame of Reference UID
    (0x0040,0xDB0D): ['Y', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Template Extension Creator UID
    (0x0040,0xDB0C): ['Y', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Template Extension Organization UID
    (0x4000,0x4000): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Text Comments
    (0x2030,0x0020): ['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Text String
    (0x0008,0x0201): ['N', 'Y', 'X', '', '', '', '', 'K', 'C', '', ''], # Timezone Offset From UTC
    (0x0088,0x0910): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Topic Author
    (0x0088,0x0912): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Topic Key Words
    (0x0088,0x0906): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Topic Subject
    (0x0088,0x0904): ['Y', 'N', 'X', '', '', '', '', '', '', '', ''], # Topic Title
    (0x0008,0x1195): ['N', 'N', 'U', '', 'K', '', '', '', '', '', ''], # Transaction UID
    (0x0040,0xA088): ['N', 'Y', 'Z', '', '', '', '', '', '', '', ''], # Verifying Observer Identification Code Sequence
    (0x0040,0xA075): ['N', 'Y', 'D', '', '', '', '', '', '', '', ''], # Verifying Observer Name
    (0x0040,0xA073): ['N', 'Y', 'D', '', '', '', '', '', '', '', ''], # Verifying Observer Sequence
    (0x0040,0xA027): ['N', 'Y', 'X', '', '', '', '', '', '', '', ''], # Verifying Organization
    (0x0038,0x4000): ['N', 'N', 'X', '', '', '', '', '', '', 'C', ''], # Visit Comments
    
    # The following are additions to the standard
    (0x8,0x1150):['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Referenced SOP Class UID Sequence
    (0x18,0x1200):['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Date of Last Calibration
    (0x18,0x1201):['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Time of Last Calibration
    (0x32,0x1064):['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Requested Procedure Sequence
    (0x40,0x1012):['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Reason for peformed procedure sequence
    (0x40,0xA124):['N', 'N', 'X', '', '', '', '', '', '', '', ''], # UID
    (0x72,0x6A):['N', 'N', 'X', '', '', '', '', '', '', '', ''], # Selector PN Value
    (0x3006,0xA6):['N', 'N', 'X', '', '', '', '', '', '', '', ''], # ROI Interpreter
}

# Return true if file should be quarantined
def quarantine(ds, allowed_modalities):
    if SERIES_DESCR in ds and ds[SERIES_DESCR].value != None:
        series_desc = ds[SERIES_DESCR].value.strip().lower()
        if 'patient protocol' in series_desc:
            return (True, 'patient protocol')
        elif 'save' in series_desc: # from link in comment below
            return (True, 'Likely screen capture')

    if MODALITY in ds:
        modality = ds[MODALITY]
        if modality.VM == 1:
            modality = [modality.value]
        for m in modality:
            if m == None or not m.lower() in allowed_modalities:
                return (True, 'modality not allowed')

    if MODALITY not in  ds:
        return (True, 'Modality missing')

    if BURNT_IN in ds and ds[BURNT_IN].value != None:
        burnt_in = ds[BURNT_IN].value
        if burnt_in.strip().lower() in ['yes', 'y']:
            return (True, 'burnt-in data')
    # The following were taken from https://wiki.cancerimagingarchive.net/download/attachments/3539047/pixel-checker-filter.script?version=1&modificationDate=1333114118541&api=v2
    if IMAGE_TYPE in ds:
        image_type = ds[IMAGE_TYPE]
        if image_type.VM == 1:
            image_type = [image_type.value]
        for i in image_type:
            if i != None and 'save' in i.strip().lower():
                return(True, 'Likely screen capture')

    if MANUFACTURER in ds:
        manufacturer = ds[MANUFACTURER].value.strip().lower()
        if 'north american imaging, inc' in manufacturer or 'pacsgear' in manufacturer:
            return(True, 'Manufacturer is suspect')

    if MANUFACTURER_MODEL_NAME in ds:
        model_name = ds[MANUFACTURER_MODEL_NAME].value.strip().lower()
        if "the DiCOM box" in model_name:
            return(True, "Manufacturer model name is suspect")     
    return (False, '')

# Determines destination of cleaned/quarantined file based on 
# source folder
def destination(source, dest, root):
    if not dest.endswith(os.path.sep):
        dest += os.path.sep

    if not root.endswith(os.path.sep):
        root += os.path.sep

    if dest.startswith(root):
        raise Exception('Destination directory cannot be inside'
            'or equal to source directory')

    if not source.startswith(root):
        raise Exception('The file to be moved must be in the root directory')

    s = difflib.SequenceMatcher(a=root, b=source)
    m = s.find_longest_match(0, len(root), 0, len(source))
    if not (m.a == m.b == 0):
        raise Exception('Unexpected file paths: source and root share no'
            ' common path.')

    sub_path = os.path.dirname(source)[m.size:]

    destination_dir = os.path.join(dest, sub_path)
    return destination_dir

def get_next_pk(tag):
    if not table_exists(table_name(tag)):
        return 1
    audit.execute(NEXT_ID % table_name(tag))
    results = audit.fetchall()
    if results[0][0]:
        return int(results[0][0]+1)
    else:
        return 1

def generate_uid(org_root):
    n = datetime.now()
    while True:
        new_guid = '%s.%s.%s.%s.%s.%s.%s' % (org_root, n.year, n.month, n.day, n.minute, n.second, n.microsecond) 
        if new_guid != generate_uid.last:
            generate_uid.last = new_guid
            break
    return new_guid
generate_uid.last = None


def enforce_profile(ds, e, study_pk, profile, white_list, org_root):
    white_listed = False
    cleaned = None
    if profile == 'clean':
        # If it's list in the ANNEX, we need to specifically be able to clean it
        if (e.tag in ANNEX_E and ANNEX_E[(e.tag.group, e.tag.element)][9]=='C') or not (e.tag in ANNEX_E.keys()):
            white_listed = white_list_handler(ds, e, white_list)
            if not white_listed:
                cleaned = basic(ds, e, study_pk)
        else:
            basic(ds, e, study_pk)
    else:
        # We are assuming basic
        # TODO need to revisit this because for sequences, X might mean to remove the whole sequence, not
        # dive into it and clean it
        cleaned = basic(ds, e, study_pk, org_root)

    if e.tag in AUDIT.keys():
       if cleaned != None and not (e.tag == STUDY_INSTANCE_UID):
           audit_save(e, e.value, cleaned, study_uid_pk=study_pk)
    if cleaned != None and e.tag in ds:
       ds[e.tag].value = cleaned

    # Tell our caller if we cleaned this element
    if e.tag in ANNEX_E.keys() or white_listed:
        return True
    return False

def basic(ds, e, study_pk, org_root):
    cleaned = audit_get(e, study_uid_pk=study_pk)
    if cleaned != None:
        return cleaned
    if e.tag in ANNEX_E.keys():
        rule = ANNEX_E[(e.tag.group, e.tag.element)][2][0] # For now we aren't going to worry about IOD type conformance, just do the first option
        print "%s %s" % (rule, e.name)
        if rule == 'D':
            cleaned = replace_vr_d(e, org_root)
        if rule == 'Z':
            cleaned = replace_vr_d(e, org_root)
        if rule == 'X':
            del ds[e.tag]
            cleaned = 'Removed by dicom-anon'
        if rule == 'K':
            pass
        if rule == 'U':
            cleaned = generate_uid(org_root)
    return cleaned
    

def replace_vr_z(e):
    if e.VR == 'DT':
        cleaned = CLEANED_TIME
    elif e.VR == 'DA':
        cleaned = CLEANED_DATE
    else:
        cleaned = ""
    return cleaned

def replace_vr_d(e, org_root):
    if e.VR == 'DT':
        cleaned = CLEANED_TIME
    elif e.VR == 'DA':
        cleaned = CLEANED_DATE
    elif e.VR == 'TM':
        cleaned = CLEANED_TIME
    elif e.VR == 'UI':
        cleaned = generate_uid(org_root)
    else:
        if e.name and len(e.name):
            cleaned = ('%s %d' % (e.name, get_next_pk(e))).encode('ascii')
        else:
            cleaned = ''
    return cleaned

def delete_handler(ds, e):
    if e.tag in ATTRIBUTES['delete'].keys():
        del ds[e.tag]
        return True
    return False

def replace_handler(ds, e):
    if e.tag in ATTRIBUTES['replace'].keys():
       ds[e.tag].value = str(ATTRIBUTES["replace"][(e.tag.group,e.tag.element)])
       return True
    return False

def vr_handler(ds, e):
    if e.VR in ['PN', 'UI', 'DA', 'DT', 'LT', 'UN', 'UT', 'ST', 'AE', 'LO', 'TM']:
        del ds[e.tag]
        return True
    return False

# Remove group 0x1000 which contains personal
# information
def personal_handler(ds,e):
    if e.tag.group == 0x1000:
        del ds[e.tag]
        return True
    return False

# Curve data is (0x50xx,0xxxxx)
def curve_data_handler(ds, e):
    if hex((e.tag.group)/0xFF) == 0x50:
        del ds[e.tag]
        return True
    return False

# Overlay comment is (0x60xx,0x4000) 
def overlay_comment_handler(ds, e):
    if hex((e.tag.group)/0xFF) == 0x60 and e.tag.element == 0x4000:
        del ds[e.tag]
        return True
    return False

# Overy data is and (0x60xx,0x3000)   
def overlay_data_handler(ds, e):
    if hex((e.tag.group)/0xFF) == 0x60 and e.tag.element == 0x3000:
        del ds[e.tag]
        return True
    return False

def white_list_handler(ds, e):
    if white_list.get((e.tag.group, e.tag.element), None):
        if not e.value.lower().strip() in white_list[(e.tag.group, e.tag.element)]:
            logger.info('"%s" not in white list for %s' % (e.value, e.name))
            return False
        return True
    return False

def clean_cb(ds, e, study_pk, profile="basic", org_root=None, white_list=None, overlay=False):
    done = enforce_profile(ds, e, study_pk, profile=profile, org_root=org_root, white_list=white_list)
    if done:
        return
    
    done = vr_handler(ds, e)
    if done: 
        return
    if not overlay:
        done = overlay_data_handler(ds, e)
        if done:
            return
    
    done = overlay_comment_handler(ds, e)
    if done:
        return
    
    done = curve_data_handler(ds, e)
    if done:
        return
        
    personal_handler(ds, e)

def convert_json_white_list(h):
    value = {}
    for tag in h.keys():
        a, b = tag.split(',')
        t = (int(a,16), int(b,16))
        value[t]=[re.sub(' +', ' ', x.lower().strip()) for x in h[tag]]
    return value

def clean_meta(ds, e):
    if ALLOWED_FILE_META.get((e.tag.group, e.tag.element), None):
        return
    else:
        del ds[e.tag]

def anonymize(ds, white_list, org_root, profile, overlay):
    # anonymize study_uid, save off id
    cleaned_study_uid = audit_get(ds[STUDY_INSTANCE_UID])
    if cleaned_study_uid == None:
        cleaned_study_uid = generate_uid(org_root)
        audit_save(ds[STUDY_INSTANCE_UID], ds[STUDY_INSTANCE_UID].value, cleaned_study_uid)

    # Get pk of study_uid
    study_pk = audit_get_study_pk(cleaned_study_uid)

    ds.remove_private_tags()

    # Walk entire file
    ds.walk(partial(clean_cb, study_pk=study_pk, org_root=org_root, white_list=white_list, profile=profile, overlay=overlay))

    # Fix file meta data portion
    if MEDIA_STORAGE_SOP_INSTANCE_UID in ds.file_meta:
        ds.file_meta[MEDIA_STORAGE_SOP_INSTANCE_UID].value = ds[SOP_INSTANCE_UID].value
    ds.file_meta.walk(clean_meta)
    return ds

def driver(ident_dir, clean_dir, quarantine_dir='quarantine', audit_file='identity.db', allowed_modalities=['mr','ct'], 
        org_root='5.555.5', white_list_file = None, log_file=None, rename=False, profile="basic", overlay = False):
    
    white_list = None

    logger.handlers = []
    if not log_file:
        h = logging.StreamHandler() 
    else:
        h = logging.FileHandler(log_file)

    h.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    h.setFormatter(formatter)
    logger.addHandler(h)
    
    if white_list_file != None:
        try:
            w = open(white_list_file, 'r')
            white_list = w.read()
            white_list = json.loads(white_list)
            w.close()
            white_list = convert_json_white_list(white_list)
        except IOError, e:
            logger.error('Error opening white list file.')
            return False

    open_audit(audit_file) 

    for root, dirs, files in os.walk(ident_dir):
         for filename in files:
             if filename.startswith('.'):
                 continue
             try:
                 ds = dicom.read_file(os.path.join(root,filename))
             except IOError:
                 logger.error('Error reading file %s' % os.path.join(root,
                     filename))
                 db.close()
                 return False

             move, reason = quarantine(ds, allowed_modalities)
             if move:
                 full_quarantine_dir = destination(os.path.join(root, filename), quarantine_dir, ident_dir)
                 if not os.path.exists(full_quarantine_dir):
                       os.makedirs(full_quarantine_dir)
                 quarantine_name = os.path.join(full_quarantine_dir, filename)
                 logger.info('"%s" will be moved to quarantine directory due to: %s' % (os.path.join(root, filename), reason))
                 shutil.copyfile(os.path.join(root, filename), quarantine_name)
                 continue

             destination_dir = destination(os.path.join(root, filename), clean_dir, ident_dir)
             if not os.path.exists(destination_dir):
                 os.makedirs(destination_dir)
             ds = anonymize(ds, white_list, org_root, profile, overlay)
             
             # Set Patient Identity Removed to YES
             t = dicom.tag.Tag((0x12,0x62))
             ds[t] = dicom.dataelem.DataElement(t,"CS","YES")
             
             # Set the De-identification method code sequene
             method_ds = Dataset()
             t = dicom.tag.Tag((0x8, 0x102))
             if (profile == "clean"):
                 method_ds[t] = dicom.dataelem.DataElement(t, "DS", dicom.multival.MultiValue(dicom.valuerep.DS, ["113100", "113105"]))
             else:
                 method_ds[t] = dicom.dataelem.DataElement(t, "DS", dicom.multival.MultiValue(dicom.valuerep.DS, ["113100"]))
             t = dicom.tag.Tag((0x12, 0x64))
             ds[t] = dicom.dataelem.DataElement(t, "SQ", Sequence([method_ds]))
             
             print ds
             if rename:
                 clean_name = os.path.join(destination_dir, ds[SOP_INSTANCE_UID].value)
             else:
                 clean_name = os.path.join(destination_dir, filename)
             try:
                 ds.save_as(clean_name)
             except IOError:
                 logger.error('Error writing file "%s"' % clean_name)
                 db.close()
                 return False
    db.close()
    return True

# SQLite audit trail functions
def open_audit(identity):
    global db, audit
    bootstrap = False
    if not os.path.isfile(identity):
       bootstrap = True
    db = sqlite3.connect(identity)
    audit = db.cursor()
    if bootstrap:
        # create the table that holds the studyintance because others will refer to it
        audit.execute(CREATE_REGULAR_TABLE % 'studyinstanceuid')
        db.commit()

def table_exists(table):
    audit.execute(TABLE_EXISTS, (table,))
    results = audit.fetchall()
    return len(results) > 0

def table_name(tag):
    return re.sub('\W+', '', tag.name.lower())

def audit_get_study_pk(cleaned):
    audit.execute(STUDY_PK, (cleaned,))
    results = audit.fetchall()
    return results[0][0]

def audit_get(tag, study_uid_pk=None):
    value = None
    original = tag.value
    if not table_exists(table_name(tag)):
        return value
        
    if tag.name.lower() == 'study instance uid':
        audit.execute(GET_OTHER % table_name(tag), (original,))
        results  = audit.fetchall()
        if len(results):
            value = results[0][0]
    else:
        audit.execute(GET_DATE % table_name(tag), (original, study_uid_pk))
        results  = audit.fetchall()
        if len(results):
            value = results[0][0]

    return value

def audit_save(tag, original, cleaned, study_uid_pk=None):
    if not table_exists(table_name(tag)):
        if tag.name.lower() == 'study instance uid':
            audit.execute(CREATE_REGULAR_TABLE % table_name(tag))
        else:
            audit.execute(CREATE_LINKED_TABLE % table_name(tag))
        db.commit()

    # Table exists
    if tag.name.lower() == 'study instance uid':
        audit.execute(INSERT_OTHER % table_name(tag), (original, cleaned))
    else:
        audit.execute(INSERT_LINKED % table_name(tag), (original, cleaned, study_uid_pk))
    db.commit()

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-q", "--quarantine", default="quarantine", dest="quarantine", action="store",
            help="Quarantine directory")

    parser.add_option("-w", "--white_list", default=None, dest="white_list", action="store",
            help="White list json file")

    parser.add_option("-a", "--audit_file", default="identity.db", dest="audit", action="store",
            help="Name of sqlite audit file")

    parser.add_option("-m", "--modalities", default="mr,ct", dest = "modalities", action="store",
            help="Comma separated list of allowed modalities. Defaults to mr,ct")

    parser.add_option("-o", "--org_root", default="5.555.5", dest = "org_root", action="store",
            help="Your organizations DICOM org root")

    parser.add_option("-l", "--log_file", default=None, dest="log_file", action = "store",
            help="Name of file to log messages to. Defaults to console")

    parser.add_option('-r', "--rename", default=False, dest="rename", action = "store_true",
            help="Rename anonymized files to the new SOP Instance UID")
    
    parser.add_option('-p', "--profile", default="basic", dest="profile", action = "store",
            help="Application Level Confidentiality Profile from DICOM 3.15 Annex E. Supported"
            " optons are 'basic' and 'clean'. 'basic means to adhere to the Basic Application Level"
            " Confidentiality Profile. 'clean' means adhere to the profile with the 'Clean Descriptors Option'."
            " Defaults to 'basic'. If specifying 'clean' you must also specify the 'white-list' option.")
    
    parser.add_option('-k', "--keepoverlay", default=False, dest="overlay", action = "store_true",
            help="Keep overlay data. Please note this will override the Basic Application Level Confidentiality Profile"
            "which does not allow for overlay data")

    (options, args) = parser.parse_args()

    ident_dir = args[0]
    clean_dir = args[1]
    allowed_modalities = [m.strip().lower() for m in options.modalities.split(",")]
    white_list_file = options.white_list
    quarantine_dir = options.quarantine
    audit_file = options.audit
    org_root = options.org_root
    if options.profile != 'basic' and white_list_file == None:
        print >> sys.stderr, "Unless using the basic profile, a white_list file must be specified"
        sys.exit()

    driver(ident_dir, clean_dir, quarantine_dir=quarantine_dir, audit_file=audit_file, 
            allowed_modalities=allowed_modalities, org_root=org_root, 
            white_list_file=white_list_file, log_file=options.log_file, rename=options.rename,profile=options.profile, overlay=options.overlay)
