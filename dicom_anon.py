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
from datetime import datetime
from optparse import OptionParser
from functools import partial

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

audit = None
db = None

TABLE_EXISTS = "SELECT name FROM sqlite_master WHERE name=?"
CREATE_REGULAR_TABLE = "CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, original, cleaned)"
CREATE_DATE_TABLE = "CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, original, cleaned, study INTEGER, FOREIGN KEY(study) REFERENCES studyinstanceuid(id))"
INSERT_OTHER = "INSERT INTO %s (original, cleaned) VALUES (?, ?)"
INSERT_DATE = "INSERT INTO %s (original, cleaned, study) VALUES (?, ?, ?)"
GET_OTHER = "SELECT cleaned FROM %s WHERE original = ?"
GET_DATE = "SELECT cleaned FROM %s WHERE original = ? AND study = ?"
STUDY_PK = "SELECT id FROM studyinstanceuid WHERE cleaned = ?"
NEXT_ID = "SELECT max(id) FROM %s"

CLEANED_DATE = "20000101"
CLEANED_TIME = "000000.00"

# Attributes taken from https://github.com/dicom/ruby-dicom
ATTRIBUTES = {
    "audit": {
        SERIES_INSTANCE_UID:1,
        SERIES_INSTANCE_UID:1,
        SOP_INSTANCE_UID:1,

        (0x8,0x50):1, # Accession Number
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
        (0x8,0x20): 1, # Study Date
    },
    "replace": {
        (0x8,0x12): "20000101", # Instance Creation Date
        (0x8,0x13): "000000.00", # Instance Creation Time
        (0x8,0x21): "20000101", # Series Date
        (0x8,0x23): "20000101", # Image Date
        (0x8,0x30): "000000.00", # Study Time
        (0x8,0x22): "20000101",  # Acquisition Date
        (0x8,0x33): "000000.00", # Image Time
        (0x10,0x30): "20000101",  # Patient's Birth Date
        (0x10,0x40): "", # Patient's Sex
        (0x10,0x1001): "", # Other Patient Names
        (0x10,0x1010): "",# Patients Age
        (0x10,0x1020): "",# Patient Size
        (0x10,0x1030): "", # Patient Weight
        (0x20,0x4000): "",# Image Comments
    },
    "delete":{
        (0x8,0x1140):1, # Referenced Image Sequence
        (0x8,0x1110):1, # Referenced Study Sequence
        (0x8,0x1120):1, # Referenced Patient Sequence
        (0x8,0x114A):1, # Referenced Instance Sequence
        (0x8,0x1150):1, # Referenced SOP Class UID Sequence
        (0x8,0x1155):1, # Referenced SOP ClassInstance UID Sequence 
        (0x10,0x50):1, # Patient Insurance Plan Sequence
        (0x10,0x1002):1, # Other Patient ID sequence
        (0x10,0x1050):1, # Patient Insurance Plan Sequence
        (0x10,0x1040):1, # Patient's Address    
        (0x10,0x1060):1, # Patient's Mother's Birth Name
        (0x10,0x1080):1, # Military Rank
        (0x10,0x1081):1, # Branch of Service
        (0x10,0x1090):1, # Medical Record Location
        (0x10,0x2000):1, # Medical alerts
        (0x10,0x2110):1, # Allergies
        (0x10,0x2150):1, # Country of Residence
        (0x10,0x2152):1, # Region of Residence
        (0x10,0x2154):1, # Patient Phone
        (0x10,0x2160):1, # Ethnic Group
        (0x10,0x2180):1, # Occupation Group
        (0x10,0x2297):1, # Responsible Persons Name
        (0x10,0x2299):1, # Responsible Organization
        (0x10,0x21A0):1, # Smoking Status
        (0x10,0x21B0):1, # Additional Patient History
        (0x10,0x21C0):1, # Pregnancy Status
        (0x10,0x21D0):1, # Last Menstrual Date
        (0x10,0x21F0):1, # Religious Pref
        (0x18,0x1200):1, # Date of Last Calibration
        (0x18,0x1201):1, # Time of Last Calibration 
        (0x20,0x52):1, # Frame of reference UID
        (0x32,0x12):1, # Study ID Issuer RET
        (0x32,0x1032):1, # Requesting Physician
        (0x32,0x1064):1, # Requested Procedure Sequence
        (0x40,0x275):1, # Requested Attributes Sequence
        (0x40,0x1001):1, # Requested Procedure ID
        (0x40,0x1010):1, # Names of intended recipient of results
        (0x40,0x1011):1, # ID sequence recipient of results
        (0x40,0x6):1, # Scheduled Performing Physician's Name
        (0x40,0x1012):1, # Reason for peformed procedure sequence
        (0x40,0x1101):1, # Person Identification Sequence
        (0x40,0x1102):1, # Person's address
        (0x40,0x1104):1, # Person's telephone numbers
        (0x40,0x1400):1, # Requested Procedure Comments
        (0x40,0x2001):1, # Reason for imagin gservie request RET
        (0x40,0x2008):1, # Order entered by
        (0x40,0x4037):1, # Human performers name
        (0x40,0xA075):1, # Verifying observers name
        (0x40,0xA123):1, # Person Name
        (0x40,0xA124):1, # UID
        (0x70,0x83):1, # Content Creators Name
        (0x72,0x6A):1, # Selector PN Value
        (0x3006,0xA6):1, # ROI Interpreter
        (0x300E,0x08):1, # Reviewer Name
        (0x4008,0x102):1, # Interpretation Recorder
        (0x4008,0x10A):1, # Interpretation Transcriber
        (0x4008,0x10B):1, # Interpretation Text
        (0x4008,0x10C):1, # Interpretation Author
        (0x4008,0x114):1, # Physician Approving Interpretation
        (0x4008,0x119):1, # Distribution Name
        # Additional Attributes as recommended by Digital Imaging and Communications in Medicine (DICOM)
        # Supplement 55: Attribute Level Confidentiality (including De-identification)
        (0x8,0x14):1, # Instance Creator UID
        (0x8,0x1040):1, # Institutional Name
        (0x8,0x1080):1, # Admitting Diagnoses Description
        (0x8,0x2111):1, # Derivation Description 
        (0x10,0x32):1, # Patient's Birth Time
        (0x10,0x1000):1, # Other Patient ID's
        (0x10,0x4000):1, # Patient Comments
        (0x18,0x1000):1, # Device Serial Number
        (0x18,0x1030):1, # Protocol Name
        (0x20,0x200):1, # Synchronization Frame of Reference UID
        (0x40,0x275):1, # Request Attribute Sequence
        (0x40,0xA730):1, # Content Sequence
        (0x88,0x140):1, # Storage Media File-set UID
        (0x3006,0x24):1, # Referenced Frame of Reference UID
        (0x3006,0xC2):1, # Related Frame of Reference UID
        (0x20,0x10):1, # Study ID
    }
}


def destination(source, dest, root):
    if not dest.endswith(os.path.sep):
        dest += os.path.sep

    if not root.endswith(os.path.sep):
        root += os.path.sep

    if dest.startswith(root):
        raise Exception("Destination directory cannot be inside"
            "or equal to source directory")

    if not source.startswith(root):
        raise Exception("The file to be moved must be in the root directory")

    s = difflib.SequenceMatcher(a=root, b=source)
    m = s.find_longest_match(0, len(root), 0, len(source))
    if not (m.a == m.b == 0):
        raise Exception("Unexpected file paths: source and root share no"
            " common path.")

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

def keep(e, white_list=None):
   if ATTRIBUTES["replace"].get((e.tag.group, e.tag.element), None) or \
      ATTRIBUTES["audit"].get((e.tag.group, e.tag.element), None):
       return True
   if white_list and white_list.get((e.tag.group, e.tag.element), None):
       return True
   return False
    
def generate_uid(org_root):
    n = datetime.now()
    while True:
        new_guid = "%s.%s.%s.%s.%s.%s.%s" % (org_root, n.year, n.month, n.day, n.minute, n.second, n.microsecond) 
        if new_guid != generate_uid.last:
            generate_uid.last = new_guid
            break
    return new_guid
generate_uid.last = None

def audit_cb(ds, e, study_pk=None, org_root=None):
    if e.tag in ATTRIBUTES['audit'].keys():
        cleaned = audit_get(e, study_uid_pk=study_pk)
        if cleaned == None:
            if e.VR == "DT":
                cleaned = CLEANED_TIME
            elif e.VR == "DA":
                cleaned = CLEANED_DATE
            elif e.VR == "UI":
                cleaned = generate_uid(org_root)
            else:
                cleaned = "%s %d" % (e.name, get_next_pk(e))
            audit_save(e, e.value, cleaned, study_uid_pk=study_pk)
        ds[e.tag].value = str(cleaned)

def delete_cb(ds, e):
    if e.tag in ATTRIBUTES["delete"].keys():
        del ds[e.tag]

def replace_cb(ds, e):
    if e.tag in ATTRIBUTES["replace"].keys():
       ds[e.tag].value = str(ATTRIBUTES["replace"][(e.tag.group,e.tag.element)])

def vr_cb(ds, e, white_list=None):
    if keep(e, white_list):
        return
    if e.VR in ["PN", "UI", "DA", "DT", "LT", "UN", "UT", "ST"]:
        del ds[e.tag]

def personal_cb(ds,e, white_list=None):
    if keep(e, white_list):
        return
    if e.tag.group == 0x1000:
        del ds[e.tag]

def white_list_cb(ds, e, w=None):
    if w.get(e.tag, None):
        if not e.value.lower.strip() in w[e.tag]:
            ds[e.tag].value = str("VALUE NOT IN WHITE LIST")

def convert_hex_json(h):
    value = {}
    for tag in h.keys():
        a,b = tag.split(',')
        t = (int(a,16),int(b,16))
        #TODO replace consecutive spaces with1
        value[t]=[x.lower().strip() for x in h[tag]]
    return value

def anonymize(ds, white_list, org_root):
    if white_list:
        w = open(white_list, 'r')
        white_list = w.read()
        white_list = json.loads(white_list)
        w.close()
        white_list = convert_hex_json(white_list) 

    # anonymize study_uid, save off id
    cleaned_study_uid = audit_get(ds[STUDY_INSTANCE_UID])
    if cleaned_study_uid == None:
        cleaned_study_uid = generate_uid(org_root)
        audit_save(ds[STUDY_INSTANCE_UID], ds[STUDY_INSTANCE_UID].value, cleaned_study_uid)
    ds[STUDY_INSTANCE_UID].value = cleaned_study_uid
    # Get pk of study_uid
    study_pk = audit_get_study_pk(cleaned_study_uid)

    ds.remove_private_tags()    

    # Take care of any attributes to be in the audit_trail
    ds.walk(partial(audit_cb, study_pk=study_pk, org_root=org_root))
    ds.walk(replace_cb)
    ds.walk(delete_cb)
    ds.walk(partial(vr_cb, white_list=white_list))
    ds.walk(partial(personal_cb, white_list=white_list))
    if white_list:
        ds.walk(partial(white_list_cb, w=white_list))
    return ds

def open_audit(identity):
    global db, audit
    bootstrap = False
    if not os.path.isfile(identity):
       bootstrap = True
    db = sqlite3.connect(identity)
    audit = db.cursor()
    if bootstrap:
        # create the table that holds the studyintance because others will refer to it
        audit.execute(CREATE_REGULAR_TABLE % "studyinstanceuid")
        db.commit()

def quarantine(ds, allowed_modalities):
    # TODO use a file allowed, not allowed
    if SERIES_DESCR in ds:
        series_desc = ds[SERIES_DESCR].value
        if series_desc.strip().lower() == "patient protocol":
            return True
    if MODALITY in ds:
        modality = ds[MODALITY].value
        if not modality.strip().lower() in allowed_modalities:
            return True
    if BURNT_IN in ds:
        burnt_in = ds[BURNT_IN].value
        if burnt.strip().lower() in ["yes", "y"]:
            return True
    return False


def driver(ident_dir, clean_dir, audit_file, whitelist_file, quarantine_dir, allowed_modalites=['mr','ct'], org_root='5.555.5'):
    open_audit(audit_file) 
    
    for root, dirs, files in os.walk(ident_dir):
         for filename in files:
             if filename.startswith("."):
                 continue
             try:
                 ds = dicom.read_file(os.path.join(root,filename))
             except IOError:
                 sys.stderr.write("Error reading file %s\n" % os.path.join(root,
                     filename))
                 db.close()
                 sys.exit()

             if quarantine(ds, allowed_modalites):
                 full_quarantine_dir = destination(os.path.join(root, filename), quarantine_dir, ident_dir)
                 if not os.path.exists(full_quarantine_dir):
                       os.makedirs(full_quarantine_dir)
                 quarantine_name = os.path.join(full_quarantine_dir, filename)
                 shutil.copyfile(os.path.join(root, filename), quarantine_name)
                 continue
             destination_dir = destination(os.path.join(root, filename), clean_dir, ident_dir)
             if not os.path.exists(destination_dir):
                 os.makedirs(destination_dir)
             ds = anonymize(ds, white_list, org_root)
             clean_name = os.path.join(destination_dir, filename)
             ds.save_as(clean_name)
    db.close()


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

    if tag.VR in ['DA', 'DT']:
        audit.execute(GET_DATE % table_name(tag), (original, study_uid_pk))
        results  = audit.fetchall()
        if len(results):
            value = results[0][0]
    else:
        audit.execute(GET_OTHER % table_name(tag), (original,))
        results  = audit.fetchall()
        if len(results):
            value = results[0][0]
    return value


def audit_save(tag, original, cleaned, study_uid_pk=None):
    if not table_exists(table_name(tag)):
        if tag.VR in ['DA', 'DT']:
            audit.execute(CREATE_DATE_TABLE % table_name(tag))
        else:
            audit.execute(CREATE_REGULAR_TABLE % table_name(tag))
        db.commit()

    # Table exists
    if tag.VR in ['DA', 'DT']:
        audit.execute(INSERT_DATE % table_name(tag), (original, cleaned, study_uid_pk))
    else:
        audit.execute(INSERT_OTHER % table_name(tag), (original, cleaned))

    db.commit()

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-q", "--quarantine", default="quarantine", dest="quarantine", action="store",
            help="Quarantine directory")
    
    parser.add_option("-w", "--whitelist", default=None, dest="whitelist", action="store",
            help="Whitelist json file")

    parser.add_option("-a", "--audit_file", default="identity.db", dest="audit", action="store",
            help="Name of sqlite audit file")

    parser.add_option("-m", "--modalities", default="mr,ct", dest = "modalities", action="store",
            help="Comma separated list of allowed modalities. Defaults to mr,ct")
    
    parser.add_option("-r", "--root", default="5.555.5", dest = "root", action="store",
            help="Your organizations DICOM org root")
    
    (options, args) = parser.parse_args()

    ident_dir = args[0]
    clean_dir = args[1]
    allowed_modalities = [m.strip().lower() for m in options.modalities.split(",")]
    white_list = options.whitelist
    quarantine_dir = options.quarantine
    audit_file = options.audit
    root = options.root

    driver(ident_dir, clean_dir, audit_file, white_list, quarantine_dir, allowed_modalities, root)
