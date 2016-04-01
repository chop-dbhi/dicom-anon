#!/usr/bin/env python
# Copyright (c) 2013, The Children's Hospital of Philadelphia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
# following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#   disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
#   following disclaimer in the documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
import dicom
from dicom.errors import InvalidDicomError
from dicom.tag import Tag
from dicom.dataelem import DataElement
from dicom.dataset import Dataset
from dicom.sequence import Sequence
from dicom.multival import MultiValue
from dicom.valuerep import DS
from datetime import datetime
import logging
import json
import re
import sqlite3
import shutil
from functools import partial
import argparse

TABLE_EXISTS = 'SELECT name FROM sqlite_master WHERE name=?'
CREATE_NON_LINKED_TABLE = 'CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, original, cleaned)'
CREATE_LINKED_TABLE = 'CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, original, cleaned, study INTEGER, ' \
                      'FOREIGN KEY(study) REFERENCES studyinstanceuid(id))'
INSERT_OTHER = 'INSERT INTO %s (original, cleaned) VALUES (?, ?)'
INSERT_LINKED = 'INSERT INTO %s (original, cleaned, study) VALUES (?, ?, ?)'
GET_NON_LINKED = 'SELECT cleaned FROM %s WHERE original = ?'
GET_LINKED = 'SELECT cleaned FROM %s WHERE original = ? AND study = ?'
UPDATE_LINKED = 'UPDATE %s SET cleaned = ? WHERE cleaned = ? AND study = ?'
STUDY_PK = 'SELECT id FROM studyinstanceuid WHERE cleaned = ?'
NEXT_ID = 'SELECT max(id) FROM %s'

MEDIA_STORAGE_SOP_INSTANCE_UID = (0x2, 0x3)
STUDY_INSTANCE_UID = (0x20, 0xD)
STUDY_DESCR = (0x8, 0x1030)
SERIES_INSTANCE_UID = (0x20, 0xE)
SERIES_DESCR = (0x8, 0x103E)
SOP_CLASS_UID = (0x8, 0x16)
SOP_INSTANCE_UID = (0x8, 0x18)
PIXEL_SPACING = (0x28, 0x30)
IMAGER_PIXEL_SPACING = (0x18, 0x1164)
WINDOW_CENTER = (0x28, 0x1050)
WINDOW_WIDTH = (0x28, 0x1051)
CALIBRATION_TYPE = (0x28, 0x402)
CALIBRATION_DESCR = (0x28, 0x404)
BURNT_IN = (0x28, 0x301)
MODALITY = (0x8, 0x60)
IMAGE_TYPE = (0x8, 0x8)
MANUFACTURER = (0x8, 0x70)
MANUFACTURER_MODEL_NAME = (0x8, 0x1090)
PIXEL_DATA = (0x7fe0, 0x10)
PHOTOMETRIC_INTERPRETATION = (0x28, 0x4)

REMOVED_TEXT = '^^Audit Trail - Removed by dicom-anon - Audit Trail^^'

ALLOWED_FILE_META = {  # Attributes taken from https://github.com/dicom/ruby-dicom
  (0x2, 0x0): 1,  # File Meta Information Group Length
  (0x2, 0x1): 1,  # Version
  (0x2, 0x2): 1,  # Media Storage SOP Class UID
  (0x2, 0x3): 1,  # Media Storage SOP Instance UID
  (0x2, 0x10): 1,  # Transfer Syntax UID
  (0x2, 0x12): 1,  # Implementation Class UID
  (0x2, 0x13): 1  # Implementation Version Name
}

AUDIT = {
    STUDY_INSTANCE_UID: 1,
    SERIES_INSTANCE_UID: 1,
    SOP_INSTANCE_UID: 1,
    (0x8, 0x20): 1,  # Study Date
    (0x8, 0x50): 1,  # Accession Number - Z BALC
    (0x8, 0x80): 1,  # Institution name
    (0x8, 0x81): 1,  # Institution Address
    (0x8, 0x90): 1,  # Referring Physician's name
    (0x8, 0x92): 1,  # Referring Physician's address
    (0x8, 0x94): 1,  # Referring Physician's Phone
    (0x8, 0x1048): 1,  # Physician(s) of Record
    (0x8, 0x1049): 1,  # Physician(s) of Record Identification
    (0x8, 0x1050): 1,  # Performing Physician's Name
    (0x8, 0x1060): 1,  # Reading Physicians Name
    (0x8, 0x1070): 1,  # Operator's Name
    (0x8, 0x1010): 1,  # Station name
    (0x10, 0x10): 1,  # Patient's name
    (0x10, 0x1005): 1,  # Patient's Birth Name
    (0x10, 0x20): 1,  # Patient's ID
    (0x10, 0x30): 1,  # Patient's Birth Date
}

CLEANED_DATE = '19010101'
CLEANED_TIME = '000000.00'

logger = logging.getLogger('dicom_anon')
logger.setLevel(logging.INFO)


class Audit(object):

    def __init__(self, filename):
        self.db = sqlite3.connect(filename)
        self.cursor = self.db.cursor()
        if not os.path.isfile(filename):
            # create the table that holds the studyintance because others will refer to it
            self.cursor.execute(CREATE_NON_LINKED_TABLE % 'studyinstanceuid')
            self.db.commit()

    @staticmethod
    def tag_to_table(tag):
        return re.sub('\W+', '', tag.name.lower())

    def close(self):
        self.db.close()

    def table_exists(self, table):
        self.cursor.execute(TABLE_EXISTS, (table,))
        results = self.cursor.fetchall()
        return len(results) > 0

    def get_study_pk(self, cleaned):
        self.cursor.execute(STUDY_PK, (cleaned,))
        results = self.cursor.fetchall()
        return results[0][0]

    def get_next_pk(self, tag):
        table_name = self.tag_to_table(tag)
        if not self.table_exists(table_name):
            return 1
        self.cursor.execute(NEXT_ID % table_name)
        results = self.cursor.fetchall()
        if results[0][0]:
            return int(results[0][0] + 1)
        else:
            return 1

    def get(self, tag, study_uid_pk=None):
        table_name = self.tag_to_table(tag)
        value = None
        if tag.VM > 1:
            original = [str(val) for val in tag.value]
            original = '/'.join(original)
        else:
            original = tag.value

        if not self.table_exists(table_name):
            return None

        if tag.name.lower() == 'study instance uid':
            self.cursor.execute(GET_NON_LINKED % table_name, (original,))
            results = self.cursor.fetchall()
            if len(results):
                value = results[0][0]
        else:
            self.cursor.execute(GET_LINKED % table_name, (original, study_uid_pk))
            results = self.cursor.fetchall()
            if len(results):
                value = results[0][0]

        return value

    def update(self, tag, cleaned, study_uid_pk):
        table_name = self.tag_to_table(tag)
        if tag.VM > 1:
            original = [str(val) for val in tag.value]
            original = '/'.join(original)
        else:
            original = tag.value
        self.cursor.execute(UPDATE_LINKED % table_name, (cleaned, original, study_uid_pk))
        self.db.commit()

    def save(self, tag, cleaned, study_uid_pk=None):
        table_name = self.tag_to_table(tag)
        if not self.table_exists(table_name):
            if tag.name.lower() == 'study instance uid':
                self.cursor.execute(CREATE_NON_LINKED_TABLE % table_name)
            else:
                self.cursor.execute(CREATE_LINKED_TABLE % table_name)
            self.db.commit()

        if tag.VM > 1:
            original = [str(val) for val in tag.value]
            original = '/'.join(original)
        else:
            original = tag.value

        # Table exists
        if tag.name.lower() == 'study instance uid':
            self.cursor.execute(INSERT_OTHER % table_name, (original, cleaned))
        else:
            self.cursor.execute(INSERT_LINKED % table_name, (original, cleaned, study_uid_pk))
        self.db.commit()


class DicomAnon(object):

    def __init__(self, **kwargs):
        self.profile = kwargs.get('profile', 'basic')
        self.spec_file = kwargs.get('spec_file', os.path.join(os.path.dirname(__file__), 'spec_files',
                                                              'annexe_ext.dat'))
        self.white_list_file = kwargs.get('white_list', None)
        self.audit_file = kwargs.get('audit_file', 'identity.db')
        self.log_file = kwargs.get('log_file', 'dicom_anon.log')
        self.quarantine = kwargs.get('quarantine', 'quarantine')
        self.modalities = [string.lower() for string in kwargs.get('modalities', ['mr', 'ct'])]
        self.org_root = kwargs.get('org_root', '5.555.5')
        self.rename = kwargs.get('rename', False)
        self.keep_overlay = kwargs.get('keep_overlay', False)
        self.keep_private_tags = kwargs.get('keep_private_tags', False)
        self.keep_csa_headers = kwargs.get('keep_csa_headers', False)
        self.relative_dates = kwargs.get('relative_dates', None)

        if self.white_list_file is not None:
            try:
                with open(self.white_list_file, 'r')) as white_list_handle:
                    self.white_list = self.convert_json_white_list(json.load(white_list_handle))
            except IOError:
                raise Exception('Error opening white list file.')

        self.spec = self.parse_spec_file(self.spec_file)

        self.audit = Audit(self.audit_file)

        self.current_uid = None

        logger.handlers = []
        if not self.log_file:
            self.log = logging.StreamHandler()
        else:
            self.log = logging.FileHandler(self.log_file)

        self.log.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log.setFormatter(formatter)
        logger.addHandler(self.log)

    @staticmethod
    def get_first_date(target_dir, tags=((0x0010, 0x0030),)):
        min_date = {tag: datetime(3000, 1, 1) for tag in tags}
        for root, _, files in os.walk(target_dir):
            filename = sorted(files)[0]
            try:
                ds = dicom.read_file(open(os.path.join(root, filename)), stop_before_pixels=True)
            except (IOError, InvalidDicomError):
                continue
            for tag in tags:
                try:
                    this_date = datetime.strptime(ds[tag].value, '%Y%m%d')
                except (KeyError, ValueError):
                    continue
                if this_date < min_date[tag]:
                    min_date[tag] = this_date
        return min_date

    @staticmethod
    def convert_json_white_list(h):
        value = {}
        for tag in h.keys():
            a, b = tag.split(',')
            t = (int(a, 16), int(b, 16))
            value[t] = [re.sub(' +', ' ', re.sub('[-_,.]', '', x.lower().strip())) for x in h[tag]]
        return value

    @staticmethod
    def parse_spec_file(filename):
        spec_dict = dict()
        tag = None
        with open(filename) as spec_file:
            for index, line in enumerate(spec_file):
                line_arr = line.strip(' \n').split('\t')
                if index % 2 == 0:
                    tag = (int('0x%s' % line_arr[1].split(',')[0].strip()[1:], 16),
                           int('0x%s' % line_arr[1].split(',')[1].strip()[:-1], 16))
                else:
                    spec_dict[tag] = line_arr
        return spec_dict

    def close_all(self):
        if self.log_file:
            self.log.flush()
            self.log.close()
        self.audit.close()

    # Determines destination of cleaned/quarantined file based on
    # source folder
    @staticmethod
    def destination(source, dest, root):
        if dest.startswith(root):
            raise Exception('Destination directory cannot be inside or equal to source directory')
        if not source.startswith(root):
            raise Exception('The file to be moved must be in the root directory')
        return os.path.normpath(os.path.join(dest, os.path.relpath(os.path.dirname(source), root)))

    def quarantine_file(self, filepath, ident_dir, reason):
        full_quarantine_dir = self.destination(filepath, self.quarantine, ident_dir)
        if not os.path.exists(full_quarantine_dir):
            os.makedirs(full_quarantine_dir)
        quarantine_name = os.path.join(full_quarantine_dir, os.path.basename(filepath))
        logger.info('%s will be moved to quarantine directory due to: %s' % (filepath, reason))
        shutil.copyfile(filepath, quarantine_name)

    # Return true if file should be quarantined
    # TODO the presence of the following attributes
    # indicates the file is a secondary capture which may
    # be something to check for if not visually inspecting
    # images (which really needs to be done anyway)
    # (0x0018,0x1010) - Secondary Capture Device ID
    # (0x0018,0x1012) - Date of Secondary Capture
    # (0x0018,0x1014) - Time of Secondary Capture
    # (0x0018,0x1016) - Secondary Capture Device Manufacturer
    # (0x0018,0x1018) - Secondary Capture Device Manufacturer's Model Name
    # (0x0018,0x1019) - Secondary Capture Device Software Versions
    def check_quarantine(self, ds):
        if SERIES_DESCR in ds and ds[SERIES_DESCR].value is not None:
            series_desc = ds[SERIES_DESCR].value.strip().lower()
            if 'patient protocol' in series_desc:
                return True, 'patient protocol'
            elif 'save' in series_desc:  # from link in comment below
                return True, 'Likely screen capture'

        if MODALITY in ds:
            modality = ds[MODALITY]
            if modality.VM == 1:
                modality = [modality.value]
            for m in modality:
                if m is None or not m.lower() in self.modalities:
                    return True, 'modality not allowed'

        if MODALITY not in ds:
            return True, 'Modality missing'

        if BURNT_IN in ds and ds[BURNT_IN].value is not None:
            burnt_in = ds[BURNT_IN].value
            if burnt_in.strip().lower() in ['yes', 'y']:
                return True, 'burnt-in data'

        # The following were taken from https://wiki.cancerimagingarchive.net/download/attachments/
        # 3539047/pixel-checker-filter.script?version=1&modificationDate=1333114118541&api=v2
        if IMAGE_TYPE in ds:
            image_type = ds[IMAGE_TYPE]
            if image_type.VM == 1:
                image_type = [image_type.value]
            for i in image_type:
                if i is not None and 'save' in i.strip().lower():
                    return True, 'Likely screen capture'

        if MANUFACTURER in ds:
            manufacturer = ds[MANUFACTURER].value.strip().lower()
            if 'north american imaging, inc' in manufacturer or 'pacsgear' in manufacturer:
                return True, 'Manufacturer is suspect'

        if MANUFACTURER_MODEL_NAME in ds:
            model_name = ds[MANUFACTURER_MODEL_NAME].value.strip().lower()
            if 'the dicom box' in model_name:
                return True, 'Manufacturer model name is suspect'
        return False, ''

    def generate_uid(self):

        while True:
            n = datetime.now()
            new_guid = '%s.%s.%s.%s.%s.%s.%s' % (self.org_root, n.year, n.month, n.day,
                                                 n.minute, n.second, n.microsecond)
            if new_guid != self.current_uid:
                self.current_uid = new_guid
                break
        return self.current_uid

    def clean_cb(self, ds, e, study_pk):
        if self.enforce_profile(ds, e, study_pk):
            return
        if self.vr_handler(ds, e):
            return
        if not self.keep_overlay and self.overlay_data_handler(ds, e):
            return
        if self.overlay_comment_handler(ds, e):
            return
        if self.curve_data_handler(ds, e):
            return
        self.personal_handler(ds, e)

    def enforce_profile(self, ds, e, study_pk):
        white_listed = False
        cleaned = None
        if self.profile == 'clean':
            # If it's list in the ANNEX, we need to specifically be able to clean it
            if (e.tag in self.spec.keys() and self.spec[(e.tag.group, e.tag.element)][9] == 'C') \
                    or not (e.tag in self.spec.keys()):
                white_listed = self.white_list_handler(e)
                if not white_listed:
                    cleaned = self.basic(ds, e, study_pk)
            else:
                cleaned = self.basic(ds, e, study_pk)
        else:
            cleaned = self.basic(ds, e, study_pk)

        if cleaned is not None and e.tag in ds and ds[e.tag].value is not None:
            ds[e.tag].value = cleaned

        # Tell our caller if we cleaned this element
        if e.tag in self.spec.keys() or white_listed:
            return True

        return False

    # Returning None from this function signfies that e was not altered
    def basic(self, ds, e, study_pk):
        # Sequences are currently just removed
        # there is no audit support
        if e.VR == 'SQ':
            del ds[e.tag]
            return REMOVED_TEXT
        cleaned = None
        value = ds[e.tag].value
        prior_cleaned = self.audit.get(e, study_uid_pk=study_pk)
        # pydicom does not want to write unicode strings back to the files
        # but sqlite is returning unicode, test and convert
        if prior_cleaned:
            prior_cleaned = str(prior_cleaned)
        if e.tag in self.spec.keys():
            rule = self.spec[(e.tag.group, e.tag.element)][2][0]  # For now we aren't going to worry about
            # IOD type conformance, just do the first option
            if rule == 'D':
                cleaned = prior_cleaned or self.replace_vr(e)
            if rule == 'Z':
                cleaned = prior_cleaned or self.replace_vr(e)
            if rule == 'X':
                del ds[e.tag]
                cleaned = prior_cleaned or REMOVED_TEXT
            if rule == 'K':
                cleaned = value
            if rule == 'U':
                cleaned = prior_cleaned or self.generate_uid()

        if e.tag in AUDIT.keys():
            if cleaned is not None and cleaned != value and prior_cleaned is None and not (e.tag == STUDY_INSTANCE_UID):
                self.audit.save(e, cleaned, study_uid_pk=study_pk)

        return cleaned

    # TODO this needs work, it should be smarter and cover more VRs properly
    def replace_vr(self, e):
        if e.VR == 'DT':
            cleaned = CLEANED_TIME
        elif e.VR == 'DA':
            cleaned = CLEANED_DATE
        elif e.VR == 'TM':
            cleaned = CLEANED_TIME
        elif e.VR == 'UI':
            cleaned = self.generate_uid()
        else:
            if e.tag in AUDIT.keys() and e.name and len(e.name):
                cleaned = ('%s %d' % (e.name, self.audit.get_next_pk(e))).encode('ascii')
            else:
                cleaned = 'CLEANED'
        return cleaned

    @staticmethod
    def vr_handler(ds, e):
        if e.VR in ['PN', 'CS', 'UI', 'DA', 'DT', 'LT', 'UN', 'UT', 'ST', 'AE', 'LO', 'TM', 'SH', 'AS', 'OB',
                    'OW'] and e.tag != PIXEL_DATA:
            del ds[e.tag]
            return True
        return False

    # Remove group 0x1000 which contains personal information
    @staticmethod
    def personal_handler(ds, e):
        if e.tag.group == 0x1000:
            del ds[e.tag]
            return True
        return False

    # Curve data is (0x50xx,0xxxxx)
    @staticmethod
    def curve_data_handler(ds, e):
        if (e.tag.group / 0xFF) == 0x50:
            del ds[e.tag]
            return True
        return False

    # Overlay comment is (0x60xx,0x4000)
    @staticmethod
    def overlay_comment_handler(ds, e):
        if (e.tag.group / 0xFF) == 0x60 and e.tag.element == 0x4000:
            del ds[e.tag]
            return True
        return False

    # Overlay data is and (0x60xx, 0x3000)
    @staticmethod
    def overlay_data_handler(ds, e):
        if (e.tag.group / 0xFF) == 0x60 and e.tag.element == 0x3000:
            del ds[e.tag]
            return True
        return False

    @staticmethod
    def clean_meta(ds, e):
        if e.VR == 'SQ':
            del ds[e.tag]
        elif ALLOWED_FILE_META.get((e.tag.group, e.tag.element), None):
            return
        else:
            del ds[e.tag]

    def white_list_handler(self, e):
        if self.white_list.get((e.tag.group, e.tag.element), None):
            if not re.sub(' +', ' ', re.sub('[-_,.]', '', e.value.lower().strip())) \
                    in self.white_list[(e.tag.group, e.tag.element)]:
                logger.info('%s not in white list for %s' % (e.value, e.name))
                return False
            return True
        return False

    def anonymize(self, ds):
        # anonymize study_uid, save off id
        cleaned_study_uid = self.audit.get(ds[STUDY_INSTANCE_UID])
        if cleaned_study_uid is None:
            cleaned_study_uid = self.generate_uid()
            self.audit.save(ds[STUDY_INSTANCE_UID], cleaned_study_uid)

        # Get pk of study_uid
        study_pk = self.audit.get_study_pk(cleaned_study_uid)

        if not self.keep_private_tags:
            ds.remove_private_tags()

        # Walk entire file
        ds.walk(partial(self.clean_cb, study_pk=study_pk))

        # Fix file meta data portion
        if MEDIA_STORAGE_SOP_INSTANCE_UID in ds.file_meta:
            ds.file_meta[MEDIA_STORAGE_SOP_INSTANCE_UID].value = ds[SOP_INSTANCE_UID].value
        ds.file_meta.walk(self.clean_meta)
        return ds, study_pk

    def run(self, ident_dir, clean_dir):
        # Get first date for tags set in relative_dates
        date_adjust = None
        audit_date_correct = None
        if self.relative_dates is not None:
            date_adjust = {tag: first_date - datetime(1970, 1, 1) for tag, first_date
                           in self.get_first_date(ident_dir, self.relative_dates).items()}
        for root, _, files in os.walk(ident_dir):
            for filename in files:
                if filename.startswith('.'):
                    continue
                source_path = os.path.join(root, filename)
                try:
                    ds = dicom.read_file(source_path)
                except IOError:
                    logger.error('Error reading file %s' % source_path)
                    self.close_all()
                    return False
                except InvalidDicomError:  # DICOM formatting error
                    self.quarantine_file(source_path, ident_dir, 'Could not read DICOM file.')
                    continue

                move, reason = self.check_quarantine(ds)

                if move:
                    self.quarantine_file(source_path, ident_dir, reason)
                    continue

                # Store adjusted dates for recovery
                obfusc_dates = None
                if self.relative_dates is not None:
                    obfusc_dates = {tag: datetime.strptime(ds[tag].value, '%Y%m%d') - date_adjust[tag]
                                    for tag in self.relative_dates}

                # Keep CSA Headers
                csa_headers = dict()
                if self.keep_csa_headers and (0x29, 0x10) in ds:
                    csa_headers[(0x29, 0x10)] = ds[(0x29, 0x10)]
                    for offset in [0x10, 0x20]:
                        elno = (0x10*0x0100) + offset
                        csa_headers[(0x29, elno)] = ds[(0x29, elno)]

                destination_dir = self.destination(source_path, clean_dir, ident_dir)
                if not os.path.exists(destination_dir):
                    os.makedirs(destination_dir)
                try:
                    ds, study_pk = self.anonymize(ds)
                except ValueError, e:
                    self.quarantine_file(source_path, ident_dir, 'Error running anonymize function. There may be a '
                                                                 'DICOM element value that does not match the specified'
                                                                 ' Value Representation (VR). Error was: %s' % e)
                    continue

                # Recover relative dates
                if self.relative_dates is not None:
                    for tag in self.relative_dates:
                        if audit_date_correct != study_pk and tag in AUDIT.keys():
                            self.audit.update(ds[tag], obfusc_dates[tag].strftime('%Y%m%d'), study_pk)
                        ds[tag].value = obfusc_dates[tag].strftime('%Y%m%d')
                    audit_date_correct = study_pk

                # Restore CSA Header
                if len(csa_headers) > 0:
                    for tag in csa_headers:
                        ds[tag] = csa_headers[tag]

                # Set Patient Identity Removed to YES
                t = Tag((0x12, 0x62))
                ds[t] = DataElement(t, 'CS', 'YES')

                # Set the De-identification method code sequence
                method_ds = Dataset()
                t = dicom.tag.Tag((0x8, 0x102))
                if self.profile == 'clean':
                    method_ds[t] = DataElement(t, 'DS', MultiValue(DS, ['113100', '113105']))
                else:
                    method_ds[t] = DataElement(t, 'DS', MultiValue(DS, ['113100']))
                t = dicom.tag.Tag((0x12, 0x64))
                ds[t] = DataElement(t, 'SQ', Sequence([method_ds]))

                out_filename = ds[SOP_INSTANCE_UID].value if self.rename else filename
                clean_name = os.path.join(destination_dir, out_filename)
                try:
                    ds.save_as(clean_name)
                except IOError:
                    logger.error('Error writing file %s' % clean_name)
                    self.close_all()
                    return False

        self.close_all()
        return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='ident_dir', type=str)
    parser.add_argument(dest='clean_dir', type=str)
    parser.add_argument('-q', '--quarantine', type=str, default='quarantine', help='Quarantine directory')
    parser.add_argument('-w', '--white_list', type=str, default=None, help='White list json file')
    parser.add_argument('-a', '--audit_file', type=str, default='identity.db', help='Name of sqlite audit file')
    parser.add_argument('-m', '--modalities', type=str, nargs='+', default=['mr', 'ct'],
                        help='Comma separated list of allowed modalities. Defaults to mr,ct')
    parser.add_argument('-o', '--org_root', type=str, default='5.555.5', help='Your organizations DICOM org root')
    parser.add_argument('-l', '--log_file', type=str, default=None,
                        help='Name of file to log messages to. Defaults to console')
    parser.add_argument('-r', '--rename', action='store_true', default=False, help='Rename anonymized files to the new'
                                                                                   'SOP Instance UID')
    parser.add_argument('-p', '--profile', type=str, default='basic', choices=['basic', 'clean'],
                        help='Application Level Confidentiality Profile from DICOM 3.15 Annex E. Supported'
                             ' optons are "basic" and "clean". "basic" means to adhere to the Basic '
                             '"Application Level". Confidentiality Profile. "clean" means adhere to the profile with '
                             'the "Clean Descriptors Option". Defaults to "basic". If specifying "clean" you must also '
                             'specify the "white_list" option.')
    parser.add_argument('-k', '--keep_overlay', action='store_true', default=False,
                        help='Keep overlay data. Please note this will override the Basic Application Level '
                             'Confidentiality Profile which does not allow for overlay data')
    parser.add_argument('-t', '--keep_private_tags', action='store_true', default=False,
                        help='Keep private tags. Please note this will override the Basic Application Level '
                             'Confidentiality Profile which does not allow private tags.')
    parser.add_argument('-c', '--keep_csa_headers', action='store_true', default=False,
                        help='Keep Siemens CSA Headers. Please note this will override the Basic Application Level '
                             'Confidentiality Profile which does not allow private tags.')
    parser.add_argument('-s', '--spec_file', type=str, default=os.path.join(os.path.dirname(__file__), 'spec_files',
                                                                            'annexe_ext.dat'),
                        help='Specification file that describes the anonymization strategy.')
    parser.add_argument('-e', '--relative_dates', type=str, nargs=2, action='append', default=None,
                        help='Dicom tags for date fields that should be made relative, rather than replaced.')
    args = parser.parse_args()
    if args.relative_dates is not None:
        args.relative_dates = [tuple([int(item[0], 16), int(item[1], 16)]) for item in args.relative_dates]
    i_dir = args.ident_dir
    c_dir = args.clean_dir
    del args.ident_dir
    del args.clean_dir
    da = DicomAnon(**vars(args))
    da.run(i_dir, c_dir)
