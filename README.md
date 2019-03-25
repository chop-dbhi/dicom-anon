[![Build Status](https://travis-ci.org/chop-dbhi/dicom-anon.png?branch=master)](https://travis-ci.org/chop-dbhi/dicom-anon) [![Coverage Status](https://coveralls.io/repos/cbmi/dicom-anon/badge.png?branch=master)](https://coveralls.io/r/cbmi/dicom-anon?branch=master)

Copyright (c) 2019, The Children's Hospital of Philadelphia
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
   disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
   following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


# Python DICOM Anonymizer

This is a DICOM anonymization script. It takes a source directory of identified files, attempts to de-identify them, and places them (with the same directory structure) in the specified target directory.

## Notice

Use this software at your own risk, no guarantees are made. Please check your results and report any issues. It is not capable of detecting burnt-in information in the pixel data or overlays. For a more complete anonymization solution it is suggested this script be used in conjunction with the [django-dicom-review](https://github.com/cbmi/django-dicom-review) and the [dicom-pipeline](https://github.com/cbmi/dicom-pipeline). The DICOM Tag Sniffer offered [here](https://wiki.cancerimagingarchive.net/display/Public/De-identification+Knowledge+Base) by the [Cancer Imaging Archive](http://www.cancerimagingarchive.net/) may be useful in verifying successful de-identification.

Please note that this script will create an sqlite3 database in the working directory that it is run from that _will_ contain identified
information (as it is the audit trail of the de-identification run). Delete the database (`identity.db` by default) after each run if you do not wish to
keep this data.

## Dependencies

* This package only runs under Python 2.7, **it does not work under Python 3**.
* This package requires a specific, older version of the pydicom package. See the requirements.txt for details.

## Usage
    python dicom_anon.py <source_directory> <target_directory>
    
There are a number of options that can be explored using the --help tag.

## Main Features
1. The software attempts to be compliant with the Basic Application Level Confidentiality Profile as specified in DICOM 3.15 Annex E document  (located at ftp://medical.nema.org/medical/dicom/2011/11_15pu.pdf), however no guarantees are made. By specifying the `--p clean` on the command line you can turn on the Clean Descriptors option which will allow for using the white list feature (specified below) where applicable. For example, if an attribute is marked as `C` in the `Clean Desc. Option` column of the standard, then if the attribute is present in the white list file and its value is found on the white list, it will be able to stay in the DICOM file. Only values either not specified in Annex E at all, or values explicitly enabled for the `Clean Desc. Option` will work with the white list feature. Please note, no attempt to clean Sequences (VR of SQ) is made, even with this option turned on- Sequences are blindly removed. This may technically be breaking with the standard, but the ramifications of keeping a proper audit trail are beyond the scope of this script. Also note that with respect to paragraph 5 on page 63 of DICOM 3.5-2011, this anonymizer will not remove all attributes not specified in Annex E. It does make an attempt to remove unspecified attributes with suspicious VRs (PN, for example).
1. Database audit trail - The anonymizer creates a sqlite database with a table containing the original and cleaned version of every attribute in the AUDIT dictionary defined at the top of the source file. This makes the process repeatable, and the sqlite database can be used in post processing. The name of the database can be specified on the command line.
1. Study, Source and Instance UID anonymization - The script will replace these UIDS with new ones based off your DICOM org root (specify using command line) and the current date and time.
1. White lists - The anonymizer supports a JSON white list file (specify on command line). The keys are DICOM tags and the values are lists of strings that the corresponding DICOM tag is allowed to be. If the value in the DICOM file matches a value on the list, it will be left, otherwise it will be removed. For example, using the following white list file:

    ```json
    {
        "0008,1030": [
            "CT CHEST W/CONTRAST",
            "NECK STUDY"
        ],
        "0008,103E": [
            "3D HEAD BONE",
            "ST HEAD"
        ]
    }
     ```
    DICOM attribute 0x8,0x1030 (Study Description) is allowed to be "CT CHEST W/CONTRAST" or "NECK STUDY". All other values will be removed. Case does not matter. Beginning and ending spaces will be stripped and consecutive spaces collapsed. Commas, dashes, underscores and periods will also be ignored for sake of comparison.

1. Quarantine - Files that are explicitly marked as containing burnt-in data along with files that have a series description of "Patient Protocol" will be copied to a quarantine directory (they are not deleted from the source directory). There are a few other conditions that will result in quarantine as well. The directory can be changed on the command line, but defaults to `quarantine` in the current working directory. Files that do not match the allowed modalities (see next item) will also be copied to quarantine. Suggestions for further heuristics are welcome.
1. Restrict modality. By default only MR and CT will be allowed. This can be changed using the command line.
1. Date Shifting. If selected, the script will check the first DICOM file in each directory for the date tags specified from the command line. It finds the earliest date for each tag. This date is shifted to 19010101 and the other dates in that tag for other files are shifted by the same amount, preserving temporal differences in the date tags, but removing the actual date component.


# Example
Assume the identified DICOM files are in a directory called `identified` in your home directory, and you want the de-identified placed in a directory called `cleaned` in your home directory.

The following command will put the audit trail in a file called identities.db in the current working directory. It will use a file called white_list.json in the current working directory for the white_list. It will use the `Clean Descriptors Option` of the DICOM Standard. It will rename the anonymized files in the target directory according to their new SOP Instance UID. It will use a DICOM org root of 1.2.3.4.5. MR,CT and CR modalities will be allowed. Files that need to be quarantined will be moved to a directory called `quarantined_files' in the current working directory.

```
python dicom_anon.py -o 1.2.3.4.5 -r -p clean -m mr,ct,cr -a identities.db -q quarantined_files -w white_list.json ~/identified ~/cleaned 
```

# Customization
To customize how specific fields are anonymized, supply a different spec file using --spec_file option to the script. The default spec file, `annexe_ext.dat` is a slight modification of the recommendations in ANNEX E (located at ftp://medical.nema.org/medical/dicom/2011/11_15pu.pdf) of the DICOM standard. The `annexe.dat` file contains the recommendations from ANNEX E if you prefer to use that. The tab separated columns in the spec file correspond to the columns in the table of the ANNEX E document starting on page 65.
