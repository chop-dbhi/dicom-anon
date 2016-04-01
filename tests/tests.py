import unittest
import dicom
import dicom_anon


class TestDICOMAnon(unittest.TestCase):

    def setUp(self):
        pass

    def test_basic(self):
        ds = dicom.read_file("tests/samples/test_wrist_cr1.dcm")
        self.assertEqual(ds.PatientName, "Identified Patient")
        da = dicom_anon.DicomAnon(quarantine="quarantine", audit_file="identity.db",
                                  modalities=["us", "cr", "ct", "mr", "pr"], org_root="1.2.826.0.1.3680043.8.1008",
                                  white_list="white_list.json", log_file=None, rename=False, profile="basic",
                                  overlay=False)
        da.run("tests/samples", "tests/clean")
        ds = dicom.read_file("tests/clean/test_wrist_cr1.dcm")
        self.assertEqual(ds.PatientName, "Patient's Name 1")
        self.assertEqual(ds.StudyID, "CLEANED")
        # Not using clean descriptions, so study and series description should be gone
        self.assertFalse(dicom_anon.SERIES_DESCR in ds)
        self.assertFalse(dicom_anon.STUDY_DESCR in ds)

    def test_clean_option(self):
        ds = dicom.read_file("tests/samples/test_wrist_cr1.dcm")
        self.assertEqual(ds.PatientName, "Identified Patient")
        da = dicom_anon.DicomAnon(quarantine="quarantine", audit_file="identity.db",
                                  modalities=["us", "cr", "ct", "mr", "pr"], org_root="1.2.826.0.1.3680043.8.1008",
                                  white_list="white_list.json", log_file=None, rename=False, profile="clean",
                                  overlay=False)

        da.run("tests/samples", "tests/clean")
        ds = dicom.read_file("tests/clean/test_wrist_cr1.dcm")
        self.assertEqual(ds.PatientName, "Patient's Name 1")
        # Study Description was in whitelist so it should have stayed 
        self.assertTrue(dicom_anon.STUDY_DESCR in ds)
        self.assertEqual(ds.StudyDescription, "WRIST MIN 3V UNILAT")
        # Series Description was not in white list
        self.assertFalse(dicom_anon.SERIES_DESCR in ds)

if __name__ == '__main__':
    unittest.main()
