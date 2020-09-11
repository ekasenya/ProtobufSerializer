import os
import unittest
import gzip

import pb
MAGIC = 0xFFFFFFFF
DEVICE_APPS_TYPE = 1
TEST_FILE = "/tmp/otus/test.pb.gz"


class TestPB(unittest.TestCase):
    deviceapps = [
        {"device": {"type": "idfa", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7c"},
         "lat": 67.7835424444, "lon": -22.8044005471, "apps": [1, 2, 3, 4]},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": [1, 2]},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": []},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "apps": [1]},
    ]

    def tearDown(self):
        if os.path.exists(TEST_FILE):
            os.remove(TEST_FILE)

    def test_write(self):
        bytes_written = pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        print(bytes_written)
        self.assertTrue(bytes_written > 0)
        with gzip.open(TEST_FILE, mode='rb') as f:
            record_magic = int.from_bytes(f.read(4), byteorder="little")
            self.assertEqual(MAGIC, record_magic)
            record_device_apps_type = int.from_bytes(f.read(2), byteorder="little")
            self.assertEqual(DEVICE_APPS_TYPE, record_device_apps_type)
            msg_len = int.from_bytes(f.read(2), byteorder="little")

            self.assertTrue(msg_len > 0)

        # check magic, type, etc.

#    def test_read(self):
#        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
#        for i, d in enumerate(pb.deviceapps_xread_pb(TEST_FILE)):
#            self.assertEqual(d, self.deviceapps[i])
