import argparse
import boto
import mock
from moto import mock_iam_deprecated, mock_s3_deprecated, mock_sns_deprecated, mock_sqs_deprecated
from os import mkdir
import pprint
from shutil import rmtree
from tempfile import mkdtemp
from tests import utils
from unittest import TestCase
import yas3fs


# import pdb


pp = pprint.PrettyPrinter(indent=1)


class testPR133(TestCase):
    """Fix deadlock when a path is re-locked while the cache is locked globally"""
    tempdir = mkdtemp()

    def setUp(self):
        mkdir("{}/cache".format(self.tempdir))
        mkdir("{}/mount".format(self.tempdir))

    def teardown(self):
        rmtree(self.tempdir)

    @mock.patch('argparse.ArgumentParser.parse_args',
                return_value=argparse.Namespace(
                    **utils.makeArgs(dict(
                        # test conditions from PR
                        cache_check=10,
                        cache_disk_size=20480,
                        cache_mem_size=2048,
                        cache_path="{}/cache".format(tempdir),
                        debug=True,
                        download_num=100,
                        download_retries_num=10,
                        log_mb_size=256,
                        log="{}/daemon.log".format(tempdir),
                        mountpoint="{}/mount".format(tempdir),
                        new_queue=True,
                        no_metadata=True,
                        read_retries_num=5,
                        region='us-east-1',
                        s3_num=100,
                        s3path='s3://pr133',
                        topic='arn:aws:sns:us-east-1:123456789012:pr133'))))
    @mock_iam_deprecated
    @mock_s3_deprecated
    @mock_sns_deprecated
    @mock_sqs_deprecated
    def test_command(self, mock_args):
        # pdb.set_trace()

        s3 = boto.connect_s3()
        s3.create_bucket('pr133')

        sns = boto.connect_sns()
        sns.create_topic('pr133')

        pp.pprint(yas3fs.main())

        self.assertTrue(1 == 1)
