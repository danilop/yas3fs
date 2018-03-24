import argparse
import boto
import mock
from moto import mock_iam_deprecated, mock_s3_deprecated, mock_sns_deprecated, mock_sqs_deprecated
from multiprocessing import Process
from os import mkdir, devnull
import pdb
import pprint
from random import randint
from shutil import rmtree
from subprocess import call
from tempfile import mkdtemp
from tests import utils
from time import sleep
# from threading import Thread
from unittest import TestCase
import yas3fs

pp = pprint.PrettyPrinter(indent=1)


class testPR133(TestCase):
    """Per https://github.com/danilop/yas3fs/pull/133 \
Fix deadlock when a path is re-locked while the cache is locked globally"""
    # just in case:
    # $ mount | grep 'yas3fs-pr133-test-' | cut -d ' ' -f 3 | xargs -n 1 fusermount -uz
    tempdir = mkdtemp(prefix='yas3fs-pr133-test-')

    def send_reset(self, sns):
        # Occasionaly reset_all for a minute
        sleep_time = 3
        noof_sleeps = 60 // sleep_time

        while (noof_sleeps):
            noof_sleeps -= 1
            sleep(sleep_time)
            print('sending reset...')
            sns.publish(topic='arn:aws:sns:us-east-1:123456789012:pr133',
                        message='{"default": "[\"\", \"reset\"]"}'.encode('ascii'))

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
                        foreground=True,
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
    def spawn_yas3fs(self, mock_args):
        mkdir("{}/cache".format(self.tempdir))
        mkdir("{}/mount".format(self.tempdir))

        sns = boto.connect_sns()
        sns.create_topic('pr133')

        s3 = boto.connect_s3()
        s3.create_bucket('pr133')

        # antagonizer = Thread(target=self.send_reset, args=(sns,))
        # antagonizer.setDaemon(True)
        # antagonizer.start()

        yas3fs.main()

        # antagonizer.join()

    def setUp(self):
        self.yas3fs_proc = Process(target=self.spawn_yas3fs)
        self.yas3fs_proc.start()

    def teardown(self):
        self.yas3fs_proc.kill()
        call(['fusermount', '-uz', "{}/mount".format(self.tempdir)])
        rmtree(self.tempdir)

    def test_deadlock(self):
        print('tmpdir {}'.format(self.tempdir))
        sleep(15)  # let YAS3FS boot

        pdb.set_trace()

        # call(['dd', 'if=/dev/urandom', "of={}/mount/file".format(self.tempdir),
        #       'bs=32M', 'count=1'], stderr=open(devnull, 'w'))
        # # read randomly from `./mount/file` until the read blocks?
        # while True:
        #     try:
        #         with utils.timeout(seconds=10):
        #             with open("{}/mount/file".format(self.tempdir), "rb") as s3file:
        #                 s3file.seek(randint(0, 3.2e+7))
        #                 s3file.read(randint(512, 1e+6))
        #     except utils.timeout.TimeoutError:
        #         pdb.set_trace()
        # pdb.set_trace()

        self.assertTrue(1 == 1)
