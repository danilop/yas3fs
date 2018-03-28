import shutil
import tempfile
import threading
import time
import unittest
from yas3fs.FSCache import FSCache


class testPR133(unittest.TestCase):
    """Per https://github.com/danilop/yas3fs/pull/133 \
Fix deadlock when a path is re-locked while the cache is locked globally"""

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='yas3fs-pr133-test-')
        self.cache = FSCache(self.tempdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_deadlock(self):
        def getGlobalLock():
            with self.cache.lock:
                print('locked global')
                while(True):
                    time.sleep(10)

        def getPathLock():
            with self.cache.get_lock('pr133'):
                print('locked local')
                self.cache.entries['pr133'] = {}
                self.cache.entries['pr133']['lock'] = self.cache.new_locks['pr133']
                del self.cache.new_locks['pr133']
                time.sleep(2)
                print('attempting to relock local')
                with self.cache.get_lock('pr133'):
                    print('relocked local')

        # lock a path
        llock = threading.Thread(target=getPathLock)
        llock.start()
        time.sleep(1)

        # lock the world
        glock = threading.Thread(target=getGlobalLock)
        glock.setDaemon(True)
        glock.start()

        # wait a bit
        llock.join(timeout=3)
        # see if we've encountered a stalemate
        self.assertFalse(llock.isAlive(), 'Attempt to relock path failed')
