import errno
from .fuse import fuse_get_context
import logging
import os
from sys import exit
import time


# Utility functions
logger = logging.getLogger('yas3fs')


def error_and_exit(error, exitCode=1):
    logger.error(error + ", use -h for help.")
    exit(exitCode)


def create_dirs(dirname):
    logger.debug("create_dirs '%s'" % dirname)
    try:
        if not isinstance(dirname, bytes):
            dirname = dirname.encode('utf-8')

        os.makedirs(dirname)
        logger.debug("create_dirs '%s' done" % dirname)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dirname):
            logger.debug("create_dirs '%s' already there" % dirname)
            pass
        else:
            raise

    except Exception as exc:  # Python >2.5
        logger.debug("create_dirs '%s' ERROR %s" % (dirname, exc))
        raise


def remove_empty_dirs(cache_path, dirname):
    logger.debug("remove_empty_dirs '%s'" % (dirname))

    try:
        if not isinstance(dirname, str):
            # dirname must be a string for replace
            dirname = dirname.decode('utf-8')

        # fix for https://github.com/danilop/yas3fs/issues/150
        # remove cache_path part from dirname to avoid accidental removal of /tmp (if empty)
        os.chdir(cache_path)
        dirname = dirname.replace(cache_path + '/', '')

        dirname = dirname.encode('utf-8')
        os.removedirs(dirname)
        logger.debug("remove_empty_dirs '%s' done", dirname)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.ENOTEMPTY:
            logger.debug("remove_empty_dirs '%s' not empty", dirname)
            pass
        else:
            raise
    except Exception as e:
        logger.exception(e)
        logger.error("remove_empty_dirs exception: %s", dirname)
        raise e


def create_dirs_for_file(filename):
    logger.debug("create_dirs_for_file '%s'" % filename)
    if not isinstance(filename, bytes):
        filename = filename.encode('utf-8')

    dirname = os.path.dirname(filename)
    create_dirs(dirname)


def remove_empty_dirs_for_file(cache_path, filename):
    logger.debug("remove_empty_dirs_for_file '%s'" % filename)
    if not isinstance(filename, bytes):
        filename = filename.encode('utf-8')

    dirname = os.path.dirname(filename)
    remove_empty_dirs(cache_path, dirname)


def get_current_time():
    return time.mktime(time.gmtime())


def get_uid_gid():
    uid, gid, pid = fuse_get_context()
    return int(uid), int(gid)


def thread_is_not_alive(t):
    return t is None or not t.is_alive()


def custom_sys_excepthook(type, value, tb):
    logger.exception("Uncaught Exception: " + str(type) + " " + str(value) + " " + str(tb))
