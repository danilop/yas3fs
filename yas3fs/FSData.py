import io
import os.path
import utils
import logging


class FSData():
    """The data (content) associated with a file."""
    stores = ['mem', 'disk']
    unknown_store = "Unknown store"

    def __init__(self, cache_path, cache, store, path):
        self.cache_path = cache_path
        self.cache = cache
        self.store = store
        self.path = path
        self.props = {}
        self.size = 0
        self.etag = None  # Something better ???
        self.logger = logging.getLogger('yas3fs')
        if store == 'mem':
            self.content = io.BytesIO()
        elif store == 'disk':
            previous_file = False
            filename = self.cache.get_cache_filename(self.path)
            if os.path.isfile(filename):
                self.logger.debug("found previous cache file '%s'" % filename)
                # There's a file already there
                self.content = open(filename, mode='rb+')
                self.update_size()
                self.content.close()
                self.set('new', None)  # Not sure it is the latest version
                # Now search for an etag file
                etag_filename = self.cache.get_cache_etags_filename(self.path)
                if os.path.isfile(etag_filename):
                    self.logger.debug("found previous cache etag file '%s'" % etag_filename)
                    with open(etag_filename, mode='r') as etag_file:
                        self.etag = etag_file.read()
                    previous_file = True
            if not previous_file:
                self.logger.debug("creating new cache file '%s'" % filename)
                with self.cache.disk_lock:
                    utils.create_dirs_for_file(filename)
                    # To create an empty file (and overwrite a previous file)
                    open(filename, mode='w').close()
                self.logger.debug("created new cache file '%s'" % filename)
            self.content = None  # Not open, yet
        else:
            raise FSData.unknown_store

    def get_lock(self, wait_until_cleared_proplist=None):
        return self.cache.get_lock(self.path, wait_until_cleared_proplist)

    def open(self):
        with self.get_lock():
            if not self.has('open'):
                if self.store == 'disk':
                    filename = self.cache.get_cache_filename(self.path)
                    self.content = open(filename, mode='rb+')
            self.inc('open')

    def close(self):
        with self.get_lock():
            self.dec('open')
            if not self.has('open'):
                if self.store == 'disk':
                    self.content.close()
                    self.content = None

    def update_etag(self, new_etag, wait_until_cleared_proplist=None):
        with self.get_lock(wait_until_cleared_proplist):
            if new_etag != self.etag:
                self.etag = new_etag
                if self.store == 'disk':
                    filename = self.cache.get_cache_etags_filename(self.path)
                    with self.cache.disk_lock:
                        utils.utils.create_dirs_for_file(filename)
                        with open(filename, mode='w') as etag_file:
                            etag_file.write(new_etag)

    def get_current_size(self):
        if self.content:
            with self.get_lock():
                self.content.seek(0, 2)
                return self.content.tell()
        else:
            return 0  # There's no content...

    def update_size(self, final=False):
        with self.get_lock():
            if final:
                current_size = 0  # The entry is to be deleted
            else:
                current_size = self.get_current_size()
            delta = current_size - self.size
            self.size = current_size
        with self.cache.data_size_lock:
            self.cache.size[self.store] += delta

    def get_content(self, wait_until_cleared_proplist=None):
        with self.get_lock(wait_until_cleared_proplist):
            if self.store == 'disk':
                filename = self.cache.get_cache_filename(self.path)
                return open(filename, mode='rb+')
            else:
                return self.content

    def get_content_as_string(self):
        if self.store == 'mem':
            with self.get_lock():
                return self.content.getvalue()
        elif self.store == 'disk':
            with self.get_lock():
                self.content.seek(0)  # Go to the beginning
                return self.content.read()
        else:
            raise FSData.unknown_store

    def has(self, prop):
        with self.get_lock():
            return prop in self.props

    def get(self, prop):
        with self.get_lock():
            try:
                return self.props[prop]
            except KeyError:
                return None

    def set(self, prop, value):
        with self.get_lock():
            self.props[prop] = value

    def inc(self, prop):
        with self.get_lock():
            try:
                self.props[prop] += 1
            except KeyError:
                self.props[prop] = 1

    def dec(self, prop):
        with self.get_lock():
            try:
                if self.props[prop] > 1:
                    self.props[prop] -= 1
                else:
                    del self.props[prop]
            except KeyError:
                pass  # Nothing to do

    def delete(self, prop=None, wait_until_cleared_proplist=None):
        with self.get_lock(wait_until_cleared_proplist):
            if prop is None:
                if self.store == 'disk':
                    filename = self.cache.get_cache_filename(self.path)
                    with self.cache.disk_lock:
                        if os.path.isfile(filename):
                            self.logger.debug("unlink cache file '%s'" % filename)
                            os.unlink(filename)
                            utils.remove_empty_dirs_for_file(self.cache_path, filename)
                    etag_filename = self.cache.get_cache_etags_filename(self.path)
                    with self.cache.disk_lock:
                        if os.path.isfile(etag_filename):
                            self.logger.debug("unlink cache etag file '%s'" % etag_filename)
                            os.unlink(etag_filename)
                            utils.remove_empty_dirs_for_file(self.cache_path, etag_filename)
                self.content = None  # If not
                self.update_size(True)
                for p in list(self.props.keys()):
                    self.delete(p)
            elif prop in self.props:
                if prop == 'range':
                    self.logger.debug('there is a range to delete')
                    data_range = self.get(prop)
                else:
                    data_range = None
                del self.props[prop]
                if data_range:
                    self.logger.debug('wake after range delete')
                    data_range.wake(False)  # To make downloading threads go on... and then exit

                # for https://github.com/danilop/yas3fs/issues/52
                if prop == 'change' and 'invoke_after_change' in self.props:
                    self.logger.debug('FSData.props[change] removed, now executing invoke_after_change lambda for: ' + self.path)
                    self.get('invoke_after_change')(self.path)
                    del self.props['invoke_after_change']  # cLeanup

    def rename(self, new_path):
        with self.get_lock():
            if self.store == 'disk':
                filename = self.cache.get_cache_filename(self.path)
                new_filename = self.cache.get_cache_filename(new_path)
                etag_filename = self.cache.get_cache_etags_filename(self.path)
                new_etag_filename = self.cache.get_cache_etags_filename(new_path)
                with self.cache.disk_lock:
                    utils.create_dirs_for_file(new_filename)
                    os.rename(filename, new_filename)
                with self.cache.disk_lock:
                    utils.remove_empty_dirs_for_file(self.cache_path, filename)
                if os.path.isfile(etag_filename):
                    with self.cache.disk_lock:
                        utils.create_dirs_for_file(new_etag_filename)
                        os.rename(etag_filename, new_etag_filename)
                    with self.cache.disk_lock:
                        utils.remove_empty_dirs_for_file(self.cache_path, etag_filename)
                if self.content:
                    self.content = open(new_filename, mode='rb+')
            self.path = new_path
