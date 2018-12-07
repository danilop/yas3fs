import copy
import logging
import time
import threading
from .FSData import FSData


class LinkedListElement():
    """ The element of a linked list."""
    def __init__(self, value, next=None):
        self.value = value
        if next:
            self.append(next)
        else:
            self.next = None
            self.prev = None

    def delete(self):
        self.prev.next = self.next
        self.next.prev = self.prev
        return self.value

    def append(self, next):
        self.prev = next.prev
        self.next = next
        next.prev.next = self
        next.prev = self


class LinkedList():
    """ A linked list that is used by yas3fs as a LRU index
    for the file system cache."""
    def __init__(self):
        self.tail = LinkedListElement(None)
        self.head = LinkedListElement(None)
        self.head.next = self.tail
        self.tail.prev = self.head
        self.index = {}
        self.lock = threading.RLock()

    def append(self, value):
        with self.lock:
            if value not in self.index:
                new = LinkedListElement(value, self.tail)
                self.index[value] = new

    def popleft(self):
        with self.lock:
            if self.head.next != self.tail:
                value = self.head.next.delete()
                del self.index[value]
                return value
            else:
                return None

    def delete(self, value):
        with self.lock:
            if value in self.index:
                self.index[value].delete()
                del self.index[value]

    def move_to_the_tail(self, value):
        with self.lock:
            if value in self.index:
                old = self.index[value]
                old.delete()
                old.append(self.tail)


class FSCache():
    """ File System Cache """
    def __init__(self, cache_path=None):
        self.logger = logging.getLogger('yas3fs')
        self.cache_path = cache_path
        self.lock = threading.RLock()
        self.disk_lock = threading.RLock()  # To safely remove empty disk directories
        self.data_size_lock = threading.RLock()
        self.reset_all()

    def reset_all(self):
        with self.lock:
            self.entries = {}
            # New locks (still) without entry in the cache
            self.new_locks = {}
            # Paths with unused locks that will be removed on the next purge if remain unused
            self.unused_locks = {}
            self.lru = LinkedList()
            self.size = {}
            for store in FSData.stores:
                self.size[store] = 0

    def get_memory_usage(self):
        return [len(self.entries)] + [self.size[store] for store in FSData.stores]

    def get_cache_filename(self, path):
        if isinstance(path, bytes):
            path = path.decode('utf-8')
        return self.cache_path + '/files' + path   # path begins with '/'

    def get_cache_etags_filename(self, path):
        if isinstance(path, bytes):
            path = path.decode('utf-8')
        return self.cache_path + '/etags' + path   # path begins with '/'

    def is_deleting(self, path, prop='deleting'):
        if not self.has(path, prop):
            return False
        if self.get(path, prop) == 0:
            return False
        return True

    def is_ready(self, path, proplist=None):
        return self.wait_until_cleared(path, proplist=proplist)

    def wait_until_cleared(self, path, proplist=None, max_retries=10, wait_time=1):
        default_proplist = ['deleting', 's3_busy']
        if proplist is None:
            proplist = default_proplist

        for prop in proplist:
            if not self.has(path, prop):
                continue
            cleared = False
            for check_count in range(0, max_retries):
                if check_count:
                    self.logger.debug("wait_until_cleared %s found something for %s. (%i) " %
                                      (prop, path, check_count))
                # the cache/key disappeared
                if not self.has(path, prop):
                    self.logger.debug("wait_until_cleared %s did not find %s anymore." %
                                      (prop, path))
                    cleared = True
                    break
                # the cache got a '.dec()' from do_on_s3_now...
                if self.get(path, prop) == 0:
                    self.logger.debug("wait_until_cleared %s got all dec for %s anymore." %
                                      (prop, path))
                    cleared = True
                    break
                time.sleep(wait_time)

            if not cleared:
                # import inspect
                # inspect_stack = inspect.stack()
                # self.logger.critical("WAIT_UNTIL_CLEARED stack: '%s'"% pp.pformat(inspect_stack))

                self.logger.error("wait_until_cleared %s could not clear '%s'" % (prop, path))
                raise Exception("Path has not yet been cleared but operation wants to happen on it '%s' '%s'" %
                                (prop, path))
        return True

    def get_lock(self, path, skip_is_ready=False, wait_until_cleared_proplist=None):
        if not skip_is_ready:
            self.is_ready(path, proplist=wait_until_cleared_proplist)

        # Work around a deadlock when operations such as flush_all_caches lock the global cache
        # and do operations that perform path-based locks while a worker holding onto a path-based
        # lock tries to re-lock it. The global operation will block waiting for the worker to give
        # the path-based lock, while the worker blocks waiting for the global lock, completely
        # locking up the FS
        entry = self.entries.get(path) or {}
        existing_lock = entry.get('lock')
        if existing_lock and existing_lock._RLock__owner == threading.current_thread().ident:
            # figure out what this is called with then spam calls to get_lock to maybe trigger it?
            return existing_lock

        with self.lock:  # Global cache lock, used only for giving file-level locks

            try:
                lock = self.entries[path]['lock']
                return lock
            except KeyError:
                try:
                    return self.new_locks[path]
                except KeyError:
                    new_lock = threading.RLock()
                    self.new_locks[path] = new_lock
                    return new_lock

    def add(self, path):
        with self.get_lock(path):
            if path not in self.entries:
                self.entries[path] = {}
                self.entries[path]['lock'] = self.new_locks[path]
                del self.new_locks[path]
                self.lru.append(path)

    def delete(self, path, prop=None):
        with self.get_lock(path):
            if path in self.entries:
                if prop is None:
                    for p in list(self.entries[path].keys()):
                        self.delete(path, p)
                    del self.entries[path]
                    self.lru.delete(path)
                else:
                    if prop in self.entries[path]:
                        if prop == 'data':
                            data = self.entries[path][prop]
                            data.delete()  # To clean stuff, e.g. remove cache files
                        elif prop == 'lock':
                            # Preserve lock, let the unused locks check remove it later
                            self.new_locks[path] = self.entries[path][prop]
                        del self.entries[path][prop]

    def rename(self, path, new_path):
        with self.get_lock(path) and self.get_lock(new_path):
            if path in self.entries:
                self.delete(path, 'key')  # Cannot be renamed
                self.delete(new_path)  # Assume overwrite
                if 'data' in self.entries[path]:
                    data = self.entries[path]['data']
                    with data.get_lock():
                        data.rename(new_path)
                self.entries[new_path] = copy.copy(self.entries[path])
                self.lru.append(new_path)
                self.lru.delete(path)

# 6.59 working except rename...
#               del self.entries[path]  # So that the next reset doesn't delete the entry props

                self.inc(path, 'deleting')
                self.inc(new_path, 's3_busy')

    def get(self, path, prop=None):
        self.lru.move_to_the_tail(path)  # Move to the tail of the LRU cache
        try:
            if prop is None:
                return self.entries[path]
            else:
                return self.entries[path][prop]
        except KeyError:
            return None

    def set(self, path, prop, value):
        self.lru.move_to_the_tail(path)  # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
                if prop in self.entries[path]:
                    self.delete(path, prop)
                self.entries[path][prop] = value
                return True
            return False

    def inc(self, path, prop):
        self.lru.move_to_the_tail(path)  # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
                try:
                    self.entries[path][prop] += 1
                except KeyError:
                    self.entries[path][prop] = 1

    def dec(self, path, prop):
        self.lru.move_to_the_tail(path)  # Move to the tail of the LRU cache
        with self.get_lock(path):
            if path in self.entries:
                try:
                    if self.entries[path][prop] > 1:
                        self.entries[path][prop] -= 1
                    else:
                        del self.entries[path][prop]
                except KeyError:
                    pass  # Nothing to do

    def reset(self, path, with_deleting=True):
        with self.get_lock(path):
            self.delete(path)
            self.add(path)
            if with_deleting:
                self.inc(path, 'deleting')

    def has(self, path, prop=None):
        self.lru.move_to_the_tail(path)  # Move to the tail of the LRU cache
        if prop is None:
            return path in self.entries
        else:
            try:
                return prop in self.entries[path]
            except KeyError:
                return False

    def is_empty(self, path):  # To improve readability
        if self.has(path) and not self.has(path, 'attr'):
            return True
        else:
            return False
        # try:
        #     return len(self.get(path)) <= 1 # Empty or just with 'lock'
        # except TypeError: # if get returns None
        #     return False

    def is_not_empty(self, path):  # To improve readability
        if self.has(path) and self.has(path, 'attr'):
            return True
        else:
            return False
        # try:
        #     return len(self.get(path)) > 1 # More than just 'lock'
        # except TypeError: # if get returns None
        #     return False
