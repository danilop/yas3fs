import logging


class PartOfFSData():
    """ To read just a part of an existing FSData, inspired by FileChunkIO """
    def __init__(self, data, start, length):
        self.content = data.get_content()
        self.start = start
        self.length = length
        self.logger = logging.getLogger('yas3fs')
        self.pos = 0

        self.init_start = start
        self.init_length = length
        self.init_pos = 0

    def seek(self, offset, whence=0):
        self.logger.debug("seek '%s' '%i' '%i' " % (self.content, offset, whence))
        if whence == 0:
            self.pos = offset
        elif whence == 1:
            self.pos = self.pos + offset
        elif whence == 2:
            self.pos = self.length + offset

    def tell(self):
        return self.pos

    def read(self, n=-1):
        self.logger.debug("read '%i' '%s' at '%i' starting from '%i' for '%i'" % (n, self.content, self.pos, self.start, self.length))

        if n >= 0:
            n = min([n, self.length - self.pos])
            self.content.seek(self.start + self.pos)
            s = self.content.read(n)

            if len(s) != n:
                self.logger.error("read length not-equal! '%i' '%s' at '%i' starting from '%i' for '%i'  length of return ['%s] " % (n, self.content, self.pos, self.start, self.length, len(s)))

            self.pos += len(s)
            return s
        else:
            return self.readall()

    def readall(self):
        return self.read(self.length - self.pos)
