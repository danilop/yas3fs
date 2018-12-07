import threading


class Interval():
    """ Simple integer interval arthmetic."""
    def __init__(self):
        self.list = []  # A list of tuples

    def add(self, t):
        assert t[0] <= t[1]
        nl = []
        for i in self.list:
            i0 = i[0] - 1  # To take into account consecutive _integer_ intervals
            i1 = i[1] + 1  # Same as above
            if ((i0 <= t[0] and t[0] <= i1) or
               (i0 <= t[1] and t[1] <= i1) or
               (t[0] <= i[0] and i[1] <= t[1])):
                    t[0] = min(i[0], t[0])  # Enlarge t interval
                    t[1] = max(i[1], t[1])
            else:
                nl.append(i)
        nl.append(t)
        self.list = nl

    def contains(self, t):
        assert t[0] <= t[1]
        for i in self.list:
            if (i[0] <= t[0] and t[1] <= i[1]):
                return True
        return False

    def intersects(self, t):
        assert t[0] <= t[1]
        for i in self.list:
            if ((i[0] <= t[0] and t[0] <= i[1]) or
               (i[0] <= t[1] and t[1] <= i[1]) or
               (t[0] <= i[0] and i[1] <= t[1])):
                    return True
        return False


class FSRange():
    """A range used to manage buffered downloads from S3."""
    io_wait = 3.0  # 3 seconds

    def __init__(self):
        self.interval = Interval()
        self.ongoing_intervals = {}
        self.event = threading.Event()
        self.lock = threading.RLock()

    def wait(self):
        self.event.wait(self.io_wait)

    def wake(self, again=True):
        with self.lock:
            e = self.event
            if again:
                self.event = threading.Event()
            e.set()
