import signal


def makeArgs(args):
    assert args['mountpoint'], "You must specify a mountpoint"
    assert args['s3path'], "You must specify a s3path"

    defaults = dict(
        # defaults that are expected
        aws_managed_encryption=False,
        buffer_prefetch=0,
        buffer_size=10240,
        cache_check=5,
        cache_disk_size=1024,
        cache_entries=10000,
        cache_mem_size=128,
        cache_on_disk=0,
        cache_path='',
        debug=False,
        download_num=4,
        download_retries_num=60,
        download_retries_sleep=1,
        expiration=30*24*60*60,
        foreground=False,
        log_backup_count=10,
        log_backup_gzip=False,
        log_mb_size=100,
        mkdir=False,
        mp_num=4,
        mp_retries=3,
        mp_size=100,
        new_queue_with_hostname=False,
        new_queue=False,
        no_allow_other=True,  # spares the need to edit /etc/fuse.conf
        no_metadata=False,
        nonempty=False,
        prefetch_num=2,
        prefetch=False,
        queue_polling=0,
        queue_wait=20,
        read_only=False,
        read_retries_num=10,
        read_retries_sleep=1,
        recheck_s3=False,
        region='us-east-1',  # moto doesn't care anyways
        requester_pays=False,
        s3_num=32,
        s3_retries_sleep=1,
        s3_retries=3,
        s3_use_sigv4=False,
        st_blksize=None,
        use_ec2_hostname=False,
        # parameters without defaults
        gid=False,
        hostname=False,
        id=False,
        port=False,
        queue=False,
        s3_endpoint=False,
        topic=False,
        uid=False,
        umask=False,
        version=False,
        with_plugin_class=False,
        with_plugin_file=False)

    for key, value in args.items():
        defaults[key] = value

    return defaults


class ShimTimeoutError(Exception):
    def __init__(self, message, errors):
        super(ShimTimeoutError, self).__init__(message)
        self.errors = errors


class timeout:
    try:
        TimeoutError = TimeoutError
    except NameError:
        TimeoutError = ShimTimeoutError

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise self.TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)
