import argparse
import sys
try:
    import transaction
    HAVE_TRANSACTION = True
except ImportError:
    HAVE_TRANSACTION = False
from pyramid.paster import bootstrap

from rq import Connection, Queue, Worker
from rq.contrib.legacy import cleanup_ghosts
from rq.logutils import setup_loghandlers
from redis.exceptions import ConnectionError


class PyramidWorker(Worker):

    def __init__(self, environment, *a, **kw):
        super(PyramidWorker, self).__init__(*a, **kw)
        self.environment = environment

    def perform_job(self, job):
        self.procline('Initializing pyramid for %s from %s' %
                      (job.func_name, job.origin))
        try:
            success = super(PyramidWorker, self).perform_job(job)
            if HAVE_TRANSACTION:
                if success:
                    transaction.commit()
                else:
                    transaction.abort()
            return success
        except:
            if HAVE_TRANSACTION:
                transaction.abort()
            raise
        finally:
            self.environment['closer']()


def main():
    args = parse_args()

    if args.path:
        sys.path = args.path.split(':') + sys.path

    if args.verbose and args.quiet:
        raise RuntimeError("Flags --verbose and --quiet are mutually exclusive.")

    if args.verbose:
        level = 'DEBUG'
    elif args.quiet:
        level = 'WARNING'
    else:
        level = 'INFO'
    setup_loghandlers(level)

    # Set up Pyramid command-line environment
    environment = bootstrap(args.config)

    with Connection(get_redis_connection(environment['registry'])):
        cleanup_ghosts()
        start_worker(environment, args)


def parse_args():
    parser = argparse.ArgumentParser(description='Starts a Pyramid-aware RQ worker.')
    parser.add_argument('config', metavar='<ini-file>',
                        help='Configuration file (and optionally section)')
    parser.add_argument('--burst', '-b', action='store_true', default=False,
                        help='Run in burst mode (quit after all work is done)')
    parser.add_argument('--name', '-n', default=None,
                        help='Specify a different name')
    parser.add_argument('--path', '-P', default='.',
                        help='Specify the import path.')
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Show more output')
    parser.add_argument('--quiet', '-q', action='store_true', default=False,
                        help='Show less output')
    parser.add_argument('--sentry-dsn', action='store', default=None, metavar='URL',
                        help='Report exceptions to this Sentry DSN')
    parser.add_argument('queues', nargs='*', default=['default'],
                        help='The queues to listen on (default: \'default\')')
    return parser.parse_args()


def get_redis_connection(registry):
    settings = registry.settings
    if 'rq.redis' not in settings:
        sys.stderr.write('Critical error: pyramid_rq not configured by application\n')
        sys.exit(1)
    return settings['rq.redis']


def start_worker(environment, args):
    try:
        queues = list(map(Queue, args.queues))
        w = PyramidWorker(environment, queues, name=args.name)

        # Should we configure Sentry?
        if args.sentry_dsn:
            from raven import Client
            from rq.contrib.sentry import register_sentry
            client = Client(args.sentry_dsn)
            register_sentry(client, w)

        w.work(burst=args.burst)
    except ConnectionError as e:
        print(e)
        sys.exit(1)


if __name__ == '__main__':  # pragma: no coverage
    sys.exit(main() or 0)
