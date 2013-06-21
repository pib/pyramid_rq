import redis
import rq


class job(object):
    def __init__(self, queue, connection=None, timeout=None,
            result_ttl=DEFAULT_RESULT_TTL):
        """A decorator that adds a ``delay`` method to the decorated function,
        which in turn creates a RQ job when called. Accepts a required
        ``queue`` argument that can be either a ``Queue`` instance or a string
        denoting the queue name.  For example:

            @job(queue='default')
            def simple_add(x, y):
                return x + y

            simple_add.delay(1, 2) # Puts simple_add function into queue
        """
        self.queue = queue
        self.connection = connection
        self.timeout = timeout
        self.result_ttl = result_ttl

    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):
            connection = resolve_connection(self.connection)
            if isinstance(self.queue, basestring):
                queue = Queue(name=self.queue, connection=connection)
            else:
                queue = self.queue
            return queue.enqueue_call(f, args=args, kwargs=kwargs,
                    timeout=self.timeout, result_ttl=self.result_ttl)
        f.delay = delay
        return f


def get_setting(config, key, default):
    settings = config.registry.settings
    for prefix in ['rq.redis', 'redis']:
        value = settings.get('%s.%s' % (prefix, key))
        if value is not None:
            return type(default)(value)
    else:
        return default


def rq_tween_factory(handler, registry):
    def rq_tween(request):
        with rq.Connection(registry.settings['rq.redis']):
            return handler(request)
    return rq_tween


def includeme(config):
    try:
        host = get_setting(config, 'host', 'localhost')
        port = get_setting(config, 'port', 6379)
        db = get_setting(config, 'db', 1)
    except ValueError:
        raise ValueError('Invalid rq/redis configuration')
    connection = redis.Redis(host=host, port=port, db=db)
    config.registry.settings['rq.redis'] = connection
    config.add_tween('pyramid_rq.rq_tween_factory')


__all__ = ['includeme']
