try:
    import cPickle as pickle
except ImportError:
    import pickle
import atexit
from functools import wraps
from path import path
import logging

logger = logging.getLogger('amitu.stats')
hdlr = logging.FileHandler('/tmp/stats.log')
formatter = logging.Formatter(
    '%(asctime)s %(levelname)s: %(filename)s:%(lineno)s %(message)s'
)
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

STACK_FILE = path("stack.dat")
WRITE_ALWAYS = False
STACK = {}

def load_stack():
    global STACK
    logger.debug("Loading stack")
    if STACK_FILE.exists():
        logger.info("Loaded form stack")
        STACK.update(pickle.load(STACK_FILE.open()))
    else:
        logger.info("Stack file not found")

def write_stack():
    logger.info("Writing stack file")
    pickle.dump(STACK, file(STACK_FILE, "w"))

def gen(key_or_func):
    """
    gen decorator
    =============

    @gen
    def total_lines_in_log():
        return len(file("log.txt").readlines()) # possibly lot of work

    In this case gen will call total_lines_in_log the first time, the method is 
    called, and its output is stored in "cache". Subsequent calls will return
    value from cache.

    If total_lines_in_log is called with keyword argument force_gen=True, then
    it wont look into cache and always compute.

    In this case there was no parameters passed to function.

    @gen("loglines_$filename")
    def total_lines_in_log(filename):
        return len(file(filename).readlines()) # possibly lot of work

    In this case filename passed would be used to create a key, based on format
    "loglines_$filename" format, and if that key exists in cache, then its value
    would be returned, else computed.

    """
    if callable(key_or_func):
        @wraps(key_or_func)
        def decorated(force_gen=False):
            name = key_or_func.__name__
            if name in STACK and not force_gen:
                logger.info("%s found in stack, returning" % name)
                return STACK[name]
            else:
                if force_gen:
                    logger.info("%s forced, computing" % name)
                else:
                    logger.info("%s not found in stack, computing" % name)
                val = key_or_func()
                STACK[name] = val
                if WRITE_ALWAYS:
                    write_stack()
                return val
        return decorated
    else:
        def decorator(func):
            key = string.Template(key_or_func)
            @wraps(func)
            def decorated(**kw):
                force_gen = kw.pop("force_gen", False)
                name = key.substitute(kw)
                if name in STACK and not force_gen:
                    logger.info("%s found in stack, returning" % name)
                    return STACK[name]
                else:
                    if force_gen:
                        logger.info("%s forced, computing" % name)
                    else:
                        logger.info("%s not found in stack, computing" % name)
                    val = func(**kw)
                    STACK[name] = val
                    if WRITE_ALWAYS:
                        write_stack()
                    return val
            return decorated
        return decorator

def init(write_always=False):
    if write_always:
        WRITE_ALWAYS = True
    else:
	   atexit.register(write_stack)
    load_stack()
    logger.info("initialized with %s", STACK_FILE.abspath())