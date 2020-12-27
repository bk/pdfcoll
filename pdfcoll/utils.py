# encoding=utf8

import contextlib
import os
import stat
import tempfile
import sys
import re
from html.parser import HTMLParser
import translitcodec
import hashlib


def slugify(s, fallback=u'missingslug', join_with='-', max_len=40):
    """
    Generates an ASCII-only slug from the input string. If the generation
    fails (i.e. if the input has no word characters), it returns the
    `fallback` string. Words are joined using the character/string specified
    in `join_with`. A `max_len` may be specified (default: 40 characters).
    """
    if isinstance(s, str):
        s = s.decode('utf-8')
    s = unescape_entities(s)
    cleaned = s.encode('translit/long').lower()
    words = [_ for _ in re.split(r'\W+', cleaned) if _]
    ret = unicode(join_with.join(words)) or fallback
    return ret[:max_len]


def unescape_entities(html):
    "Converts HTML entities (named as well as numeric) to Unicode"
    if not html:
        return ''
    h = HTMLParser()
    return h.unescape(html)


def vprint(msg):
    "Prints to stderr - like `warn` in perl"
    sys.stderr.write(msg + "\n")


def sha1sum(fn):
    """
    Returns the SHA1 hexdigest of the contents of a file.
    Not suitable for very large files.
    """
    with open(fn) as f:
        return hashlib.sha1(f.read()).hexdigest()


# The following is from http://code.activestate.com/recipes/579097-safely-and-atomically-write-to-a-file/

@contextlib.contextmanager
def atomic_write(filename, text=True, keep=True,
                 owner=None, group=None, perms=None,
                 suffix='.bak', prefix='tmp'):
    """Context manager for overwriting a file atomically.

    Usage:

    >>> with atomic_write("myfile.txt") as f:  # doctest: +SKIP
    ...     f.write("data")

    The context manager opens a temporary file for writing in the same
    directory as `filename`. On cleanly exiting the with-block, the temp
    file is renamed to the given filename. If the original file already
    exists, it will be overwritten and any existing contents replaced.

    (On POSIX systems, the rename is atomic. Other operating systems may
    not support atomic renames, in which case the function name is
    misleading.)

    If an uncaught exception occurs inside the with-block, the original
    file is left untouched. By default the temporary file is also
    preserved, for diagnosis or data recovery. To delete the temp file,
    pass `keep=False`. Any errors in deleting the temp file are ignored.

    By default, the temp file is opened in text mode. To use binary mode,
    pass `text=False` as an argument. On some operating systems, this make
    no difference.

    The temporary file is readable and writable only by the creating user.
    By default, the original ownership and access permissions of `filename`
    are restored after a successful rename. If `owner`, `group` or `perms`
    are specified and are not None, the file owner, group or permissions
    are set to the given numeric value(s). If they are not specified, or
    are None, the appropriate value is taken from the original file (which
    must exist).

    By default, the temp file will have a name starting with "tmp" and
    ending with ".bak". You can vary that by passing strings as the
    `suffix` and `prefix` arguments.
    """
    t = (uid, gid, mod) = (owner, group, perms)
    if any(x is None for x in t):
        info = os.stat(filename)
        if uid is None:
            uid = info.st_uid
        if gid is None:
            gid = info.st_gid
        if mod is None:
            mod = stat.S_IMODE(info.st_mode)
    path = os.path.dirname(filename)
    fd, tmp = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=path, text=text)
    try:
        replace = os.replace  # Python 3.3 and better.
    except AttributeError:
        # Atomic on POSIX. Not sure about Cygwin, OS/2 or others.
        replace = os.rename
    try:
        with os.fdopen(fd, 'w' if text else 'wb') as f:
            yield f
        # Perform an atomic rename (if possible). This will be atomic on 
        # POSIX systems, and Windows for Python 3.3 or higher.
        replace(tmp, filename)
        tmp = None
        os.chown(filename, uid, gid)
        os.chmod(filename, mod)
    finally:
        if (tmp is not None) and (not keep):
            # Silently delete the temporary file. Ignore any errors.
            try:
                os.unlink(tmp)
            except:
                pass
