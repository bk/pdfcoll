# encoding=utf8

import yaml
import re
import os
import glob
import ftfy

from pdfcoll import config
from pdfcoll.utils import slugify, vprint, atomic_write


"""
Interface to pdfcollection meta.yml (and notes.md)

Usage summary
-------------

    from pdfcollection.meta import Meta
    m = Meta()

    # info and changes are MetaItem instances, i.e. lightly souped-up dicts;
    # sha1 is of course SHA1 sum which serves as ID
    info = m.read_meta(sha1)
    m.write_meta(sha1, info) # or: info.write()
    m.edit_meta(sha1, info, changes) # or: info.edit(changes)
    slug = m.get_slug(sha1, save=False, overwrite=False, ignore_saved=False)
    # or: slug = info.slug(save=False, overwrite=False, ignore_saved=False)
    m.set_slug(sha1, newslug) # or: info.set_slug(newslug)
"""


def _clean_perl_yaml(fc):
    "Prevents errors caused by content written by Perl module"
    fc = re.sub(r'!!perl/.*Boolean 0', 'false', fc)
    fc = re.sub(r'!!perl/.*Boolean 1', 'true', fc)
    return fc


class Meta(object):

    def __init__(self, basedir=None, meta_file=None, notes_file=None, verbose=False):
        self.basedir = basedir or config.DEFAULT_BASEDIR
        self.meta_file = meta_file or config.DEFAULT_META_FILE
        self.notes_file = notes_file or config.DEFAULT_NOTES_FILE
        self.verbose = verbose

    def expand_sha1(self, shortened_sha1):
        """
        Expands a shortened sha1 into the full 40 characters. Raises an error for
        ambiguous abbreviations, and None if no mathcing sha1 is found. The
        minimum length for a shortened sha1 is 4 characters.
        """
        if 4 < len(shortened_sha1) < 40:
            fglob = '%s/%s/%s*' % (
                self.basedir, shortened_sha1[:2], shortened_sha1)
            found = glob.glob(fglob)
            if found and len(found) > 1:
                raise ValueError('Shortened SHA %s is ambiguous' % shortened_sha1)
            elif found:
                return os.path.split(found[0])[1]
        return None

    def read_meta(self, sha1):
        """
        Reads a meta file given sha1. Returns contents as MetaItem
        (which essentially behaves as a dict).
        """
        fn = self.meta_path(sha1, True)
        fc = unicode(open(fn).read(), 'utf8')
        return MetaItem(sha1, yaml.load(_clean_perl_yaml(fc)))

    def meta_path(self, sha1, must_exist=True):
        return self.get_path(sha1, 'meta_file', must_exist)

    def notes_path(self, sha1, must_exist=True):
        return self.get_path(sha1, 'notes_file', must_exist)

    def get_path(self, sha1, key, must_exist=True):
        if key not in ('meta_file', 'notes_file'):
            raise ValueError('Key must be either meta_file or notes_file')
        fn = self.meta_file if key == 'meta_file' else self.notes_file
        fn = os.path.join(self.basedir, sha1[:2], sha1, fn)
        if must_exist and not os.path.exists(fn):
            raise ValueError('File not found: ' + fn)
        return fn

    def get_slug(self, sha1, save=False, overwrite=False, ignore_saved=False):
        """
        Get and/or generate a slug for the entry stored under `sha1`.
        """
        info = self.read_meta(sha1)
        # Return a pre-existing slug unless it is to be overwritten/ignored
        if info.get('slug', None):
            if save:
                save = overwrite
            if save:
                ignore_saved = False
            if not ignore_saved:
                return info['slug']
        # Assemble the elements for the automatic slug
        author = slugify(info.get('author', ''), '')
        title = slugify(info.get('title', ''), '')
        foundyear = re.search(r'\b(\d{4})\b', str(info.get('year', '')))
        year = foundyear.group(1) if foundyear else None
        bibkey = info.get('bibkey', None)
        if bibkey:
            bibkey = re.sub(r'\W', r'-', bibkey)
        partial_sha = sha1[:7]
        slug = '_'.join(
            [_ for _ in (author, title, year, bibkey, partial_sha) if _])
        if save and slug and info.get('slug', '') != slug:
            self.set_slug(sha1, slug)
        return slug

    def set_slug(self, sha1, slug):
        return self.edit_meta(sha1, {'slug': slug})

    def edit_meta(self, sha1, changes):
        """
        Adds or changes a set of keys in meta, the writes the changed entry;
        `changes` should be a dict.
        """
        info = self.read_meta(sha1)
        for k in changes:
            if self.verbose:
                vprint("Adding/updating key %s in meta for %s" % (k, sha1))
            info[k] = changes[k]
        return self.write_meta(sha1, info)

    def write_meta(self, sha1, info):
        """
        Writes a complete meta entry containing `info` (which should be a dict).
        """
        if not isinstance(info, MetaItem):
            info = MetaItem(sha1, info)
        fn = self.meta_path(sha1)
        info_str = yaml.dump(dict(info))
        if self.verbose:
            vprint("writing meta: %s" % fn)
        with atomic_write(fn) as f:
            f.write(info_str)
        return info

    def get_notes(self, sha1):
        fn = self.notes_path(sha1)
        try:
            f = open(fn)
            return f.read().decode('utf-8')
        except IOError, e:
            if self.verbose:
                vprint('WARNING: ' + e)
            return None


class MetaItem(dict):
    """
    Light wrapper around dict, representing the contents of a given
    meta.yml file.

    Properties:
        - sha1
        - path (corresponds to Meta.meta_path)

    Methods:
        - edit (corresponds to Meta.change_meta)
        - write (== Meta.write_meta)
        - slug (== Meta.get_slug)
        - set_slug (== Meta.set_slug)
    """

    def __init__(self, sha1, info):
        super(MetaItem, self).__init__(info)
        self.sha1 = sha1

    @property
    def path(self):
        return Meta().meta_path(self.sha1)

    def edit(self, changes):
        return Meta().change_meta(self.sha1, changes)

    def write(self, info):
        return Meta().write_meta(self.sha1, info)

    def slug(self, *args, **kwargs):
        return Meta().get_slug(self.sha1, *args, **kwargs)

    def set_slug(self, slug):
        return Meta().set_slug(self.sha1, slug)
