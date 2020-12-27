# encoding=utf8

import hashlib
import datetime
import os
import shutil
import yaml
from urllib.request import Request, urlopen
import json
import subprocess

import bibtexparser
from bibtexparser.bparser import BibTexParser
from bibtexparser.customization import convert_to_unicode

from .meta import Meta, MetaItem
from .sqlite_fts import FTS
from .utils import sha1sum
from . import config


def maybe_bibrec(bibrec):
    """
    Return a dict embodying a BibTeX record, if passed a valid string
    representation of it.
    """
    if not bibrec:
        return None
    parser = BibTexParser()
    parser.customizations = convert_to_unicode
    try:
        bib_database = bibtexparser.loads(bibrec, parser=parser)
        return bib_database.entries[0]
    except:
        return None


class AlreadyHaveFile(Exception):
    pass


class Archiver(object):
    """
    Archive pdf files and place them into a collection.

    Usage:

        from pdfcoll.archiver import Archiver
        arch = Archiver()
        arch.archive(pdf_filename)
    """
    def __init__(self, basedir=None, meta_file=None,
                 makefile_template=None, no_fts_index=False):
        self.basedir = basedir or config.DEFAULT_BASEDIR
        self.meta_file = meta_file or config.DEFAULT_META_FILE
        self.makefile_template = makefile_template \
            or config.DEFAULT_META_TEMPLATE
        if not self.makefile_template.startswith('/'):
            self.makefile_template = os.path.join(
                self.basedir,
                self.makefile_template)
        self.no_fts_index = no_fts_index

    def archive(self, fn, *args, **kwargs):
        """
        Hardlink/copy pdf file to collection directory, explode it into pages
        and convert each page into a .txt file. Also, create a meta.yml file
        with information extracted from the pdf file and/or found based on
        detected doi or isbn.
        """
        if not fn.lower().endswith('.pdf'):
            raise ValueError(
                "'{}' is not a PDF file (based on extension)".format(fn))
        sha1 = sha1sum(fn)
        dir_path = self._maybe_mkdir(sha1)
        new_fn = os.path.join(dir_path, (sha1 + ".pdf"))
        if os.path.exists(new_fn):
            raise AlreadyHaveFile(new_fn)
        mi = MetaItem(sha1, self._init_meta(sha1, fn))
        try:
            os.link(fn, new_fn)
        except OSError:
            shutil.copyfile(fn, new_fn)
        pages, doi, isbn = self._pdf_processing(dir_path, sha1, fn)
        mi['pages'] = pages
        tags = []
        for item in args:
            if re.match(r'\w+: ', item):
                key, val = re.split(r':\s+', item, 1)
                mi[key] = val
            else:
                tags.append(item)
        for key in kwargs:
            mi[key] = kwargs[key]
        if tags:
            mi['tags'] = tags
        # The Perl version renames the PDF file based on doi/isbn -- that is
        # not done here. Instead, we just look up the info and add it to the
        # MetaInfo object.
        self._pdfinfo_lookup(new_fn, mi)
        self._doi_lookup(doi, mi)
        self._isbn_lookup(isbn, mi)
        os.chdir(dir_path)
        mi['full_path'] = new_fn
        mi['filename'] = os.path.basename(new_fn)
        # Try to set basic fields such as author, title, etc.,
        # based on already-available but disorganized information
        self._standardize_meta(mi)
        mi.write()
        if not self.no_fts_index:
            fts = FTS(basedir=self.basedir, meta_file=self.meta_file)
            fts.index_bundle(sha1)
        # Do some housekeeping: Add a Makefile to the directory and
        # create a zipfile. Then call make clean so as to remove the
        # temporary pdf and text files.
        os.link(self.makefile_template, "Makefile")
        cmd = "zip " + sha1 + ".txt.zip *.page_[0-9][0-9][0-9][0-9].txt"
        os.system(cmd)
        os.system("make clean")
        return mi

    def _maybe_mkdir(self, sha1):
        "If needed, create directory for the files belonging to given sha1."
        subdir = sha1[:2]
        subdir_path = os.path.join(self.basedir, subdir)
        if not os.path.exists(subdir_path):
            os.mkdir(subdir_path)
        dir_path = os.path.join(subdir_path, sha1)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        return dir_path

    def _init_meta(self, sha1, fn):
        """
        Inspects file named in `fn` and returns data for MetaItem.
        """
        stat = os.stat(fn)
        size = stat.st_size
        mtime = stat.st_mtime
        ts = datetime.datetime.fromtimestamp(mtime)
        now = datetime.datetime.now()
        basename = os.path.basename(fn)
        return {
            'original_filename': fn,
            'sha1': sha1,
            'file_ts': str(ts),
            'new_ts': str(now),
            'bytes': size,
        }

    def _pdf_processing(self, dir_path, sha1, orig_fn):
        """
        Bursts the pdf file into pages, converts them to text and looks for DOI
        or ISBN identifiers.
        """
        doi = None
        isbn = None
        os.chdir(dir_path)
        cmd = ' '.join(
            "pdftk",
            (sha1 + '.pdf'),
            "burst",
            "output",
            (sha1 + ".page_%04d.pdf"))
        os.system(cmd)
        pages = [_ for _ in os.listdir('.')
                 if re.match(sha1 + r'\.page_\d+\.pdf$', _)]
        page_count = len(pages)
        for page in pages:
            txt_fn = page.replace('.pdf', '.txt')
            num = int(re.match(r'_(\d+)\.pdf$', page).group(1))
            os.system("pdftotext " + page)
            # don't look for DOI after the first 2 pages;
            # don't look for ISBN after the first 9 pages.
            if num < 10 and not (doi or isbn):
                with open(txt_fn) as f:
                    txt = f.read()
                    if num < 3 and not (doi or isbn):
                        found_doi = re.search(
                            r'\b[Dd][Oo][Ii]:?\s*(\S+)/', txt)
                        if found_doi:
                            doi = found_doi.group(1)
                    found_isbn = re.search(
                        r'\b[Ii][Ss][Bb][Nn](?:10|13)?:?\s*(\S+)',
                        txt)
                    if found_isbn:
                        isbn = found_isbn.group(1)
                        isbn = re.sub(r'\D', '', isbn)
                        if len(isbn) not in (10, 13):
                            isbn = None
                    if doi or isbn:
                        break
        # Fallback: check for DOI/ISBN in filename
        if not doi:
            orig_fn = re.sub(r'%2f', '/', orig_fn)
            found_doi = re.search(r'[Dd][Oo][Ii].?(10\.[\w\-/\.]+)/', orig_fn)
            if found_doi:
                doi = found_doi.group(1)
        if not isbn:
            orig_fn = re.sub(r'\-', '', orig_fn)
            found_isbn = re.search(
                r'[Ii][Ss][Bb][Nn]\D?(\d{10,13})\b', orig_fn)
            if found_isbn:
                isbn = found_isbn.group(1)
        return (page_count, doi, isbn)

    def _doi_lookup(self, doi, mi):
        """
        Looks up the DOI, if any, and adds it (as well as retrieved bibtex
        info) to the MetaInfo object.

        Note that unlike the Perl version, no *.bib file is written to the
        current directory.
        """
        if not doi:
            return
        mi['doi'] = doi
        mi['doi_is_auto'] = True
        bib = None
        bibkey = None
        req = Request(
            'http://dx.doi.org/' + doi,
            headers={'Accept': 'text/bibliography; style=bibtex'})
        content = urlopen(req).read()
        if content:
            bib = maybe_bibrec(content)
            if bib:
                bibkey = bib['ID']
            content = re.sub(r'\s+', ' ', content)
            mi['bibrec'] = content.strip()
            if bibkey:
                mi['bibkey'] = bibkey
            if bib:
                mi['parsed_bibrec'] = bib

    def _isbn_lookup(self, isbn, mi):
        """
        Looks up the ISBN, if any, and adds it to the MetaInfo object.
        """
        if not isbn:
            return
        data = None
        # Gets data from Google Books
        url = 'https://www.googleapis.com/books/v1/volumes?q=isbn:' + isbn
        content = urlopen(url).read()
        if content:
            try:
                data = json.loads(content)
            except:
                pass
        if data and data['items']:
            mi['isbn_info'] = data
            mi['isbn_info_source'] = 'Google Books'
        else:
            # Fallback: get data from ottobib.com
            url = "http://www.ottobib.com/isbn/" + isbn + "/bibtex"
            content = urlopen(url).read()
            if content and content.find('textarea') > -1:
                content = re.sub(r'^[\S\s]*<textarea[^>]+>', '', content)
                content = re.sub(r'</textarea>[\S\s]*$', '', content)
                if content:
                    data = maybe_bibrec(content)
                    if data:
                        data['bibrec'] = content
                        data = {'ottobib': data}
            if data:
                mi['isbn_info'] = data
                mi['isbn_info_source'] = 'OttoBib'

    def _get_pdfinfo(self, new_fn, mi):
        info = subprocess.check_output('pdfinfo', new_fn)
        ret = {}
        for line in info.split('\n'):
            if not re.match(r'^\w', line):
                continue
            kv = re.split(r':\s+', line, 1)
            if len(kv) == 2 and kv[0]:
                ret[unicode(kv[0], 'utf8')] = unicode(kv[1], 'utf8')
        if ret:
            return ret

    def _standardize_meta(self, mi):
        """
        Try to set author, title, subtitle, summary, year, keywords
        in meta item (based on bibrec/doi/isbn info).

        Corresponds to update_core_info() in the Perl version.
        """
        author = ''
        title = ''
        subtitle = ''
        summary = ''
        year = None
        keywords = []
        chk = {}
        bibrec = maybe_bibrec(mi.get('bibrec', None)) or {}
        pdfinfo = mi.get('pdfinfo', {'Creator': ''})
        pi_title = ''
        pi_author = ''
        # We trust the Internet Archive to provide appropriate Title
        # and Author in pdfinfo; others: not so much.
        if pdfinfo['Creator'].find('Internet Archive') > -1:
            pi_title = pdfinfo.get('Title', '')
            pi_author = pdfinfo.get('Author', '')
        aitems = []
        if 'items' in mi.get('isbn_info', {}):
            for outer in mi['isbn_info']['items']:
                it = outer.get('volumeInfo', {})
                rec = {}
                if it.get('authors', None):
                    rec['authors'] = it['authors']
                    if isinstance(it['authors'], list):
                        if len(it['authors']) == 1:
                            rec['authors'] = it['authors'][0]
                kkmap = [('title', 'title'), ('subtitle', 'subtitle'),
                         ('publisedDate', 'year'), ('categories', 'keywords'),
                         ('description', 'summary')]
                for src, trg in kkmap:
                    if src in it:
                        rec[trg] = it[src]
                if rec:
                    aitems.append(rec)
        elif 'ottobib' in mi.get('isbn_info', {}):
            aitems.append(mi['isbn_info']['ottobib'])
        aitem = aitems[0] if aitems else {}
        chk['author'] = bibrec.get('author', aitem.get('author', pi_author))
        chk['title'] = bibrec.get('title', aitem.get('title', pi_title))
        chk['subtitle'] = aitem.get('subtitle', '')
        chk['year'] = bibrec.get('year', aitem.get('year', None))
        chk['keywords'] = aitem.get('keywords', [])
        chk['summary'] = aitem.get('summary', '')
        for k in chk:
            if chk[k]:
                mi[k] = chk[k]
