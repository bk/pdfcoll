# encoding=utf8

import yaml
import re
import os
import glob
import ftfy
import sqlite3
from collections import OrderedDict
from html.parser import HTMLParser

from . import config
from .utils import slugify, vprint
from .meta import Meta


def sql_quote(s):
    """
    Escape single quotes using sql conventions, and then surround string with
    single quotes.
    """
    s = s.replace("'", "''")
    return "'" + s + "'"


def selectall(dbh, sql, bind):
    """
    Run a select statement using a cursor and return the result a a list of
    rows.
    """
    c = dbh.cursor()
    if bind:
        c.execute(sql, bind)
    else:
        c.execute(sql)
    res = c.fetchall()
    c.close()
    return res


def do_sql(dbh, sql, bind=[], lastrowid=False):
    """
    Run a query and return the number of affected rows (-1 means that
    the query would never be able to affect rows). If lastrowid is true,
    return the id of the last row inserted using the cursor.
    """
    c = dbh.cursor()
    if bind:
        c.execute(sql, bind)
    else:
        c.execute(sql)
    rowcount = c.rowcount
    dbh.commit()
    if lastrowid:
        lastrowid = c.lastrowid
        c.close()
        return lastrowid
    else:
        c.close()
        return rowcount


class FTS(object):
    """
    Index and search a SQLite-based full-text index for the pdf collection.
    """

    def __init__(self, basedir=None, db_dir=None, db=None,
                 force_update=False, verbose=False):
        self.basedir = basedir or config.DEFAULT_BASEDIR
        self.db_dir = db_dir or os.path.join(self.basedir, 'var')
        self.db = db or config.DEFAULT_DB_FILE
        if self.db.find('/') == -1:
            self.db = os.path.join(self.db_dir, self.db)
        createdb = not os.path.exists(self.db)
        self.reftime = 0 if createdb else os.path.getmtime(self.db)
        self.dbh = sqlite3.connect(self.db)
        self.dbh.row_factory = sqlite3.Row
        c = self.dbh.cursor()
        c.execute('pragma encoding = "UTF-8"')
        self.dbh.commit()
        c.close()
        if createdb:
            self._create_schema()
        self.m = Meta(verbose=verbose)

    def _create_schema(self):
        sql_cmds = [
            """
            create table meta (
              meta_id integer primary key,
              folder_sha1 text,
              author text,
              title text,
              subtitle text,
              year int,
              pages int,
              summary text,
              isbn text,
              doi text,
              ts int,
              unique(folder_sha1))""",
            """
            create table page (
              page_id integer primary key,
              folder_sha1,
              file_name text,
              unique (file_name))""",
            """
            create virtual table fts_page
              using fts4 (
                file_contents)""",
            ]
        c = self.dbh.cursor()
        for sql in sql_cmds:
            c.execute(sql)
        self.dbh.commit()
        c.close()

    def search(self, query, search_meta=True, meta_queries=None,
               group_by_sha1=True):
        """
        Search for items in the index and optionally in meta info. Parameters:

        - `query` (string): query string

        - `search_meta` (boolean): search in basic meta information if True.
           Meta results have higher precedence than FTS results.

        - `meta_queries` (list of strings): Optional special queries for meta,
          different from the FTS search. Useful if the FTS search contains
          booleans or other special FTS operators.

        - group_by_sha1 (boolean): If True, returns the result as an
          OrderedDict grouped by folder_sha1, with each value a list.
          Otherwise, you get a simple list of Row objects.
        """
        meta_res = []
        if search_meta:
            if not meta_queries:
                meta_queries = [query]
            col_expr = "lower(' '||" \
                + "coalesce(author,'')||' '||coalesce(title,'')||' '||" \
                + "coalesce(subtitle, '')||' '||coalesce(summary,'')||' ')"
            like_query = (' OR ' + col_expr + ' LIKE ').join(
                [sql_quote('%' + _.lower() + '%') for _ in meta_queries])
            meta_sql = """
              select
                'meta' as type,
                meta_id,
                folder_sha1,
                author,
                title,
                subtitle,
                summary
              from meta
                where %s LIKE %s
              order by folder_sha1
            """ % (col_expr, like_query)
            meta_res = selectall(self.dbh, meta_sql, [])
        lquery = self._prepare_query(query)  # mainly lowercasing
        # TODO: create rank() function taking matchinfo(fts_page) as its
        # material, and order by that
        sql = """
          select
            'fts' as type,
            a.page_id,
            a.folder_sha1,
            a.file_name,
            snippet(fts_page) as snippet,
            offsets(fts_page) as offsets
          from page a join fts_page b
            on a.page_id = b.rowid
          where fts_page match ?
          order by 2, 3
        """
        res = selectall(self.dbh, sql, [lquery])
        if group_by_sha1:
            ret = OrderedDict()
            for row in (meta_res + res):
                if row['folder_sha1'] in ret:
                    ret[row['folder_sha1']].append(row)
                else:
                    ret[row['folder_sha1']] = [row]
            return ret
        else:
            return meta_res + res

    def _prepare_query(self, query):
        """
        Lowercases everything except boolean keywords; keeps OR, but eliminates
        AND.
        """
        # "standard" FTS syntax; not "enhanced"
        have_booleans = re.search(r' (?:OR|AND) ', query)
        if not have_booleans:
            return query.lower()
        lquery = u''
        while query:
            bool_atstart = re.match(r'(OR|AND)\s+', query)
            word_atstart = re.match(r'\s*(\S+)\s+', query)
            if bool_atstart:
                if bool_atstart.group(1) == 'OR':
                    lquery += 'OR '
                query = re.sub(r'^(?:OR|AND)\s+', '', query)
            elif word_atstart:
                lquery += word_atstart.group(1).lower() + ' '
                query = re.sub(r'^\s*(?:\S+)\s+', '', query)
            else:
                lquery += query
                break
        return lquery

    def index_all(self, meta_only=False):
        """
        Index whole collection (or only meta if `meta_only` is True).
        """
        subdirs = [d for d in os.listdir(self.basedir)
                   if re.match(r'^[0-9a-f][0-9a-f]$', d)]
        for sd in subdirs:
            bundles = [d for d in os.listdir(os.path.join(self.basedir, sd))
                       if re.match(r'^[0-9a-f]{40}$', d)]
            for bundle in bundles:
                if not meta_only:
                    self.index_bundle(bundle)
                self.index_meta(bundle)
        # Optimize and combine b-trees
        if getattr(self, '_indexed_bundles', None):
            # TODO: tell 'Optimizing...' if verbose
            sql = "INSERT INTO fts_page(fts_page) VALUES ('optimize')"
            rowcount = do_sql(self.dbh, sql)

    def index_meta(self, sha1, force_update=False):
        """
        Update SQLite-stored meta info for a pdf bundle.
        """
        sha1 = sha1.strip('/')
        dirname, mod_dir, mod_meta = self._bundleinfo(sha1)
        # before: foldar_sha1, after: ts
        core_fields = ('author', 'title', 'subtitle', 'year', 'pages',
                       'summary', 'isbn', 'doi')
        sql = 'select * from meta where folder = ?'
        found = selectall(self.dbh, sql, [sha1])
        rec = found[0] if found else {}
        mi = self.m.read_meta(sha1)
        meta_bind = []
        # The primary purpose here is to ensure that the author is a scalar
        for field in core_fields:
            val = u' ; '.join(mi[field]) \
                if isinstance(mi.get(field, None), list) \
                else mi.get(field, None)
            meta_bind.append(val)
        if not rec.get('meta_id', None):
            # insert:
            # TODO: warn " - $sha1 (meta - insert)\n" if $self->{verbose};
            sql = "insert into meta (filder_sha1, " \
                  + ", ".join(core_fields) \
                  + ", ts) values (?, ?,?,?,?,?,?,?,?, ?)"
            meta_bind.append(mod_meta)
            do_sql(self.dbh, sql, meta_bind)
        elif rec.get('ts', 0) < mod_meta or force_update:
            # update:
            # TODO: warn " - $sha1 (meta - update)\n" if $self->{verbose};
            sql = "update meta set " \
                  + " = ?, ".join(core_fields) \
                  + " = ?, ts = ? where meta_id = ? and folder_sha1 = ?"
            meta_bind += [mod_meta, rec['meta_id'], sha1]
            do_sql(self.dbh, sql, meta_bind)

    def expunge(self, sha1, meta_only=False):
        """
        Remove a given sha1 key from the index (both meta and fts, unless
        meta_only is True, in which case only meta).
        """
        if not sha1 and len(sha1) == 40:
            return
        if not meta_only:
            found = selectall(
                self.dbh,
                "select page_id from page where folder_sha1 = ?",
                [sha1])
            for row in found:
                self.delete_page(row['page_id'])
        do_sql("delete from meta where folder_sha1 = ?", [sha1])

    def _bundleinfo(self, sha1):
        """
        Get directory, its modification time and the mod time of the meta.yml
        file.
        """
        sha1 = re.sub(r'^.*/', '', sha1)  # remove possible directory path
        sd = sha1[:2]
        dirname = os.path.join(self.basedir, sd, sha1)
        if not os.path.isdir(dirname):
            print("WARNING:", dirname, "not found: not indexed")
            return
        # assumes that nothing in dir will change without either
        # adding/removing a file or updating meta.yml
        mod_dir = os.stat(dirname).st_mtime
        mod_meta = os.stat(os.path.join(dirname, 'meta.yml')).st_mtime
        return (dirname, mod_dir, mod_meta)

    def index_bundle(self, sha1):
        """
        Index a given directory/sha1 container.
        """
        dirname, mod_dir, mod_meta = self._bundleinfo(sha1)
        if self.reftime > mod_dir and self.reftime > mod_meta \
                and not self.force_update:
            return
        # TODO: warn "- $sha1\n" if $self->{verbose};
        if hasattr(self, '_indexed_bundles'):
            self._indexed_bundles += 1
        else:
            self._indexed_bundles = 1
        need_make = not os.path.exists(
            os.path.join(dirname, sha1 + '.page_0001.txt'))
        if need_make:
            os.chdir(dirname)
            os.system("make")
        files = [fn for fn in os.listdir(dirname)
                 if re.search('page_\d+\.txt$', fn)]
        for fn in files:
            with open(os.path.join(dirname, fn)) as f:
                text = f.read().decode('utf-8')
                self.index_page(sha1, fn, text)
        if need_make:
            os.system("make clean")

    def index_page(self, folder_sha1, file_name, text):
        """
        Index a single .txt file, which represents a page in a pdf file.
        """
        text = self._munge_text(text)
        # Only changes affecting the search object matter, so don't store
        # the text directly
        page_id = None
        found = selectall(
            self.dbh, "select page_id from page where file_name = ?",
            [file_name])
        if found:
            if not self.force_update:
                return
            page_id = found[0]['page_id']
        if page_id:
            do_sql(
                self.dbh,
                "update fts_page set file_contents = ? where rowid = ?",
                [text, page_id])
        else:
            page_id = do_sql(
                self.dbh,
                "insert into page (folder_sha1, file_name) values (?, ?)",
                [folder_sha1, file_name],
                lastrowid=True)
            do_sql(
                self.dbh,
                "insert into fts_page (rowid, file_contents) values (?, ?)",
                [page_id, text])

    def delete_page(self, page_id=None, file_name=None):
        """
        Remove a page from the index.
        """
        if not page_id or file_name:
            raise RuntimeError(
                'Need either page_id or file_name for delete_page')
        if not page_id:
            found = selectall(
                self.dbh,
                "select page_id from page where file_name = ?",
                [file_name])
            if found:
                page_id = found[0]['page_id']
            else:
                raise RuntimeError('Could not find page_id based on file_name')
        ret = do_sql(self.dbh, "delete from page where page_id = ?", [page_id])
        do_sql(self.dbh, "delete from fts_page where rowid = ?", [page_id])
        return int(ret) if ret else 0

    def _munge_text(self, s):
        # html/xml tags
        s = re.sub(r'<script.*</script>', '', s, re.I)
        s = re.sub(r'<style.*</style>', '', s, re.I)
        s = re.sub(r'<[^>]*>', ' ', s)
        # join hyphenated words at end of lines
        s = re.sub(r'(\S)-\n\r?\s*([a-z])', r'\1\2', s)
        # whitespace
        s = re.sub(r'\s+', ' ', s)
        s = s.strip()
        # html/xml entities + ligatures
        s = ftfy.fix_text(s)
        # lowercase
        return s.lower()
