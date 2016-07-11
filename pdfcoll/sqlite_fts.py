# encoding=utf8

import yaml
import re
import os
import glob
import ftfy
import sqlite3
from collections import OrderedDict

from pdfcoll import config
from pdfcoll.utils import slugify, vprint
from pdfcoll.meta import Meta

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
            col_expr = """lower(' '||coalesce(author,'')||' '||coalesce(title,'')||' '||coalesce(subtitle, '')||' '||coalesce(summary,'')||' ')"""
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
        lquery = self._prepare_query(query) # mainly lowercasing
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

    # TODO (unimplemented methods relative to Perl version):
    #
    #  - index_all
    #  - index_meta
    #  - expunge
    #  - _bundleinfo
    #  - index_bundle
    #  - index_page
    #  - delete_page
    #  - _munge_text
