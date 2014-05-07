#! /usr/bin/env python3.3
'''PrintHandler'''

import binascii
import os
import logging
import tempfile

from P4 import OutputHandler, P4Exception

import p4gf_git
from   p4gf_l10n                  import _
from   p4gf_p2g_rev_list          import RevList
from   p4gf_p4file                import P4File
import p4gf_progress_reporter     as     ProgressReporter
import p4gf_util

LOG = logging.getLogger('p4gf_copy_to_git').getChild('print_handler')


# pylint: disable=C0103,R0201
# C0103 Invalid name
# These names are imposed by P4Python
# R0201 Method could be a function
class PrintHandler(OutputHandler):
    """OutputHandler for p4 print, hashes files into git repo"""
    def __init__(self, ctx):
        OutputHandler.__init__(self)
        self.rev = None
        self.revs = RevList()
        self.tempfile = None
        self.p4 = ctx.p4
        self.p4gf = ctx.p4gf
        self.change_set = set()
        self.repo = ctx.view_repo
        self.ctx  = ctx

    def outputBinary(self, h):
        """assemble file content, then pass it to hasher via temp file"""
        self.appendContent(h)
        return OutputHandler.HANDLED

    def outputText(self, h):
        """assemble file content, then pass it to hasher via temp file

        Either str or bytearray can be passed to outputText.  Since we
        need to write this to a file and calculate a SHA1, we need bytes.

        For unicode servers, we have a charset specified which is used to
        convert a str to bytes.

        For a nonunicode server, we will have specified "raw" encoding to
        P4Python, so we should never see a str.
        """
        if self.p4.charset:
            try:
                # self.p4.__convert() doesn't work correctly here
                if type(h) == str:
                    b = getattr(self.p4, '__convert')(self.p4.charset, h)
                else:
                    b = getattr(self.p4, '__convert')(self.p4.charset, h.decode())
            except:
                msg = _("error: failed '{}' conversion for '{}#{}'").format(
                    self.p4.charset, self.rev.depot_path, self.rev.revision)
                raise P4Exception(msg)
        else:
            if type(h) == str:
                raise RuntimeError(_('unexpected outputText'))
            b = h
        self.appendContent(b)
        return OutputHandler.HANDLED

    def appendContent(self, h):
        """append a chunk of content to the temp file

        It would be nice to incrementally compress and hash the file
        but that requires knowing the size up front, which p4 print does
        not currently supply.  If/when it does, this can be reworked to
        be more efficient with large files.  As it is, as long as the
        TemporaryFile doesn't rollover, it won't make much of a difference.

        So with that limitation, the incoming content is stuffed into
        a TemporaryFile.
        """
        if not len(h):
            return
        self.tempfile.write(h)

    def flush(self):
        """compress the last file, hash it and stick it in the repo

        Now that we've got the complete file contents, the header can be
        created and used along with the spooled content to create the sha1
        and zlib compressed blob content.  Finally that is written into
        the .git/objects dir.
        """
        if not self.rev:
            return
        size = self.tempfile.tell()
        if size > 0 and self.rev.is_symlink():
            # p4 print adds a trailing newline, which is no good for symlinks.
            self.tempfile.seek(-1, 2)
            b = self.tempfile.read(1)
            if b[0] == 10:
                size = self.tempfile.truncate(size - 1)
        self.tempfile.close()
        # pylint:disable=W0703
        # Catching too general exception Exception
        try:
            tmpname = os.path.basename(self.tempfile.name)
            oid = self.repo.create_blob_fromfile(tmpname)
            self.rev.sha1 = binascii.hexlify(oid).decode()
            self.revs.append(self.rev)
            self._chmod_644_minimum(self.rev.sha1)
        except Exception as e:
            LOG.error('failed to write blob to repository: {}'.format(e))
        finally:
            try:
                os.unlink(self.tempfile.name)
            finally:
                self.tempfile = None
                self.rev = None
        # pylint:enable=W0703

    def outputStat(self, h):
        """save path of current file"""
        self.flush()
        self.rev = P4File.create_from_print(h)
        self.change_set.add(self.rev.change)
        ProgressReporter.increment(_('Copying files'))
        LOG.debug2("PrintHandler.outputStat() ch={} {}#{}".format(
            self.rev.change, self.rev.depot_path, self.rev.revision))
        # use the git working tree so we can use create_blob_fromfile()
        tmpdir = os.getcwd()
        self.tempfile = tempfile.NamedTemporaryFile(
            buffering=10000000, prefix='p2g-print-', dir=tmpdir, delete=False)
        return OutputHandler.HANDLED

    def outputInfo(self, _h):
        """outputInfo call not expected"""
        return OutputHandler.REPORT

    def outputMessage(self, _h):
        """outputMessage call not expected, indicates an error"""
        return OutputHandler.REPORT

    def _chmod_644_minimum(self, sha1):
        '''
        pygit2 created a blob. If that blob is a loose object,
        make sure that loose object's file has at least file mode 644
        so that we (owner) has read+write, and the world has read access.

        Don't raise/abort if this fails due to file not found. Assume that
        is due to the blob landing in a packfile.
        '''
        blob_path = os.path.join( self.ctx.view_dirs.GIT_WORK_TREE
                                , p4gf_git.object_path(sha1))
        try:
            p4gf_util.chmod_644_minimum(blob_path)
        except OSError as e:
            LOG.warn("chmod 644 failed path={path} err={e}"
                     .format(blob_path, e=e))

# pylint: enable=C0103,R0201
