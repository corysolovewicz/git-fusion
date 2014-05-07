#! /usr/bin/env python3.3
'''
Support for admin-supplied custom "commit rejection hook" called during
preflight check time, once per commit x branch.
'''
import logging
import re
import shlex
import sys
import tempfile

import p4gf_config
from   p4gf_l10n    import _, NTR
import p4gf_proc
import p4gf_protect
import p4gf_util

LOG = logging.getLogger(__name__)

                        # pylint:disable=R0913
                        # Too many arguments
                        # Yeah, several functions in here have a really wide
                        # list of arguments. That's still better than
                        # creating a per-commit state NamedTuple
                        # and passing that around.

class PreflightHook:
    '''
    An object that knows how to call a custom "commit rejection hook"
    that can say whether a Git commit is permitted into Perforce.
    '''

    def __init__(self):
        self.action          = None
        self.msg             = None
        self.cmd             = None
        self._spec_file      = None
        self._spec_file_path = None
        self._p4_spec        = None

    def __str__(self):
        if self.action == None:
            return 'None'
        elif self.action == ACTION_NONE:
            return 'none'
        elif self.action in [ACTION_PASS, ACTION_FAIL]:
            return '{}: {}'.format(self.action, self.msg)
        else:
            return '{}: {}'.format(self.action, self.cmd)

    def is_callable(self):
        '''
        Are we worth calling?

        Return True if we've got something to call, False if not.
        Returning False allows calling code to avoid wasting time calculating
        parameters for our __call__ function.
        '''
        return ACTION_NONE != self.action

    def needs_spec_file(self):
        '''
        Is it worth writing a fake spec to a temp file for each __call__()?

        Only if we're calling an external tool, not if we're just PASS/FAIL.
        '''
        return ACTION_RUN == self.action

    @staticmethod
    def from_context(ctx):
        '''
        Search for a preflight-hook configuration in the current config.

        Can be specified in repo config or inherited from global config.
        We neither know nor care.
        '''
        hook = PreflightHook()

        config = p4gf_config.get_repo(ctx.p4gf, ctx.config.view_name)
        value  = config.get( p4gf_config.SECTION_REPO
                          , p4gf_config.KEY_PREFLIGHT_COMMIT
                          , fallback = '')
        value_lower = str(value).strip().lower()

        LOG.debug2('from_context() config {} = {}'
                   .format(p4gf_config.KEY_PREFLIGHT_COMMIT, value))

        if re.search(r'none\b', value_lower) or not value_lower:
            hook.action = ACTION_NONE
            LOG.debug('from_context() none')
            return hook
        elif re.search(r'pass\b', value_lower):
            hook.action = ACTION_PASS
            hook.msg    = value[4:].strip()
            LOG.debug('from_context() pass: {}'.format(hook.msg))
            return hook
        elif re.search(r'fail\b', value_lower):
            hook.action = ACTION_FAIL
            hook.msg    = value[4:].strip()
            LOG.debug('from_context() fail: {}'.format(hook.msg))
            return hook
        else:
            hook.action = ACTION_RUN
            hook.cmd    = shlex.split(value)
            LOG.debug('from_context() cmd : {}'.format(hook.cmd))
            return hook

    def __call__( self
                , ctx
                , fe_commit
                , branch_id
                , jobs ):
        '''
        If preflight hook configured, invoke it (or PASS/FAIL it).

        If fail, raise exception detailing why.

        Route hook's stdout and stderr to our stderr.
        '''
        _debug3('call() {} {} {}'
               , p4gf_util.abbrev(fe_commit['sha1'])
               , p4gf_util.abbrev(branch_id)
               , self )

        if self.action is ACTION_NONE:
            return
        elif self.action is ACTION_PASS:
            if self.msg:
                sys.stderr.write(self.msg + '\n')
            return
        elif self.action is ACTION_FAIL:
            self.raise_rejection(fe_commit['sha1'], self.msg)
        else: # self.action is ACTION_RUN:
            cmd_line_vars = self.calc_cmd_line_vars(
                             ctx                 = ctx
                           , fe_commit           = fe_commit
                           , branch_id           = branch_id
                           , jobs                = jobs
                           , spec_file_path      = self.spec_file_path(ctx)
                           )

            d = (ctx.gwt_to_depot_path(fe_file['path'])
                 for fe_file in fe_commit['files'])
            depot_file_list = (dd for dd in d if dd)

            self._write_spec_file(
                             ctx                = ctx
                           , fe_commit          = fe_commit
                           , depot_file_list    = depot_file_list
                           , jobs               = jobs
                           , spec_file_path     = self.spec_file_path(ctx)
                           , cmd_line_vars      = cmd_line_vars )

            cmd = [self.substitute_cmd_line_vars(cmd_line_vars, word)
                   for word in self.cmd]
            _debug3('cmd {}', cmd)
            d = p4gf_proc.popen_no_throw(cmd)
            _debug3('{}', d)
            msg = p4gf_util.join_non_empty('\n', d['out'], d['err'])
            if d['ec']:
                self.raise_rejection(fe_commit['sha1'], msg)
            sys.stderr.write(msg)

    def spec_file_path(self, ctx):
        '''
        Lazy-create, then reuse over and over, a single temp file to hold
        our fake changelist spec.
        '''
        if self._spec_file_path:
            return self._spec_file_path

                        # Don't bother if we're just PASS/FAIL/None
        if not self.needs_spec_file():
            self._spec_file_path = ''
            return self._spec_file_path

        self._spec_file = tempfile.NamedTemporaryFile(
                                       dir    = ctx.tempdir.name
                                     , prefix = 'preflight-commit-'
                                     , delete = False)
        self._spec_file_path = self._spec_file.name
        return self._spec_file_path

    def p4_spec(self, ctx):
        '''
        Return a 'p4 change -o' spec suitable for use with
        p4.format_change(spec).

        NOT the same as a Python dict, although it behaves as one
        most of the time.
        '''
        if self._p4_spec:
            return self._p4_spec
        self._p4_spec = ctx.p4.fetch_change()
        return self._p4_spec

    @staticmethod
    def raise_rejection(sha1, msg):
        '''
        preflight-commit hook rejected. Tell the Git pusher.
        '''
        raise RuntimeError(_('preflight-commit rejected: {sha1} {msg}\n')
                       .format( sha1 = p4gf_util.abbrev(sha1)
                              , msg  = msg))

    @staticmethod
    def substitute_cmd_line_vars(cmd_line_vars, word):
        '''
        Change '%user' into 'myron'.

        'p4 trigger'-like variable substitution.
        '''
        result = word
        for k, v in cmd_line_vars.items():
            result = result.replace(k, v)
        return result

    @staticmethod
    def calc_cmd_line_vars( ctx
                          , fe_commit
                          , branch_id
                          , jobs
                          , spec_file_path
                          ):
        '''
        Return our mapping of command-line variable substitutions,
        populating if necessary.

        DANGER: Little Bobby Tables! Sanitize your shell inputs!

        These become strings in a command. Watch out for any input
        that a user can control such as repo name or jobs list.
        '''
        branch = ctx.branch_dict()[branch_id]
        git_branch_name = _or_space(branch.git_branch_name)
        client_host     = _or_space(p4gf_protect.get_remote_client_addr())

        r = {
               '%repo%'             : _sanitize(ctx.config.view_name)
             , '%sha1%'             : fe_commit['sha1']
             , '%branch_id%'        : branch_id
             , '%git-branch-name%'  : _sanitize(git_branch_name)
             , '%client%'           : ctx.p4.client
             , '%clienthost%'       : client_host
             , '%serverport%'       : ctx.p4.port
             , '%quote%'            : '"'
             , '%user%'             : fe_commit['owner']
             , '%formfile%'         : spec_file_path
             , '%formname%'         : NTR('new')
             , '%formtype%'         : NTR('change')
             , '%jobs%'             : _sanitize(' '.join(jobs)) if jobs else ''
             }

        return r

    def _write_spec_file( self
                        , ctx
                        , fe_commit
                        , depot_file_list
                        , jobs
                        , spec_file_path
                        , cmd_line_vars ):
        '''
        Write our fake change description spec to a temp file.

        File is a combination 'p4 change -o' and 'p4 describe'.
        Also includes Git Fusion fields such as 'repo' and 'sha1'.
        Note that any Jobs will appear in BOTH the Description and Jobs fields.
        This is correct.

        Change: new

        Date:   2013/09/16 16:17:03

        Client: git-fusion-p4gf_repo

        User:   myron

        Status: pending

        Description:
            Description text

            Jobs:
                job01234

        Jobs:
            job01234

        Files:
            //depot/main/foo#4 edit

        repo:   my_repo

        sha1:   719172d6e978b132aeaac134947191ba7978626d

        '''
        if not self.needs_spec_file():
            return

                        # Let P4Python do most of our formatting, including
                        # all multi-line stuff like description and jobs.
        spec = self.p4_spec(ctx)
        spec.clear()
        spec['Change']      = NTR('new')
        spec['Client']      = cmd_line_vars['%client%']
        spec['User'  ]      = cmd_line_vars['%user%']
        spec['Status']      = NTR('pending')
        spec['Description'] = fe_commit['data']
        if jobs:
            spec['Jobs'] = jobs
        if depot_file_list:
            spec['Files'] = list(depot_file_list)
        spec_text = ctx.p4.format_change(spec)

                        # Append our custom Git Fusion fields.
                        # P4.format_change() will fail with error if
                        # ask it to format these.
        l = []
        _tabpend(l, NTR('repo'),            cmd_line_vars['%repo%'])
        _tabpend(l, NTR('sha1'),            cmd_line_vars['%sha1%'])
        _tabpend(l, NTR('branch-id'),       cmd_line_vars['%branch_id%'])
        _tabpend(l, NTR('git-branch-name'), cmd_line_vars['%git-branch-name%'])

        spec_text = spec_text + '\n' + '\n\n'.join(l) + '\n'

        with open(spec_file_path, "w") as out:
            _debug3('writing {}', spec_file_path)
            out.write(spec_text)


def _tabpend(l, key, val):
    '''
    Append key: <tab> val to a list.
    '''
    l.append(NTR('{key}:\t{val}').format(key=key, val=val))

ACTION_NONE = NTR('none')
ACTION_PASS = NTR('pass')
ACTION_FAIL = NTR('fail')
ACTION_RUN  = NTR('run')

def _or_space(w):
    '''
    Convert None to ''
    '''
    if not w:
        return ''
    return w

def _sanitize(w):
    '''
    Prohibit characters that could escape the shell.

    Even though WE don't invoke the shell, any customer-supplied hooks
    are almost surely running their own shell, and thus vulnerable
    to Little Bobby Tables.
    '''
    ww = w
    ww = ww.replace(';', '')
    return ww


def _debug3(msg, *arg, **kwarg):
    '''
    If logging at DEBUG3, do so. If not, do nothing.
    '''
    if LOG.isEnabledFor(logging.DEBUG3):
        LOG.debug3(msg.format(*arg, **kwarg))
