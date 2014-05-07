#! /usr/bin/env python3.3
"""
Git Fusion repo name mangling: permit Git repo names that cannot be legal {repo} names
as used in depot paths and client spec names.

Not natural language translation: see p4gf_l10n for that.
Not character encoding translation: see p4gf_char for that.
"""

import sys
import p4gf_util

from   p4gf_l10n      import _, NTR

class TranslateReponame:
    """Class to convert among three different formats of the GF reponame.

        The git name may contain:
            /     -for git repo syntax
            :     -for p4 client syntax
        The repo name /git-fusion/repos/reponame may not contain '/' ':' ' '
        p4 clients names may not contain / but may contain ':' or '_' substituted for ' '
    """
    def __init__(self):
        pass

    @classmethod
    def git_to_repo(cls, viewname, forward=True, translate_p4=True):
        '''
        Translate  '/'   and optionally    ( ':'  and  ' ' )  ... as shown in translatables[]



        This supports git urls containing '/' and P4 clients containing ':' and ' '
        Example:   git clone:gitfusion@localhost:/my/teste:er/repo
                   git           /my/teste:er/repo
                   repo          my_0xS_test_0xC_er_0xS_repo
        Note: Leading slash in git name is stripped. So this clones the same repo
        Example:   git clone:gitfusion@localhost:my/teste:er/repo
                   git           my/teste:er/repo
                   repo          my_0xS_test_0xC_er_0xS_repo

        '''
        # Slash to support git urls
        translatables = [ ['/', '_0xS_'] ]

        # these are the P4 client legal chars
        # which are not legal for the internal file path of the GF repos/...
        #  '_' mapping must be last of all target items containing '_' ]
        #p4_translatables = [ [':' ,  '_0xC_'] , [' '  , '_']]
        p4_translatables = [ [':' ,  '_0xC_'] ]
        if translate_p4:
            translatables.extend(p4_translatables)

        translated = ''.join(viewname)  # copy
        for pair in translatables:
            if forward:
                translated = translated.replace(pair[0], pair[1])
            else:
                # Do not re-translate '_' back to SPACE
                if pair[0] == ' ':
                    continue
                translated = translated.replace(pair[1], pair[0])

        return translated
    @classmethod
    def repo_to_git(cls, viewname):
        '''
        Un-Translate /  and : as shown in table
        Example:   git clone:gitfusion@localhost:/my/teste:er/repo
                   git              my/teste:er/repo
                   repo             my_0xS_test_0xC_er_0xS_repo
        '''
        return TranslateReponame.git_to_repo(viewname, forward=False)

    @classmethod
    def git_to_p4client(cls, viewname):
        '''
        Translate  '/'
        This supports git urls containing '/'
        Example:   git clone:gitfusion@localhost:/my/teste:er/repo
                   repo             my/teste:er/repo
                   p4client         my_0xS_test:er_0xS_repo
        '''
        return TranslateReponame.git_to_repo(viewname, translate_p4=False)

    @classmethod
    def p4client_to_git(cls, viewname):
        '''
        Un-Translate /
        Example:   git clone:gitfusion@localhost:/my/teste:er/repo
                   p4client         my_0xS_test:er_0xS_repo
                   repo             my/teste:er/repo
        '''
        return TranslateReponame.git_to_repo(
                viewname,  forward=False, translate_p4=False)

    @classmethod
    def p4client_to_repo(cls, viewname):
        '''
        Un-Translate /
        Example:   git clone:gitfusion@localhost:/my/teste:er/repo
                   p4client         my_0xS_test:er_0xS_repo
                   repo             my_0xS_test_0xC_er_0xS_repo
        '''
        return TranslateReponame.git_to_repo(
                    TranslateReponame.p4client_to_git(viewname))

    @classmethod
    def repo_to_p4client(cls, viewname):
        '''
        Un-Translate /
        Example:   git clone:gitfusion@localhost:/my/teste:er/repo
                   p4client         my_0xS_test:er_0xS_repo
                   repo             my_0xS_test_0xC_er_0xS_repo
        '''
        return TranslateReponame.git_to_p4client(
                    TranslateReponame.repo_to_git(viewname))

def print_names(git, gf, p4):
    """printer"""

    print(NTR('git    ') + git)
    print(NTR('repo   ') + gf)
    print(NTR('p4     ') + p4)

TYPES = NTR(['git', 'repo', 'p4'])

def parse_args(argv):
    """parser"""
    parser = p4gf_util.create_arg_parser(_('Translate Git Fusion repo name formats.')
        , usage       = _('p4gf_translate.py   --type git|repo|p4   <name>'))
    parser.add_argument('--type')
    parser.add_argument(NTR('name'),      metavar=NTR('name'))
    args = parser.parse_args(argv)
    _type = args.type
    if not _type in TYPES:
        print(parser.usage)
        print(_("Unknown input type '{bad}', must be one of [{good}.]")
              .format(bad=_type, good = ", ".join(TYPES)))
        sys.exit(1)
    return args

def main(argv):
    """Main"""
    args = parse_args(argv)
    _type = args.type
    name = args.name

    if _type == 'git':
        print_names(
                   name
                  ,TranslateReponame.git_to_repo(name)
                  ,TranslateReponame.git_to_p4client(name))


    elif _type == 'repo':
        print_names(
                   TranslateReponame.repo_to_git(name)
                  ,name
                  ,TranslateReponame.repo_to_p4client(name))


    else:
        print_names(
                   TranslateReponame.p4client_to_git(name)
                  ,TranslateReponame.p4client_to_repo(name)
                  ,name)



if __name__ == "__main__":
    main(sys.argv[1:])
