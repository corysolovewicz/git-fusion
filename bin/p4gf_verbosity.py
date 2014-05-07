#! /usr/bin/env python3.3
'''
Common code for argv verbosity parsing and reporting.
'''

from   p4gf_l10n      import _, NTR


class Verbosity:
    '''
    Deal with argv --verbose/--quiet argv.

    Verbosity is really just a namespace, not a class you'd instantiate.
    '''

    # Class constants

    # report() will print all messages at or below this level.
    # 0 is just errors
    # 1 is short/occasional status
    # 2 is NOISY
    QUIET =  0 # Still want errors
    ERROR =  0
    WARN  =  1
    INFO  =  2
    DEBUG =  3

    VERBOSE_MAP =          { "QUIET"    : QUIET
                           , "ERROR"    : ERROR
                           , "ERR"      : ERROR
                           , "WARNING"  : WARN
                           , "WARN"     : WARN
                           , "INFO"     : INFO
                           , "DEBUG"    : DEBUG }
    VERBOSE_SEQUENCE = NTR([ "QUIET"
                           , "ERROR"
                           , "ERR"
                           , "WARNING"
                           , "WARN"
                           , "INFO"
                           , "DEBUG" ])

    # Default verbose level.
    VERBOSE_LEVEL = 2 # INFO

    @staticmethod
    def add_parse_opts(parser):
        '''
        Add --verbose/-v and --quiet/-q options.
        '''
        # pylint:disable=C0301
        # line too long? Too bad. Keep tabular code tabular.
        parser.add_argument('--verbose', '-v', metavar=NTR('level'),  nargs='?'
                           , default='INFO', help=_('Reporting verbosity.'))
        parser.add_argument('--quiet',   '-q', action='store_true'
                           , help=_('Report only errors. Same as --verbose QUIET'))
        # pylint:enable=C0301

    @staticmethod
    def parse_level(args):
        '''
        Between --verbose and --quiet argv options, pick a level
        and store it in global variable VERBOSE_LEVEL.
        '''
        if args.quiet:
            args.verbose = Verbosity.QUIET
        elif not args.verbose:
            # -v with no arg means "debug"
            args.verbose = Verbosity.DEBUG
        # Convert text levels like "INFO" to numeric 2
        if str(args.verbose).upper() in Verbosity.VERBOSE_MAP.keys():
            args.verbose = Verbosity.VERBOSE_MAP[str(args.verbose).upper()]
        elif not args.verbose in Verbosity.VERBOSE_MAP.values():
            Verbosity.report(Verbosity.ERROR
                        , _("Unknown --verbose value '{val}'. Try '{good}'")
                          .format( val=args.verbose
                                 , good="', '".join(Verbosity.VERBOSE_SEQUENCE)))
            exit(2)

        Verbosity.VERBOSE_LEVEL = args.verbose
        Verbosity.report(Verbosity.DEBUG, _("args={}").format(args))

    @staticmethod
    def report(lvl, msg):
        """Tell the human what's going on."""
        if lvl <= Verbosity.VERBOSE_LEVEL:
            print(msg)


    def __init__(self):
        '''Don't instantiate Verbosity.'''
        raise RuntimeError("No.")

