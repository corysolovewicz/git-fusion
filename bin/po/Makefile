# L10N/Localization translation files for Git Fusion
#
# Starting from scratch?
#
# 1. make po
#		Extracts _() strings to a git-fusion.pot file, copies to
#		po/en_US.UTF-8.po.
#
# 2. (optional) edit po/en_US.UTF-8.po
#			 Add any translations you want to test.
#			 Edit the "msgstr" lines (which start out all empty),
#			 not "msgid" strings.
#		     Careful: this .po file is clobbered by 'make po'.
#
# 3. make mo
#		 Compiles po/en_US.UTF-8.po to mo/en_US.UTF-8/LC_MESSAGES/git-fusion.mo
#
# 4. run Git Fusion to see the results.
#
#
# .pot file: extract all the US English string literals from our Python
#            source and collect into a single .pot file that we can send to a
#            translator. They send us back .po files (no t) with the translated
#            content.
#
# .po files: Human-edited files of translated messages.
#            These are source, created by humans. Only the en_US.UTF-8.po file
#            is computer-generated, just so that we'll have SOMETHING to compile
#            into .mo files and test later.
#
# .mo files: Compiled version of a corresponding .po file. This is what
#			 gettext() loads and uses at runtime to supply translated versions
#			 of our US English string literals.
#
.PHONY: mo po pot

BIN_DIR=..
MO_DIR=$(BIN_DIR)/mo
PO_DIR=$(BIN_DIR)/po

SOURCE_PY=$(wildcard $(BIN_DIR)/p4gf_*.py)
SOURCE_PO=$(wildcard $(PO_DIR)/*.po)
COMPILED_MO=$(patsubst $(PO_DIR)/%.po,$(MO_DIR)/%/LC_MESSAGES/git-fusion.mo,$(SOURCE_PO))

pot: $(PO_DIR)/git-fusion.pot

$(PO_DIR)/git-fusion.pot : $(SOURCE_PY)
	-@mkdir -p $(@D)
	pygettext.py --extract-all --output=$@ $^

# Just so that we have at least ONE .po file that we can test, here, have a US
# English "translation" file. Feel free to edit the resulting en_US.UTF-8.po
# with your test "translations".
#
# Edit the "msgstr" lines (all start as empty) not the "msgid" lines.
#
po : $(PO_DIR)/en_US.UTF-8.po

$(PO_DIR)/en_US.UTF-8.po : $(PO_DIR)/git-fusion.pot
	cp $(PO_DIR)/git-fusion.pot $@

#
# Makefile target pattern won't work until there's at least ONE .po source file.
#
mo: $(COMPILED_MO)

$(MO_DIR)/%/LC_MESSAGES/git-fusion.mo : $(PO_DIR)/%.po
	-@mkdir -p $(@D)
	msgfmt.py --output-file=$@ $^
