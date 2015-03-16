MAIN := report
LTMK := bin/latexmk

.PHONY: all clean

define latexmk =
	$(LTMK) -silent -pdf "$1" &>/dev/null \
	  || (bin/parse-latex-log.py "$1.log"; exit 1)
endef

all: $(DEPS)
	$(LTMK) -silent -pdf $(MAIN) &>/dev/null \
	  || (bin/parse-latex-log.py "$(MAIN).log"; exit 1)	

clean:
	$(LTMK) -C

SPELLTEX := $(shell ./bin/get-tex-files.sh $(MAIN).tex)
spell:
	@for i in $(SPELLTEX); do aspell -x --mode=tex -p ./aspell.words -c $$i; done
	@( head -1 aspell.words ; tail -n +2 aspell.words | sort ) > aspell.words~
	@mv aspell.words~ aspell.words
