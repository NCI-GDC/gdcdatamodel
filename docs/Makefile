pdf='viz/gdc_data_model.gv.pdf'
html='html/gdc_docs.html'

all: pdf html

pdf:
	python bin/schemata_to_graphviz.py

html:
	python bin/gen_html_docs.py

.PHONY: html pdf

clean:
	rm $(pdf) $(html)
