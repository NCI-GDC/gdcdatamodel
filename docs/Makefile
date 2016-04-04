pdf='viz/gdc_data_model.gv.pdf'
html='html/gdc_docs.html'

all: pdf

pdf:
	python bin/schemata_to_graphviz.py

.PHONY: pdf

clean:
	rm $(pdf) $(html)
