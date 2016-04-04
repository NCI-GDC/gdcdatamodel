To generate the graphviz documentation you will need the
`gdcdatamodel` installed in your environment and you will need
[Graphviz](www.graphviz.org) installed (`brew install graphviz` should
do it on OSX).

To build the visualization run

```bash
make
```

You should get an output like
```
python bin/schemata_to_graphviz.py
Building schema documentation...
experiment_metadata
pathology_report
sample
tag
...
graphviz output to ~/gdcdatamodel/docs/viz/gdc_data_model.gv
```

And there should be a graphviz output file and a PDF document rendered
from it in `viz/`.
