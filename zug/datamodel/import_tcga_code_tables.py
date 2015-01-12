import csv


def import_code_table(graph, path, label, **kwargs):
    with open(path, 'r') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            graph.node_merge(
                node_id=row[0],
                label=label,
                properties={
                    key: row[index]
                    for key, index in kwargs.items()
                }
            )


def import_center_codes(graph, path):
    import_code_table(
        graph, path, 'center',
        legacy_id=1,
        namespace=2,
        center_type=3,
        name=4,
        short_name=4,
    )


def import_tissue_source_site_codes(graph, path):
    import_code_table(
        graph, path, 'tissue_source_site',
        legacy_id=1,
        name=2,
        project=3,
        bcr_id=4,
    )
