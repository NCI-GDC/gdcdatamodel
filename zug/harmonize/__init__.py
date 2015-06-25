from gdcdatamodel.models import File


def alignment_report(g, file):
    """
    Generate the alignment report.
    """
    header = "\t".join(["File name", "Source", "Protected", "Status",
                        "Digital ID", "File Size",
                        "Realigned", "Experimental Strategy"])
    file.seek(0)
    file.write(header)
    tcga = g.nodes(File).sysan(source="tcga_cghub")
    target = g.nodes(File).sysan(source="target_cghub")
    cghub = tcga.union(target)
    print "processing all cghub files"
    for f in cghub.all():
        fname = '{}/{}'.format(
            f.system_annotations['analysis_id'], f['file_name'])
        if f.derived_files:
            realn = "ALIGNED"
        else:
            realn = "UNALIGNED"
        if f.experimental_strategies:
            strat = f.experimental_strategies[0].name
        else:
            strat = 'N/A'
        file.write('\t'.join([
            fname,
            f.sysan["source"].upper().replace('_', ' '),
            'FALSE' if f.acl == ['open'] else 'TRUE',
            'DOWNLOADED' if f["state"] == "live" else 'IMPORTED',
            f.node_id,
            str(f.file_size),
            realn,
            strat,
        ]) + '\n')
