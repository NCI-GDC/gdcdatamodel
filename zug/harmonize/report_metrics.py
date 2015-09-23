from psqlgraph import PsqlGraphDriver
import os
from queries import wgs, exome, mirnaseq, rnaseq
from datadog import statsd
statsd.host = 'datadogproxy.service.consul'


class Reporter(object):
    def __init__(self):
        self.graph = PsqlGraphDriver(
            os.environ["PG_HOST"],
            os.environ["PG_USER"],
            os.environ["PG_PASS"],
            os.environ["PG_NAME"],
        )

    def report_totals(self):
        with self.graph.session_scope():
            exome_totals = exome(self.graph, 'tcga_cghub').count()\
                + exome(self.graph, 'target_cghub').count()
            print 'exome_totals', exome_totals
            statsd.gauge('harmonization.total',
                         exome_totals,
                         tags=['alignment_type:tcga_exome_aligner'])

            wgs_totals = wgs(self.graph, 'tcga_cghub').count()\
                + wgs(self.graph, 'target_cghub').count()
            print 'wgs_totals', wgs_totals
            statsd.gauge('harmonization.total',
                         wgs_totals,
                         tags=['alignment_type:tcga_wgs_aligner'])

            mirna_totals = mirnaseq(self.graph, 'tcga_cghub').count()\
                + mirnaseq(self.graph, 'target_cghub').count()
            print 'mirna', mirna_totals 
            statsd.gauge('harmonization.total',
                         mirna_totals,
                         tags=['alignment_type:tcga_mirnaseq_aligner'])

            rna_totals = rnaseq(self.graph, 'tcga_cghub').count()\
                + rnaseq(self.graph, 'target_cghub').count()
            print 'rna', rna_totals 
            statsd.gauge('harmonization.total',
                         rna_totals,
                         tags=['alignment_type:tcga_rnaseq_aligner'])

if __name__ == "__main__":
    report = Reporter()
    report.report_totals()
