import os

from datetime import datetime
from datetime import timedelta

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders

from consulate import Consul
from gdcdatamodel.models import File

from zug.harmonize.queries import exome, wgs, mirnaseq, rnaseq
from cdisutils.log import get_logger

from psqlgraph import PsqlGraphDriver
from sqlalchemy.pool import NullPool


def with_derived(q):
    return q.filter(File.derived_files.any())


def alignment_time(file):
    return (file._FileDataFromFile_out[0].sysan["alignment_finished"] -
            file._FileDataFromFile_out[0].sysan["alignment_started"])


class AlignmentReporter(object):

    def __init__(self, graph=None, mailserver=None, toaddrs=None):
        if graph:
            self.graph = graph
        else:
            self.graph = PsqlGraphDriver(
                os.environ["PG_HOST"],
                os.environ["PG_USER"],
                os.environ["PG_PASS"],
                os.environ["PG_NAME"],
                poolclass=NullPool,
            )
        self.consul = Consul()
        self.mailserver = mailserver
        self.toaddrs = toaddrs
        self._aligned = None
        self.log = get_logger("alignment_report")

    @property
    def totals(self):
        "totals per zhenyu"
        return {
            "WGS": 4799,
            "WXS": 24189,
            "miRNA-Seq": 11914,
            "RNA-Seq (TARGET)": 721,
            "RNA-Seq (TCGA)": 11293,
        }

    @property
    def aligned_files(self):
        if not self._aligned:
            self.log.info("Querying for aligned files")
            self._aligned = {
                "WGS": with_derived(wgs(self.graph, "tcga_cghub")).all() + with_derived(wgs(self.graph, "target_cghub")).all(),
                "WXS": with_derived(exome(self.graph, "tcga_cghub")).all() + with_derived(exome(self.graph, "target_cghub")).all(),
                "miRNA-Seq": with_derived(mirnaseq(self.graph, "tcga_cghub")).all() + with_derived(mirnaseq(self.graph, "target_cghub")).all(),
                "RNA-Seq (TARGET)": with_derived(rnaseq(self.graph, "target_cghub")).all(),
                "RNA-Seq (TCGA)": with_derived(rnaseq(self.graph, "tcga_cghub")).all(),
            }
        return self._aligned

    def aligned_file_counts(self):
        return {key: len(val) for key, val in self.aligned_files.iteritems()}

    def attach_numbers_file(self, msg):
        self.log.info("Generating file with aligned numbers")
        filename = "alignment_numbers.txt"
        aligned_counts = self.aligned_file_counts()
        attachment = "Aligned Files\n"
        attachment += "=============\n\n"
        attachment += "\n".join(["{key}: {aligned} / {total} ({percent:.2f}%)"
                                 .format(key=key,
                                         aligned=aligned_counts[key],
                                         total=total,
                                         percent=100*(float(aligned_counts[key])/total))
                                 for key, total in self.totals.iteritems()])
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        "attachment; filename= {}".format(filename))
        msg.attach(part)

    def attach_aligned_analysis_ids_file(self, msg):
        self.log.info("Generating file with aligned analysis ids")
        analysis_ids = []
        for _, files in self.aligned_files.iteritems():
            analysis_ids.extend([f.sysan["analysis_id"] for f in files])
            filename = "analysis_ids_complete.txt"
        attachment = "\n".join(analysis_ids)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        "attachment; filename= {}".format(filename))
        msg.attach(part)

    def attach_in_progress_analysis_ids_file(self, msg):
        self.log.info("Generating file with in progress analysis ids")
        in_progres_gdc_ids = [k.split("/")[-1] for k in self.consul.kv.keys()
                              if "align" in k and "current" in k]
        self.log.info("Querying for analysis ids of files currently being aligned")
        analysis_ids = [res[0] for res in
                        self.graph.nodes(File._sysan["analysis_id"])
                        .filter(File.node_id.in_(in_progres_gdc_ids)).all()]
        filename = "analysis_ids_in_progress.txt"
        attachment = "\n".join(analysis_ids)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        "attachment; filename= {}".format(filename))
        msg.attach(part)

    def attach_timings_file(self, msg):
        self.log.info("Generating wgs timings file")
        filename = "wgs_timings.txt"
        aligned_wgs_files = self.aligned_files["WGS"]
        attachment = ""
        for file in aligned_wgs_files:
            attachment += ",".join([file.sysan["analysis_id"], str(alignment_time(file))])
            attachment += "\n"
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        "attachment; filename= {}".format(filename))
        msg.attach(part)

    def attach_fixmate_problem_analysis_ids_file(self, msg):
        self.log.info("Generating file with in FixMateInformation failure analysis ids")
        problem_files = self.graph.nodes(File)\
                                  .sysan(alignment_fixmate_failure=True)\
                                  .all()
        analysis_ids = [f.sysan["analysis_id"] for f in problem_files]
        filename = "analysis_ids_fixmate_problem.txt"
        attachment = "\n".join(analysis_ids)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        "attachment; filename= {}".format(filename))
        msg.attach(part)

    def send_email(self):
        self.log.info("Building email")
        fromaddr = "alignmentreport@opensciencedatacloud.org"
        toaddrs = self.toaddrs
        msg = MIMEMultipart()

        msg["From"] = fromaddr
        if len(toaddrs) == 1:
            msg["To"] = toaddrs[0]
        else:
            msg["To"] = ", ".join(toaddrs)
        msg["Subject"] = "Alignment tracking (as of {})".format(
            (datetime.now() - timedelta(hours=5)).strftime("%Y-%m-%d %I:%M:%S %p")
        )

        body = "Attached is the latest alignment information."

        msg.attach(MIMEText(body, 'plain'))

        self.log.info("Attaching files")
        # first the numbers file
        with self.graph.session_scope():
            self.attach_numbers_file(msg)
            self.attach_aligned_analysis_ids_file(msg)
            self.attach_in_progress_analysis_ids_file(msg)
            self.attach_timings_file(msg)
            self.attach_fixmate_problem_analysis_ids_file(msg)

        self.log.info("Connecting and sending email")
        server = smtplib.SMTP(self.mailserver, 25)
        if len(toaddrs) == 1:
            server.sendmail(fromaddr, toaddrs[0], msg.as_string())
        else:
            server.sendmail(fromaddr, toaddrs, msg.as_string())
