import os

from datetime import datetime
from datetime import timedelta

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders

from consulate import Consul
import salt.client
from gdcdatamodel.models import File, FileDataFromFile

from zug.harmonize.queries import exome, wgs, mirnaseq, rnaseq
from cdisutils.log import get_logger

from psqlgraph import PsqlGraphDriver
from sqlalchemy.pool import NullPool

from sqlalchemy import create_engine, desc


def with_derived(q):
    return q.filter(File.derived_files.any())


def alignment_time(file):
    return (file._FileDataFromFile_out[0].sysan["alignment_finished"] -
            file._FileDataFromFile_out[0].sysan["alignment_started"])


ALIGNER_NAMES = {
    "tcga_wgs_aligner": "TCGA WGS",
    "tcga_exome_aligner": "TCGA Exome",
    "tcga_rnaseq_aligner": "TCGA RNA-Seq",
    "tcga_mirnaseq_aligner": "TCGA miRNA-Seq",
    "target_exome_aligner": "TARGET Exome",
    "target_rnaseq_aligner": "TARGET RNA-Seq",
}


class AlignmentReporter(object):

    def __init__(self, graph=None, os_mysql=None, mailserver=None, toaddrs=None):
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
        self.salt_caller = salt.client.Caller()
        if os_mysql:
            self.os_mysql = os_mysql
        else:
            conn_str = 'mysql://{user}:{pw}@{host}/{db}'.format(
                user=os.environ["OS_MYSQL_USER"],
                pw=os.environ["OS_MYSQL_PASS"],
                host=os.environ["OS_MYSQL_HOST"],
                db=os.environ["OS_MYSQL_NAME"],
            )
            self.os_mysql = create_engine(conn_str)
        self.mailserver = mailserver
        self.toaddrs = toaddrs
        self._aligned = None
        self.log = get_logger("alignment_report")

    @property
    def totals(self):
        "totals per zhenyu"
        return {
            "WGS (>= 320 GB)": 364,
            "WGS (< 320 GB)": 4355,
            "WXS (TCGA)": 22561,
            "WXS (TARGET)": 1630,
            "miRNA-Seq": 11914,
            "RNA-Seq (TARGET)": 721,
            "RNA-Seq (TCGA)": 11293,
        }

    @property
    def total_sizes(self):
        "total sizes (in bytes) per zhenyu"
        return {
            "WGS (>= 320 GB)": 161761091978632,
            "WGS (< 320 GB)": 365747226114438,
            "WXS (TCGA)": 315891670201207,
            "WXS (TARGET)": 21382957729925,
            "miRNA-Seq": 236939308278 + 2791773509555,
            "RNA-Seq (TARGET)": 7760649861120,
            "RNA-Seq (TCGA)": 74780058034896,
        }

    @property
    def aligned_files(self):
        if not self._aligned:
            self.log.info("Querying for aligned files")
            wgs_files = with_derived(wgs(self.graph, "tcga_cghub")).all() + with_derived(wgs(self.graph, "target_cghub")).all()
            self._aligned = {
                "WGS (>= 320 GB)": [f for f in wgs_files if f.file_size >= 320000000000],
                "WGS (< 320 GB)": [f for f in wgs_files if f.file_size < 320000000000],
                "WXS (TCGA)": with_derived(exome(self.graph, "tcga_cghub")).all(),
                "WXS (TARGET)": with_derived(exome(self.graph, "target_cghub")).all(),
                "miRNA-Seq": with_derived(mirnaseq(self.graph, "tcga_cghub")).all() + with_derived(mirnaseq(self.graph, "target_cghub")).all(),
                "RNA-Seq (TARGET)": with_derived(rnaseq(self.graph, "target_cghub")).all(),
                "RNA-Seq (TCGA)": with_derived(rnaseq(self.graph, "tcga_cghub")).all(),
            }
        return self._aligned

    def aligned_file_counts(self):
        return {key: len(val) for key, val in self.aligned_files.iteritems()}

    def aligned_file_sizes(self):
        return {key: sum([f.file_size for f in val])
                for key, val in self.aligned_files.iteritems()}

    def generate_files_to_attach(self):
        self.log.info("Generating files to attach")
        return {
            "alignment_numbers.txt": self.generate_numbers_file(),
            "analysis_ids_complete.txt": self.generate_aligned_analysis_ids_file(),
            "analysis_ids_in_progres.txt": self.generate_in_progress_analysis_ids_file(),
            "wgs_timings.txt": self.generate_timings_file(),
            "analysis_ids_fixmate_problem.txt": self.generate_fixmate_problem_analysis_ids_file(),
            "analysis_ids_markdups_failure.txt": self.generate_markdups_failure_analysis_ids_file(),
        }

    def generate_numbers_file(self):
        self.log.info("Generating file with aligned numbers")
        aligned_counts = self.aligned_file_counts()
        aligned_sizes = self.aligned_file_sizes()
        attachment = "Aligned Files (counts)\n"
        attachment += "=============\n\n"
        attachment += "\n".join(["{key}: {aligned} / {total} ({percent:.2f}%)"
                                 .format(key=key,
                                         aligned=aligned_counts[key],
                                         total=total,
                                         percent=100*(float(aligned_counts[key])/total))
                                 for key, total in iter(sorted(self.totals.iteritems()))])
        attachment += "\n\n"
        attachment += "Total sizes aligned\n"
        attachment += "=============\n\n"
        attachment += "\n".join(["{key}: {aligned:.2f} TB / {total:.2f} TB ({percent:.2f}%)"
                                 .format(key=key,
                                         aligned=float(aligned_sizes[key])/1e12,
                                         total=float(total_size)/1e12,
                                         percent=100*(float(aligned_sizes[key])/total_size))
                                 for key, total_size in iter(sorted(self.total_sizes.iteritems()))])
        attachment += "\n\n"
        # breakdown WGS by step completed
        attachment += "WGS Aligned files breakdown by step completed\n"
        attachment += "=============\n\n"
        for key in ["WGS (< 320 GB)", "WGS (>= 320 GB)", "WXS (TCGA)", "WXS (TARGET)"]:
            all_of_key = self.aligned_files[key]
            edges = [self.graph.edges(FileDataFromFile).src(f.node_id)
                     .order_by(desc(FileDataFromFile.created))
                     .first() for f in all_of_key]
            fully_complete = [e for e in edges
                              if e.sysan.get("alignment_last_step") in [None, "md"]]
            fixmate_finished = [e for e in edges
                                if e.sysan.get("alignment_last_step") == "fixmate"]
            merge_finished = [e for e in edges
                              if e.sysan.get("alignment_last_step") == "reheader"]
            attachment += (key + "\n")
            attachment += ("=" * len(key)) + "\n"
            attachment += "Merge finished: {count} ({size:.2f} TB)".format(
                count=len(merge_finished), size=float(sum([e.src.file_size for f in merge_finished]))/1e12
            ) + "\n"
            attachment += "Fixmate finished: {count} ({size:.2f} TB)".format(
                count=len(fixmate_finished), size=float(sum([e.src.file_size for f in fixmate_finished]))/1e12
            ) + "\n"
            attachment += "Fully Complete: {count} ({size:.2f} TB)".format(
                count=len(fully_complete), size=float(sum([e.src.file_size for f in fully_complete]))/1e12
            ) + "\n"
            attachment += "\n"
        # now add running alignment counts
        attachment += "Currently running aligners\n"
        attachment += "==========================\n"
        consul_keys = self.consul.kv.keys()
        mine_results = self.salt_caller.sminion.functions["mine.get"](
            "service:aligner", "grains.items", "grain"
        )
        for prefix, name in ALIGNER_NAMES.items():
            running = len([k for k in consul_keys if prefix in k])
            # this is true for now, let's keep it the case
            alignment_type_grain = prefix.replace("_aligner", "")
            allocated = len({k: v for k, v in mine_results.items()
                             if v.get("alignment_type") == alignment_type_grain})
            attachment += "{name}: {running} currently running / {allocated} allocated\n".format(
                name=name, running=running, allocated=allocated
            )
        return attachment

    def generate_aligned_analysis_ids_file(self):
        self.log.info("Generating file with aligned analysis ids")
        analysis_ids = []
        for _, files in self.aligned_files.iteritems():
            analysis_ids.extend([f.sysan["analysis_id"] for f in files])
        attachment = "\n".join(analysis_ids)
        return attachment

    def generate_in_progress_analysis_ids_file(self):
        self.log.info("Generating file with in progress analysis ids")
        in_progres_gdc_ids = [k.split("/")[-1] for k in self.consul.kv.keys()
                              if "align" in k and "current" in k]
        self.log.info("Querying for analysis ids of files currently being aligned")
        analysis_ids = [res[0] for res in
                        self.graph.nodes(File._sysan["analysis_id"])
                        .filter(File.node_id.in_(in_progres_gdc_ids)).all()]
        attachment = "\n".join(analysis_ids)
        return attachment

    def generate_timings_file(self):
        self.log.info("Generating wgs timings file")
        aligned_wgs_files = []
        for key, files in self.aligned_files.iteritems():
            if "WGS" in key:
                aligned_wgs_files.extend(files)
        self.log.info("Getting uuid -> hostname mapping from gdc mysql")
        uuid_to_host = dict(
            self.os_mysql.execute("SELECT uuid, host from instances;")
            .fetchall()
        )
        attachment = "analysis_id,alignment_time,input_file_size,aligner_uuid,aligner_host\n"
        self.log.info("Generating rows")
        for file in aligned_wgs_files:
            edge = self.graph.edges(FileDataFromFile).src(file.node_id)\
                                                     .order_by(desc(FileDataFromFile.created))\
                                                     .first()
            os_uuid = edge.sysan.get("alignment_host_openstack_uuid")
            row = [
                file.sysan["analysis_id"],
                str(alignment_time(file)),
                str(file.file_size),
                str(os_uuid),
                str(uuid_to_host.get(os_uuid)),
            ]
            attachment += ",".join(row)
            attachment += "\n"
        return attachment

    def generate_fixmate_problem_analysis_ids_file(self):
        self.log.info("Generating file with in FixMateInformation failure analysis ids")
        problem_files = self.graph.nodes(File)\
                                  .sysan(alignment_fixmate_failure=True)\
                                  .all()
        analysis_ids = [f.sysan["analysis_id"] for f in problem_files]
        attachment = "\n".join(analysis_ids)
        return attachment

    def generate_markdups_failure_analysis_ids_file(self):
        self.log.info("Generating file with in MarkDuplicates failure analysis ids")
        problem_files = self.graph.nodes(File)\
                                  .sysan(alignment_markdups_failure=True)\
                                  .all()
        analysis_ids = [f.sysan["analysis_id"] for f in problem_files]
        attachment = "\n".join(analysis_ids)
        return attachment

    def attach_files(self, msg, files):
        for name, contents in files.iteritems():
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(contents)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition',
                            "attachment; filename= {}".format(name))
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

        with self.graph.session_scope():
            files = self.generate_files_to_attach()
        self.attach_files(msg, files)
        self.log.info("Connecting and sending email")
        server = smtplib.SMTP(self.mailserver, 25)
        if len(toaddrs) == 1:
            server.sendmail(fromaddr, toaddrs[0], msg.as_string())
        else:
            server.sendmail(fromaddr, toaddrs, msg.as_string())
