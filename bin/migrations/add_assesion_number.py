import mappings
import argparse
from psqlgraph import PsqlGraphDriver
from gdcdatamodel.models import *

mapping = mappings.Mapping()


def migrate(host, user, password, database):
    graph = PsqlGraphDriver(host, user, password, database)
    with graph.session_scope():
        for project, phsids in mapping.PROJECT_TO_PHSID.items():
            tokens = project.split("-")
            program_code = tokens[0]
            project_code = "-".join(tokens[1:])
            program_phsid = mapping.get_program_level_phsid(project)
            project_phsid = mapping.get_project_level_phsid(project)
            program = graph.nodes(Program).props(name=program_code).first()
            project = graph.nodes(Project).props(code=project_code).first()
            if program:
                program.props["dbgap_accession_number"] = program_phsid
            if project:
                project.props["dbgap_accession_number"] = project_phsid


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", type=str, action="store", default="localhost", help="psql-server host"
    )
    parser.add_argument(
        "--user", type=str, action="store", default="test", help="psql test user"
    )
    parser.add_argument(
        "--password",
        type=str,
        action="store",
        default="test",
        help="psql test password",
    )
    parser.add_argument(
        "--database",
        type=str,
        action="store",
        default="automated_test",
        help="psql test database",
    )

    args = parser.parse_args()
    migrate(args.host, args.user, args.password, args.database)
