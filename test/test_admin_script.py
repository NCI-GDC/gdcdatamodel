import psqlgraph
import pytest
from psqlgraph import ext
from sqlalchemy.exc import ProgrammingError

from gdcdatamodel import gdc_postgres_admin as pgadmin
from gdcdatamodel import models


def get_base_args(host="localhost", database="automated_test", namespace=None):
    return ["-H", host, "-U", "postgres", "-D", database, "-N", namespace or ""]


def get_admin_driver(db_config, namespace=None):

    # assumes no password postgres user
    g = psqlgraph.PsqlGraphDriver(
        package_namespace=namespace,
        host=db_config["host"],
        user="postgres",
        password=None,
        database=db_config["database"],
    )

    return g


def drop_all_tables(g):
    orm_base = (
        ext.get_orm_base(g.package_namespace)
        if g.package_namespace
        else psqlgraph.base.ORMBase
    )
    psqlgraph.base.drop_all(g.engine, orm_base)


def run_admin_command(args, namespace=None):
    args += get_base_args(namespace=namespace)
    parsed_args = pgadmin.get_parser().parse_args(args)
    pgadmin.main(parsed_args)


def invalid_write_access_fn(g):
    with pytest.raises(ProgrammingError):
        with g.session_scope() as s:
            s.add(models.Case("1"))


def valid_write_access_fn(g):
    with g.session_scope() as s:
        s.merge(models.Case("1"))
    yield
    with g.session_scope() as s:
        n = g.nodes().get("1")
        s.delete(n)


def invalid_read_access_fn(g):
    # verify user does not have read access
    with pytest.raises(ProgrammingError):
        with g.session_scope():
            g.nodes().count()


def valid_read_access_fn(g):
    with g.session_scope():
        assert g.nodes().count() == 0


@pytest.fixture()
def add_test_database_user(db_config):

    dummy_user = "pytest_dummy"
    dummy_pwd = "pytest_du33y"

    g = get_admin_driver(db_config)

    try:
        g.engine.execute(
            f"CREATE USER {dummy_user} WITH PASSWORD '{dummy_pwd}'"
        )
        g.engine.execute(f"GRANT USAGE ON SCHEMA public TO {dummy_user}")
        yield dummy_user, dummy_pwd
    finally:
        g.engine.execute("DROP OWNED BY {0}; DROP USER {0}".format(dummy_user))


@pytest.mark.parametrize("namespace", [None, "gdc"], ids=["default", "custom"])
def test_create_tables(db_config, namespace):
    """Tests tables can be created with the admin script using either a custom dictionary or the default
    Args:
        db_config (dict[str,str]): db connection config
        namespace (str): module namespace, None for default
    """

    # simulate loading a different dictionary
    if namespace:
        models.load_dictionary(dictionary=None, package_namespace=namespace)

    # drop existing tables
    admin = get_admin_driver(db_config, namespace)
    drop_all_tables(admin)

    # get the proper module for loading classes
    m = models
    if namespace:
        m = getattr(models, namespace)

    # test tables currently do not exist. Testing one table should be ok
    with pytest.raises(ProgrammingError) as e:
        with admin.session_scope():
            assert admin.nodes(m.Case).count() == 0

    # Hard coded message expected to be in the exception
    assert 'relation "node_case" does not exist' in str(e.value)

    # create tables using admin script
    args = ["graph-create", "--delay", "1", "--retries", "0"] + get_base_args(
        namespace=namespace
    )
    parsed_args = pgadmin.get_parser().parse_args(args)
    pgadmin.main(parsed_args)

    # validate case and program tables was created
    with admin.session_scope():
        assert admin.nodes(m.Case).count() == 0
        assert admin.nodes(m.Program).count() == 0


@pytest.mark.parametrize("namespace", [None, "gdc"], ids=["default", "custom"])
@pytest.mark.parametrize(
    "permission, invalid_permission_fn, valid_permission_fn",
    [
        ("read", invalid_read_access_fn, valid_read_access_fn),
        ("write", invalid_write_access_fn, valid_write_access_fn),
    ],
    ids=["read", "write"],
)
def test_grant_permissions(
    db_config,
    namespace,
    add_test_database_user,
    permission,
    invalid_permission_fn,
    valid_permission_fn,
):

    # simulate loading a different dictionary
    if namespace:
        models.load_dictionary(dictionary=None, package_namespace=namespace)

    dummy_user, dummy_pwd = add_test_database_user

    g = psqlgraph.PsqlGraphDriver(
        host=db_config["host"],
        user=dummy_user,
        password=dummy_pwd,
        database=db_config["database"],
        package_namespace=namespace,
    )

    # verify user does not have permission
    invalid_permission_fn(g)
    run_admin_command(["graph-grant", f"--{permission}={dummy_user}"])

    # verify user now has permission
    valid_permission_fn(g)


@pytest.mark.parametrize("namespace", [None, "gdc"], ids=["default", "custom"])
@pytest.mark.parametrize(
    "permission, invalid_permission_fn, valid_permission_fn",
    [
        ("read", invalid_read_access_fn, valid_read_access_fn),
        ("write", invalid_write_access_fn, valid_write_access_fn),
    ],
    ids=["read", "write"],
)
def test_revoke_permissions(
    db_config,
    namespace,
    add_test_database_user,
    permission,
    invalid_permission_fn,
    valid_permission_fn,
):

    # simulate loading a different dictionary
    if namespace:
        models.load_dictionary(dictionary=None, package_namespace=namespace)

    dummy_user, dummy_pwd = add_test_database_user

    g = psqlgraph.PsqlGraphDriver(
        host=db_config["host"],
        user=dummy_user,
        password=dummy_pwd,
        database=db_config["database"],
        package_namespace=namespace,
    )

    # grant user permissions
    run_admin_command(["graph-grant", f"--{permission}={dummy_user}"])

    # verify user has permission
    valid_permission_fn(g)

    # revoke permission
    run_admin_command(["graph-revoke", f"--{permission}={dummy_user}"])
    # verify user no longer has permission
    invalid_permission_fn(g)
