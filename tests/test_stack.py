from pathlib import Path
from sqlalchemy import Table, create_engine
from pydantic import BaseModel
from pydantic.main import ModelMetaclass
from fastapi import FastAPI

from lazystack import LazyStack


def test_create_sqla_models():
    ge_dir = Path("./great_expectations/expectations").resolve()
    
    stack = LazyStack(ge_dir=ge_dir)
    sqla_model_names = stack.create_sqla_models()

    assert type(sqla_model_names) == list
    assert len(sqla_model_names) > 0
    
    # Check type
    assert all([type(table) == Table for name, table in stack.sqla_models.items()])

    # created tables are the same as GE suites
    assert set(sqla_model_names) == set([suite.stem for suite in ge_dir.iterdir() if not str(suite.stem).startswith(".")])

    # SQLAlchemy tables are the same as created tables
    assert set(sqla_model_names) == set(stack.sqla_models.keys())


def test_create_pydantic_models():
    ge_dir = Path("./great_expectations/expectations").resolve()
    engine = create_engine("sqlite:///./db/sqlite.db")
    
    # Create SQLAlchemy models
    stack = LazyStack(ge_dir=ge_dir)
    sqla_models = stack.create_sqla_models()

    # Create SQL tables
    stack.metadata.drop_all(engine, checkfirst=True)
    stack.metadata.create_all(engine, checkfirst=False)

    # Create pydantic models
    pydantic_model_names = stack.create_pydantic_models()

    assert type(pydantic_model_names) == list
    assert len(pydantic_model_names) > 0

    # Check that there is a Pydantic model for each SQLAlchemy model
    assert set(pydantic_model_names) == set(stack.sqla_models.keys())

    # Check type
    assert all([type(model) == ModelMetaclass for name, model in stack.pydantic_models.items()])


def test_create_crud_routers():
    ge_dir = Path("./great_expectations/expectations").resolve()
    engine = create_engine("sqlite:///./db/sqlite.db")
    
    # Create SQLAlchemy models
    stack = LazyStack(ge_dir=ge_dir)
    sqla_models = stack.create_sqla_models()

    # Create SQL tables
    stack.metadata.drop_all(engine, checkfirst=True)
    stack.metadata.create_all(engine, checkfirst=False)

    # Create pydantic models
    pydantic_model_names = stack.create_pydantic_models()

    # create CRUD routes
    crud_app = stack.create_crud_routers(engine=engine)

    assert type(crud_app) == FastAPI

    paths = set([r.path for r in crud_app.routes]) - {'/openapi.json', '/docs', '/docs/oauth2-redirect', '/redoc'}
    assert len(paths) > 0

    base_routes = set([p[1:].split("/")[0] for p in paths])
    assert len(base_routes) > 0

    # Check that a route is created for every SQL model
    assert base_routes == set(stack.sqla_models.keys()) == set(stack.sqla_models.keys())
