import json
from typing import Union, List

from pathlib import Path
from sqlalchemy import Table, Column, Integer, String, Float, ForeignKey, MetaData
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic_sqlalchemy import sqlalchemy_to_pydantic
from fastapi import FastAPI
from fastapi_crudrouter import SQLAlchemyCRUDRouter


def get_column_names(expectations: dict) -> List[str]:
    """
    Returns a list of strings containing column names of a GE suite

    Parameters
    ----------
    expectations: dict
        Dictionary with suite expectations as defines in suite json file.

    Returns
    -------
    List[str]
        List of column names
    """

    column_expectations = filter(lambda x: x["expectation_type"].startswith("expect_column"), expectations)
    column_names = map(lambda x: x["kwargs"]["column"], column_expectations)
    return list(set(column_names))


def ge_to_sqla_types(ge_type: str, **kwargs):
    if ge_type == "str":
        return String(kwargs["length"])
    if ge_type == "int":
        return Integer()
    if ge_type == "float":
        return Float()


def ge_suite_to_sqla_columns(suite: str) -> dict:
    expectations = suite["expectations"]

    table_name = suite["expectation_suite_name"].split(".")[0]
    column_names = get_column_names(expectations)
    
    sqla_columns = []
    for column_name in column_names:
        column = Column(name=column_name)

        all_columns_type_expectations = filter(lambda x: x["expectation_type"] == "expect_column_values_to_be_of_type", expectations)
        column_type_expectations = filter(lambda x: x["kwargs"]["column"] == column_name, all_columns_type_expectations)
        ge_type = list(column_type_expectations)[0]["kwargs"]["type_"]
        kwargs = {}
        
        if ge_type == "str":
            all_columns_length_expectations = filter(lambda x: x["expectation_type"] == "expect_column_value_lengths_to_be_between", expectations)
            column_length_expectations = list(filter(lambda x: x["kwargs"]["column"] == column_name, all_columns_length_expectations))
            
            if len(column_length_expectations) == 1:
                length = column_length_expectations[0]["kwargs"]["max_value"]
            else:
                length = 100
            
            kwargs = {"length": length}

        column.type = ge_to_sqla_types(ge_type, **kwargs)
        
        if column_name == "id" or column_name.endswith("_id"):
            column.primary_key = True

        if column_name.endswith("_id") and column_name.split("_")[0] != table_name:
            foreign_table = column_name.split("_")[0]
            column.foreign_keys = [ForeignKey(f"{foreign_table}.id")]

        sqla_columns.append(column)
        
    return sqla_columns


def ge_suite_to_sqla_table(suite: str, metadata: MetaData) -> Table:
    columns = ge_suite_to_sqla_columns(suite)
    table_name = suite["expectation_suite_name"].split(".")[0]

    return Table(
        table_name,
        metadata,
        *columns
    )


def sqla_core_to_orm(name, core_model):
    Base = declarative_base()

    return type(
        name,
        (Base, ),
        {"__table__": core_model}
    )


class LazyStack:
    """Class to create SQLAlchemy models and CRUD routes from Great Expectations suites

    Parameters
    ----------
    ge_dir : str or path object
        Path of Great Expectations suites directory. By default, all suites in expectations directory
        with the name `main.json` are read. For example, if the `great_expectations/expectations` has 
        the sub-directories and suites: `employee/main.json` and `company/main.json`, then 2 tables will
        be created: `employee` and `company`.
    """

    def __init__(self, ge_dir: Union[str, Path], metadata=None):
        self.ge_dir: Union[str, Path] = ge_dir
        if metadata is None:
            self.metadata = MetaData()
        else:
            self.metadata = metadata

        if type(self.ge_dir) == str:
            self.ge_dir = Path(self.ge_dir).resolve()

        self.suite_names: List[str] = [suite.stem for suite in self.ge_dir.iterdir() if not str(suite.stem).startswith(".")]

        self.engine = None

        self.suites = None
        self.sqla_models = None
        self.pydantic_models = None

    def create_sqla_models(self):
        self.sqla_models: dict = {}
        for suite_name in self.suite_names:
            suite: dict = json.load(open(self.ge_dir / suite_name / "main.json", "r"))
            table = ge_suite_to_sqla_table(suite=suite, metadata=self.metadata)
            self.sqla_models[suite_name] = table

        return list(self.metadata.tables.keys())

    def create_pydantic_models(self):
        self.pydantic_models = {}

        for suite_name, sqla_model in self.sqla_models.items():
            orm_table_class = sqla_core_to_orm(suite_name, sqla_model)
            self.pydantic_models[suite_name] = sqlalchemy_to_pydantic(orm_table_class)

        return list(self.pydantic_models.keys())

    def create_crud_routes(self, engine):
        app = FastAPI()
        
        SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine
        )
        
        def get_db():
            session = SessionLocal()
            try:
                yield session
                session.commit()
            finally:
                session.close()


        for name in self.pydantic_models.keys():
            pydantic_model = self.pydantic_models[name]
            sqla_orm_model = sqla_core_to_orm(name, self.sqla_models[name])

            router = SQLAlchemyCRUDRouter(
                schema=pydantic_model,
                db_model=sqla_orm_model,
                db=get_db,
                prefix=name
            )

            app.include_router(router)

        return app
