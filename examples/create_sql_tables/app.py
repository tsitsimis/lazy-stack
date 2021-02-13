from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from lazystack import LazyStack


# Initialize with Great Expectations
stack = LazyStack(ge_dir="./great_expectations/expectations")

# Create SQLAlchemy models
stack.create_sqla_models()
engine = create_engine("sqlite:///./db/sqlite.db")
stack.metadata.drop_all(engine, checkfirst=False)
stack.metadata.create_all(engine, checkfirst=False)

# Create Pydantic models and fastAPI CRUD routes
stack.create_pydantic_models()
fastapi_app = stack.create_fastapi_routers(engine=engine)
