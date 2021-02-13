from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from lazystack import LazyStack


# Initialize with Great Expectations
builder = LazyStack(ge_dir="./great_expectations/expectations")

# Create SQLAlchemy models
builder.create_sqla_models()
engine = create_engine("sqlite:///./db/sqlite.db")
builder.metadata.drop_all(engine, checkfirst=False)
builder.metadata.create_all(engine, checkfirst=False)

# Create Pydantic models and fastAPI routes
builder.create_pydantic_models()
fastapi_app = builder.create_fastapi_routers(engine=engine)
