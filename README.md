![](docs/logos/logo.png)

<p style="display: flex; justify-content: center; align-items: center;">
<i>Automatically create SQL Alchemy models from Great Expectations suites.</i>
</p>

The goal of this package is to automate the combination of a set of awesome existing tools commonly used in projects:
- **[Great Expectations](https://greatexpectations.io/)**. Data testing, documentation, and profiling
- **[SQL Alchemy](https://www.sqlalchemy.org/)**. The Python SQL Toolkit and Object Relational Mapper
- **[Pydantic](https://pydantic-docs.helpmanual.io/)**. Data validation and settings management
- **[FastAPI](https://fastapi.tiangolo.com/)**. Modern, fast (high-performance), web framework


These tools can be glued together as follows:
- From **Great Expectations** suites create **SQL Alchemy** models: *this package*
- From **SQL Alchemy** models create **Pydantic** models: [Pydantic-SQLAlchemy](https://github.com/tiangolo/pydantic-sqlalchemy)
- From **Pydantic** models create **FastAPI** routes: [FastAPI-CRUDRouter](https://github.com/awtkns/fastapi-crudrouter)


> **Note**: This package is merely an opinionated combination of the above existing tools to automate some common tasks. For proper and extensive use of their capabilities use each tool independently.

# How to use

```python
# app.py

from sqlalchemy import create_engine
from lazystack import LazyStack


# Use existing or new Great Expectations suites as starting point to define data sources and constraints
stack = LazyStack(ge_dir="./great_expectations/expectations")

# Create SQLAlchemy models for each GE data source
stack.create_sqla_models()

# Create tables in the database
engine = create_engine("sqlite://")
stack.metadata.create_all(engine)

# Create Pydantic models
stack.create_pydantic_models()

# Create CRUD routes
fastapi_app = stack.create_crud_routes(engine=engine)
```

Then in terminal:
```bash
uvicorn app:fastapi_app --reload --port 8000
```

Go to your browser at [http://localhost:8000/docs](http://localhost:8000/docs) to see the created routes.


# Motivation
Defining data dictionaries (or data quality tests), table schemas, API response models and routes for consuming these tables involves a lot of repetitive and overlapping definitions. 

For example, let's say we have an **employees** CSV dataset and we want to create data quality tests to ensure it is clean, then use it to populate a SQL table and finally expose a REST api to consume it. So we follow the below steps:

**1. Data dictionary**

First, we need to define the high-level business definition of what it should contain.

<table>
<th>Column Name</th>
<th>Data Type</th>
<th>Constraints</th>
<th>Required/Optional</th>

<tr>
    <td>ID</td>
    <td>Integer</td>
    <td>Has to be increasing number</td>
    <td>required</td>
</tr>

<tr>
    <td>Name</td>
    <td>String</td>
    <td>Length has to be up to 50 characters</td>
    <td>required</td>
</tr>

<tr>
    <td>Sex</td>
    <td>String</td>
    <td>Has to be "male" or "female"</td>
    <td>required</td>
</tr>

<tr>
    <td>Age</td>
    <td>Float</td>
    <td>Has to be greater than 0</td>
    <td>required</td>
</tr>

</table>

*Note: Since writing a `great_expectations` suite (JSON) would take too much space for this example, expectations are pesented in tabular format.*
<br>
<br>

**2. Table schema**

Then, define a SQL Alchemy model to query it

```python
from sqlalchemy import Table, Column, Integer, String, Float

employee = Table(
    "employee",
    metadata,

    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(50), nullable=False),
    Column("sex", String(20), nullable=False),
    Column("age", Float, nullable=False),
)
```
<br>

**3. API response model**

Finally, define API CRUD routes and respective response types

```python
from pydantic import BaseModel

class Employee(BaseModel):
    id: int
    name: str
    sex: str
    age: float
```
<br>

**4. API routes**
```python
from typing import List
from fastapi import APIRouter

router = APIRouter()

@router.get("/employees/", response_model=List[Employee])
async def get_employees():
    query = employee.select()
    return await database.fetch_all(query)

@router.get("/employees/{id}", response_model=Employee)
async def get_one_employee(id: int):
    query = employee.select().where(employee.c.id == id)
    return await database.fetch_one(query)

# etc
```


As you can see, there are multiple places in the code (data dictionary, SQL Alchemy model, Pydantic model) where we define the same column names, their data types and constraints. This is prone to errors since in case of a single change in data requirements, we have to make sure that all updates are propagated.

Also, creating models and routes for dozens of tables is boring and distracts us from our main (and more interesting) tasks.

