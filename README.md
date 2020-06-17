# Mara Avro file writer

**WIP**

Commands to write a SQL query into an [Apache Avro™](https://avro.apache.org/docs/current/) file within an [mara pipeline](https://github.com/mara/mara-pipelines).

Current supported dbs:
* SqlServerDB

&nbsp;

## Installation

To use the library directly:

```
pip install git+https://github.com/hz-lschick/mara-write-avro.git
```

&nbsp;

## Example

```python
from mara_pipelines.pipelines import Pipeline, Task
from mara_pipelines.commands.sql import ExecuteSQL
from mara_write_avro.commands.files import WriteAvroFile

pipeline = Pipeline(
    id="avro_demo",
    description="A small pipeline that demonstrates the avro save")

pipeline.add(
    Task(id='create',
        description="Creates a dummy table with some sample data and writes the data to an avro file",
        commands=[
            ExecuteSQL(sql_statement=f"""
DROP TABLE IF EXISTS public.avro_demo_users (
    name TEXT,
    age INT
)
"""),
            ExecuteSQL(sql_statement=f"""
INSERT INTO public.avro_demo_users (name, age)
VALUES
('Pierre-Simon Laplace', 77),
('John von Neumann', 53)
"""),
            WriteAvroFile(file_name='avro_demo_users.avro', # the output file, will be written into config.data_dir()
                          sql_query='SELECT * FROM public.avro_demo_users' # the SQL query defining the data to be written
                          # note: you ca add a AVRO schema via arg. 'schema' (as a dict) or 'schame_file_name' (from a JSON file)
            )
        ]
    )
)

```

&nbsp;

## Credits

... to [this guide](https://www.perfectlyrandom.org/2019/11/29/handling-avro-files-in-python/) telling how to handle AVRO files in python.
