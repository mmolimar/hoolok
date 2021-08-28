# Hoolok

Hoolok is an extensible and configurable ETL tool allowing data movement, transformation, and validation from
batch and/or streaming sources built on top of [Apache Spark](https://spark.apache.org).

Every *Holook job* uses a YAML file in which all steps in the pipeline are easily configured so that there is no
need to write code. You just have to configure this YAML file setting your Spark config, sources, sinks,
transformations, schema validation, and/or the data quality rules you want to apply for each dataset.

## Motivation

It's very common when writing data pipelines, implementing the same steps over and over again to process data. To avoid
this, you can even structure your code to reuse the way you read, write, or validate your data but sometimes,
and due to the business or technical needs you have, customizations are needed in your pipeline to accomplish it.

Adding new features to your pipeline basically represents making it even more complex on each iteration
(and all the things related with it such as building, testing, deployment, etc), and is also less productive to
deliver new changes.

We are in the *config era* to simplify all these things and reinventing the wheel doesn't make any sense. This is
the gap Hoolok is aiming to fill: speed up the delivery of data pipelines just writing simple YAML config files.

## Getting started

### Requirements

Before starting, you'll need to have installed JDK 11 and [SBT](https://www.scala-sbt.org/).

### Building from source

Just clone the ``hoolook`` repo and build it:

``git clone https://github.com/mmolimar/hoolok.git && cd hoolok``

``sbt clean compile && sbt 'set assembly / test := {}' assembly``

To execute unit and integration tests:

``sbt test it:test coverageReport coverageAggregate``

## Architecture

As mentioned above, *Hoolok* uses a YAML file to configure your Spark jobs. The application reads this file
before starting and configures the following components:
- **Spark config**: sets all the available configuration in the ``SparkSession`` (conf and context).
- **Schemas**: registers all available schemas in the job to data validation.
- **Inputs**: defines all the data sources the Spark job is going to use.
- **Steps**: a sequence of steps to apply transformations or other tools to be used in the pipeline.
- **Outputs**: list of writers to store the final results.

### Spark config

In the YAML file there is a section named ``app`` to configure all the things you need in the ``SparkSession``.

- Available settings for ``app`` section:

| Property          | Type                | Required | Description                                                                                                                                                                                         |
|-------------------|---------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name              | String              | Yes      | App name for the job.                                                                                                                                                                               |
| enableHiveSupport | Boolean             | No       | Enables Hive support in Spark.                                                                                                                                                                      |
| streamingPolicy   | String              | No       | Streaming policy to apply when using streaming jobs. Available values: ``default`` (job waits till all streaming jobs finish) or ``failfast`` (job finished when the first streaming job finishes). |
| sparkConf         | Map[String, String] | No       | Spark configurations to set (ie: ``spark.master``, ``spark.sql.warehouse.dir``, etc).``                                                                                                            |
| sparkContext      | SparkContextConfig  | No       | Object setting the Spark context configurations (Hadoop conf, checkpoint dir, etc).                                                                                                                 |

- Available settings for ``sparkContext`` subsection. 

- Available configurations:

| Property            | Type                 | Required | Description                                                                   |
|---------------------|----------------------|----------|-------------------------------------------------------------------------------|
| archive             | String               | No       | Archive to be downloaded and unpacked with the Spark job on every node.       |
| description         | String               | No       | Description of the current job.                                               |
| file                | String               | No       | Extra archive to be downloaded and unpacked with the Spark job on every node. |
| jar                 | String               | No       | JAR dependency for all tasks to be executed on the ``SparkContext``.          |
| hadoopConfiguration | Map[String, String] | No       | Hadoop Configuration for the Hadoop code (e.g. file systems).                 |
| checkpointDir       | String               | No       | Directory under which RDDs are going to be checkpointed.                      |

This would be a sample configuration for the ``app`` section:

```yaml
app:
  name: SampleJob
  enableHiveSupport: true
  sparkConf:
    spark.sql.adaptive.enabled: "true"
    spark.master: local
    spark.sql.warehouse.dir: /path/to/warehouse
  sparkContext:
    description: Sample job description
    hadoopConfiguration:
      hive.default.fileformat: delta
      hive.default.fileformat.managed: delta
```

### Schemas

The schemas section named ``schemas`` is a list of schemas with the following settings:

| Property | Type                | Required | Description                                                                                 |
|----------|---------------------|----------|---------------------------------------------------------------------------------------------|
| id       | String              | Yes      | Unique identifier of the schema.                                                            |
| kind     | String              | Yes      | Type of schema to use. Available values: ``inline``, ``uri``, and ``schema-registry``.      |
| format   | String              | Yes      | Format of the schema. Available values: ``spark-ddl``,``json-schema``, and ``avro-schema``. |
| options  | Map[String, String] | No       | Directory under which RDDs are going to be checkpointed.                                    |

#### Extra options for schemas

- For ``inline`` schema, you'll have to add an option in the ``options`` settings:
  * ``value``: ``<the content of the schema>``
- For ``uri`` schema, you'll have to add an option in the ``options`` settings:
  * ``path``: ``<URI in which the resource is located to be downloaded>``
- For ``schema-registry`` schema, you'll have to add an option in the ``options`` settings:
  * ``schemaRegistryUrls``: ``<Base URL of the Schema Registry>``
  * ``subject``: ``<Schema subject to use>``
  * ``id``: ``<Schema ID to use>``

Notice that in case of using the ``schema-registry`` kind, ``subject`` and/or ``id`` will be required.

#### Schema formats

This represents which is the format about the schema resource itself. It can be defined in the YAML config file
(``inline``), can be a resource in a URI which will be downloaded by the application (``uri``) or can be a schema
stored in the Schema Registry (``schema-registry``).

This would be a sample configuration for the ``schemas`` section:

```yaml
schemas:
  - id: schema_id_1
    kind: inline
    format: spark-ddl
    options:
      value: field1 LONG, field2 STRING, field3 BOOLEAN, field4 INT
  - id: schema_id_2
    kind: uri
    format: json-schema
    options:
      path: https://schemas-host/my-schema.json
  - id: schema_id_3
    kind: schema-registry
    format: avro-schema
    options:
      schemaRegistryUrls: https://schema-registry-url
      subject: schema_subject
      id: "1"
```

### Inputs
TODO

### Steps
TODO

### Outputs
TODO