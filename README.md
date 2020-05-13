## COVIDPIPE

A sample project of working with Apache Beam to write a pipeline
to analyze interesting trends of COVID 19 data.

Check out the [associated slides for this repository](
https://docs.google.com/presentation/d/1x0nEZDVYwzWYifKG6hdxxhPiUfHJGL-VRj4mAm5pcps/edit?usp=sharing).

NOTE: This was developed by a software engineer, not by a public
health expert. This is meant as a demo of the technology - not as
any sort of tool to draw conclusions regarding COVID-19.

## Setting up a project for a Beam pipeline

You can set up your Beam project like any Python project ([review python
 project structure](https://docs.python-guide.org/writing/structure/).
You’d have a package structure like so:

```
base/
base/setup.py
base/mypackage/
base/mypackage/file.py
base/mypackage/file_test.py
```

When defining Beam as a dependency, note that Beam has multiple tags to specify
particular dependencies. I strongly recommend adding the beam test dependencies.
Like so:

```
pip install apache-beam[test]
pip install apache-beam[test,gcp,aws]
```

You can add other sets of dependencies, such as gcp and aws to add GCP-related
IOs and filesystems. You can also add these specifications to your
requirements.txt file, and to your setup.py’s install_requires field.

I always recommend using a virtual environment.

## Writing a test for your PTransform

Beam has utilities to run, set up, and verify test pipelines. You can use these
to write unit tests, and even integration tests!
Some valuable utilities are:

- Beam has many utilities in apache_beam.testing.util
  - `assert_that` matches PCollections to expected outputs
  - You can use matchers for PCollections. For example:
    - `is_empty`
    - `is_not_empty`
    - `equal_to`
    - `matches_all`

Check out the tests in `covidpipe/datasource_test.py`. These tests use the
various utilities to write / run / verify unit tests with your Beam pipelines.

## Passing parameters to your PTransforms / Pipelines

The [pipeline options](https://beam.apache.org/releases/pydoc/current/apache_beam.options.pipeline_options.html)
 abstraction is what Beam uses to pass parameters to a pipeline.

For a complex pipeline, you should define a PipelineOptions class to hold your pipeline arguments. This is useful because the options you pass will be available at pipeline execution as well.

