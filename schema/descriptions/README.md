# Field Descriptions

This directory contains BigQuery Field Description YAML files for schema
definitions in github.com/m-lab/etl/schema.

The content of these files should be formatted as bqx.SchemaDocs from the
github.com/m-lab/go/cloud/bqx package.

Multiple files can be applied to the same schema.

The keys are as follows:
* Description - A short, possibly incomplete, description of a field
* Discussion - Free form discussion of any any aspect of the field that can not
be deduced from other keys.
* Units - If omitted, they are event counts (i.e. error counts).
* Reference - URL to a document describing a canonical version of algorithm
exposed by this instrument.  Kernel code generally does not match exactly.
* Kernel - Kernel variable, field or function (for searching sources) and 
file with primary definition.
* SMItype - Structured Management Information type, where important.
Currently SMItype only calls out Counter32 and Counter64 which have overflow
semantics which are different from Big Query and all programming languages.
In general values that appear to be negative need to be biased by an 
appropriate MAXINT.

## Updating Field Desriptions and Generating New Schema Docs

The schema descriptions found here are used to automatically generate 
schema documentation for the M-Lab website, and therefore need to be
periodically updated to match the schemas in production BigQuery tables.

Update the relevant description file, and then in the root folder of 
this repository, run the following to update the `bindata` used by the 
etl build:

```
go get -u github.com/go-bindata/go-bindata/go-bindata
go generate ./schema
git commit -m 'Update bindata' -- ./schema/bindata.go
```

After review and merging, add a new tag on this repo using the format: `v#.#.#` 
These tags are used to build a new Dockerhub image using that tag, 
named `generate-schema-docs`, which the website uses to generate schema
include files. For example: `measurementlab/generate-schema-docs:v0.1.0`.

Finally, the new tag must be updated in [m-lab/website/.travis.yml](https://github.com/m-lab/website/blob/master/.travis.yml#L23).
