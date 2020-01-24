# Field Descriptions

This directory contains BigQuery Field Description YAML files for schema
definitions in github.com/m-lab/etl/schema.

The content of these files should be formatted as bqx.SchemaDocs from the
github.com/m-lab/go/bqx package.

Multiple files can be applied to the same schema.

The keys are as follows:
* Description - A short, possibly incomplete, description of a field
* Discussion - Free form discussion of any any aspect of the field that can not
be deduced from other keys.
* Units - If omitted, they are event counts (i.e. error counts).
* Reference - URL to a document describing a canonical version of algorithm
exposed by this instrument.  Kernel code generally does not match exactly.
* Kernel - Kernel variable, field or function (for searching sources) and file
with primary definition.
* SMItype - Structured Management Information type, where important.
Currently SMItype only calls out Counter32 and Counter64 which have overflow
semantics which
are different from Big Query and all programming languages.  In general values
that appear to be negative need to be biased by an appropriate MAXINT.
