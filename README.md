# datadotworld-singer
This is a [singer.io](https://singer.io) tap and target for integrating with [data.world](https://data.world)

## installing

**note** since this isn't yet in pypi, you need to install from source

```
$ python setup.py install
```

## tap-datadotworld
The singer Tap for connecting to data.world executes a SQL or SPARQL
query against data.world, and streams the results as JSON-formatted data
according to the
[Singer Spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

configuration takes several options:
```
{
  "auth_token": "<INSERT AUTH TOKEN HERE>",
  "dataset_key": "bryon/odin-2015-2016",
  "stream": "overall-odin-score-2016",
  "query": "SELECT country_code, country, overall_subscore FROM odin_2015_2016_standardized WHERE year = ? AND elements = 'All categories' ORDER BY overall_subscore DESC",
  "parameters": ["2016"]
}

```

* *auth_token* -
your data.world API token from https://data.world/settings/advanced
* *dataset_key* -
the dataset or project to serve as the base for the query
* *stream* -
the name of the stream to produce (defaults to *"results"*)
* *query* - the query to execute
* *query_type* -
the type of query (*"sql"* or *"sparql"*) to execute
(defaults to *"sql"*)
* *parameters* - parameters to the query -
an array of positional parameters for SQL or a dict of named
parameters for SPARQL (defaults to *None*)

## target-datadotworld
The singer Target for connecting to data.world writes the contents of
the streams as either `.csv` or `.jsonl` (JSON lines) files into a
data.world dataset.

configuration takes several options:

```
{
  "auth_token": "<INSERT AUTH TOKEN HERE>",
  "dataset_key": "bryon/test-singer",
  "timestamp_files": false,
  "output_format": "jsonl"
}
```

* *auth_token* -
your data.world API token from https://data.world/settings/advanced
* *dataset_key* -
the dataset or project into which the files should be written
* *timestamp_files* -
if `true`, files are written with a timestamp value, so that each
time a new, unique file is written (defaults to *false*)
* *output_format* -
the type of output file (*"csv"* or *"jsonl"*) to write (defaults to
*"csv"*)

## example
using the above configurations as `tapconfig.json` and `targetconfig.json`,
respectively, the following command executes a query with the data.world
tap and writes the results to a new dataset as a `jsonl` file:

```
$ tap-datadotworld -c tapconfig.json | target-datadotworld -c targetconfig.json
  INFO querying [bryon/odin-2015-2016] with query [SELECT country_code, country, overall_subscore FROM odin_2015_2016_standardized WHERE year = ? AND elements = 'All categories' ORDER BY overall_subscore DESC] streaming to [overall-odin-score-2016]
  INFO writing stream [overall-odin-score-2016] to file [overall-odin-score-2016.jsonl] in dataset [bryon/test-singer]
  INFO wrote 173 records to stream [overall-odin-score-2016]
  INFO wrote 173 rows from stream [overall-odin-score-2016] to file [overall-odin-score-2016.jsonl] in dataset [bryon/test-singer]
```

You can see that source query on data.world at:

https://data.world/bryon/odin-2015-2016/workspace/query?queryid=65f80d21-b072-4a3b-97f3-9f450889093c

You can see the resulting file on data.world at:

https://data.world/bryon/test-singer/workspace/file?filename=overall-odin-score-2016.jsonl

