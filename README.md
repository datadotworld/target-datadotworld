# datadotworld-singer
This is a [singer.io](https://singer.io) target for integrating with [data.world](https://data.world)

## TODO

- [x] Tests   
- [x] CLI interface  
- [x] Streams API (incl. user agent)  
- [x] Batching  
- [x] Backoff / Retry  
- [x] Validation  
- [x] Exception handling  
- [x] Logging  
- [x] Metrics  
- [ ] CircleCI (incl. PyPI)  
- [ ] Docs

## installing

**note** since this isn't yet in pypi, you need to install from source

```
$ python setup.py install
```

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
using the above configurations as `targetconfig.json`, and the following
configuration for `tapconfig.json`:
```
{
  "auth_token": "<INSERT AUTH TOKEN HERE>",
  "dataset_key": "bryon/odin-2015-2016",
  "stream": "overall-odin-score-2016",
  "query": "SELECT country_code, country, overall_subscore FROM odin_2015_2016_standardized WHERE year = ? AND elements = 'All categories' ORDER BY overall_subscore DESC",
  "parameters": ["2016"]
}

```

the following command executes a query with the data.world tap and
writes the results to a new dataset as a `jsonl` file:

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

