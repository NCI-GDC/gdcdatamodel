
## How to make a plugin

In order to make a plugin, you need to do three things:

##### 1) copy paste the base and override the following functions

The functions that can be over-ridden are listed below:

* `initialize(self, **kwargs)`: setup the scheduler and pull **kwargs from settings.yaml
* `process(self, **kwargs)`: make a connection to outside resource and load documents

##### 2) add the name of the file to the settings.yaml file

### OR

##### 1) write a callable 

Write a function that takes a doc and returns a doc:
`process(doc)`. Decorate it with `@zug.process`.

##### 2) add the name of the file to the settings.yaml file


```
plugins:
  schedulers:
    - file
  conversions:
    - json_flat
  exports:
    - stdout
    - postgres
```

##### 3) add kwargs to the settings file per plugin

```
plugin_kwargs:
  uri:
    docs: [/home/ubuntu/workers/data_model/centerCodes.tsv]
  
  tsv2graph:
    type: center
    id_field: Code
      
  graph2neo:
    host: localhost
    port: 7474
```

## Plugin Class

**Don't override __init__(), start(), yieldDoc()**.  You can, but
  don't do it.  The zugflow is based on concurrency.  Each plugin has
  it's own process, and it's own queue that gets piped into it.  Each
  plugin then returns finished documents from process(), or calls
  self.yieldDoc() to pass a finished document to each plugin the next
  level down.


### Things to consider

#### `self.isDaemon = True`

This will prevent the plugin process from closing if its input queue
is empty.  Possibly useful for root plugins that need to handle large
amounts of data and can't enqueue it all at once.

### `self.enqueue(doc)`

This is the main way that a document can schedule work for itself
upfront in initialize().  Keep in mind the default queue len is 10.
If you try to add more docs to the queue in initialize, the app qill
block.  See below.

### Setting the max queue_len

You can set the queue length per plugin, or, for a root plugin, you
can simply replace the imput queue with one of infinite length.

## XML Requirements
	sudo -E apt-get update
	sudo -E apt-get install python-dev libxml2-dev libxslt-dev

## Install for {dev}/{production}
        sudo python setup.py {develop}/{install}
