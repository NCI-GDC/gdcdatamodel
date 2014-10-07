
## How to make a plugin

In order to make a plugin, you need to do three things:

##### 1) write a Scheduler(), Conversion(), or Export() class that over-rides the base class

The functions that are required to be over-ridden are listed below:

###### Schedulers

* `initialize(self, **kwargs)`: setup the scheduler and pull **kwargs from settings.yaml
* `load(self, **kwargs)`: make a connection to outside resource and load documents
* `__iter__(self)`: needs to yeild a document until all documents have been converted

###### Conversions

* `initialize(self, **kwargs)`: setup the conversion and pull **kwargs from settings.yaml
* `convert(doc, **kwargs)`: return a converted document
 
###### Exports

* `initialize(self, **kwargs)`: setup the exporter and pull **kwargs from settings.yaml
* `export(doc, **kwargs)`: exports the document to an outside resource

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

The excerpt from the settings file above specifies a pipeline that reads urls from a file and downloads an xml.  It then passes it to the Conversion plugin called `json_flat`, which returns a document containing the flattened json conversion.  Then the converted document is passed to BOTH exporter plugins `stdout` and `postgres`.

##### 3) add any __init__() arguments to the settings.yaml file

Example for an Export() plugin called ```postgres```.

```
postgres: 
  database: name
  user: user
  password: password1
  
  ignoreConversions:
    - flat_nested
  ignoreSchedulers:
    - signpost
```

The super class of the Export plugin will pre-emptively return None if it is passed a document from a Conversion or Scheduler listed in the ignoreConversions, ignoreSchedulers fields respectively

## Requirements
	sudo -E apt-get update
	sudo -E apt-get install python-dev libxml2-dev libxslt-dev

