Zugs
===========

A 'zug' is a deployable GDC data processing microservice. They have a basic shared template with a run.py file that is called to start up the zug. Then any custom python script can be written to perform the processing task required.

Each zug should have a meaningful name that describes the process performed. Examples of zugs include:
  -  tcgadcc_data_downloader: a zug that queries the Neo4j data model and downloads archives that have not yet been downloaded
  -  tcgadcc_datamodel_sync: a zug that pulls in the various XML, code tables and other sources of metadata to construct the GDC data model representation in Neo4j
  
Future zugs will include those that perform bioinformatics QC and harmonization tasks.

Tungsten is 'zug-aware' and can be configured to deploy multiple VMs hosting zug processes. In this way the system components that the zugs are participating in, such as data download, data import, QC tasks can be distributed and scaled.
