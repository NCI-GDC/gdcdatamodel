## GDC Level 1 Data Import Plan - DRAFT

### Assumptions
1) Level 1 sequence data (DNAseq, mRNAseq, miRNAseq) for TCGA and TARGET will be obtained from CGHub
2) Metadata for the Level 1 sequence data, both the CGHub XML and the SRA XML, will be obtained from CGHub
3) Level 1 SNP and clinical data will be obtained from the TCGA and TARGET DCCs

### Level 1 Sequence Data

We plan to set up a Postgres database, the GDC Metadata DB, to natively store the XML metadata obtained from CGHub alongside GDC-specific metadata. Postgres provides capabilities to query and index XML without requiring any transformation of the data.

We plan to store the GDC-specific data using JSON, which Postgres also has the capability to store natively. The JSON will consist of key-value pairs that can be easily modified if the fields required changes. An initial list of the metadata fields include:
import_state
md5sum status
import start time
import finish time

A server will be running a metadata synchronization process to update the GDC Metadata DB to stay in sync with the CGHub metadata. This process will retrieve the CGHub metadata programmatically via cgquery, or other automated means, and compare to the metadata stored in the GDC Metadata DB. It will then detect any change between the two databases and update the GDC Metadata DB to be synchronized with the CGHub metadata. Of note, this will contain all metadata provided by CGHub, not just live analysis metadata, so that any state changes that occur in the CGHub metadata will be mirrored in the GDC Metadata DB.

Several servers will then be used to run the download process, using GeneTorrent (gtdownload) to perform the actual download of the data from CGHub. The download process will query the GDC Metadata DB to obtain an analysis ID that has not yet been downloaded to the GDC. The process will then update the GDC metadata to indicate that the analysis is being downloaded. Once the download is complete, the state in the GDC Metadata DB will be updated and an md5sum check will occur. If the md5sum is correct the import will be finished, if it is not the state will be marked as such. We expect to have low rates of md5sum failure so plan to manually investigate any md5sum failures. 

There will also be a process that queries against the GDC Metadata DB to create reports of the current status of the data import into the GDC.

### Clinical & Biospecimen Data

We will use the GDC Metadata DB to store the TCGA Level 1 XML data natively in postgres. 

TARGET will be stored in a tabular format in the GDC Metadata DB to correspond with the cleaned excel spreadsheets. This will enable querying, we will also store and provide the original excel spreadsheets.

There are past versions of these data, will there be new versions?

### SNP
Do we need to do any processing with this data? What metadata should we be storing/indexing? Or do we just host the raw files?
