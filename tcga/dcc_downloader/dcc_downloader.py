'''
Just wanted to sketch out notes:

Around all of this, if the exit state is non-zero should report back with a message. Probably a heartbeat later also.

- Query metadata service for work
{
    "query" : {
        "term" : { "import_state" : "not_started" }
    }
}

- Take first result and claim by posting new doc with
"import_state" : "host_assigned"
"import_host" : <myself>

- If post returns as 201 - proceed, otherwise repeat

- Download md5sum as <archive_url>.md5, if errored report
"import_state" : "error"
"message" : <good error message>

- Now claimed this download, update with
"import_state" : "downloading"
"download_start": <current timestamp>

- Download finish, update with:
"import_state" : "md5summing"
"download_finish" : <current timestamp>
"md5sum_start" : <current timestamp>

- Begin md5sum

- Finish md5sum, if ok report
"md5sum_finish" : <current timestamp>
"import_state" : "uploading"

- If not OK - go back to downloading and retry, if still not ok
"import_state" : "error"
"message" : <good error message>

- Obtain GDC ID from digital ID service
"upload_start" : <current timestamp>
"gdc_did" : <did>

- Once upload complete
"upload_finish" : <current timestamp>

update did url : <swift url>
"import_state" : "complete"
