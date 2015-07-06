-- The intent of this migration is to rename participants to
-- cases. Prior to this, the names of the tables were simple
-- transformations of their class names (e.g. ExperimentalStrategy ->
-- experimentalstrategy). However, CASE is sql syntax, so you can't
-- call a table that. This necessitated adding a prefix to all the
-- table names (ExperimentalStrategy ->
-- node_experimentalstrategy). This script just does this prefixing,
-- with the exception of participant, which becomes node_case (since
-- the class is called Case now)

-- nodes
ALTER TABLE aliquot RENAME TO node_aliquot;
ALTER TABLE program RENAME TO node_program;
ALTER TABLE project RENAME TO node_project;
ALTER TABLE clinical RENAME TO node_clinical;
ALTER TABLE center RENAME TO node_center;
ALTER TABLE sample RENAME TO node_sample;
ALTER TABLE portion RENAME TO node_portion;
ALTER TABLE analyte RENAME TO node_analyte;
ALTER TABLE slide RENAME TO node_slide;
ALTER TABLE file RENAME TO node_file;
ALTER TABLE annotation RENAME TO node_annotation;
ALTER TABLE archive RENAME TO node_archive;
ALTER TABLE tissuesourcesite RENAME TO node_tissuesourcesite;
ALTER TABLE platform RENAME TO node_platform;
ALTER TABLE datatype RENAME TO node_datatype;
ALTER TABLE datasubtype RENAME TO node_datasubtype;
ALTER TABLE tag RENAME TO node_tag;
ALTER TABLE experimentalstrategy RENAME TO node_experimentalstrategy;
ALTER TABLE dataformat RENAME TO node_dataformat;
ALTER TABLE publication RENAME TO node_publication;
-- This is the only line that's not a simple rename, participant
-- becomes case, the whole point of this migration
ALTER TABLE participant RENAME TO node_case;


-- edges

-- these are all renames, with the exception of anything to do
-- with participant / case, which gets renamed (e.g. filedescribesparticipant -> edge_filedescribescase)
ALTER TABLE aliquotderivedfromanalyte RENAME TO edge_aliquotderivedfromanalyte;
ALTER TABLE aliquotderivedfromsample RENAME TO edge_aliquotderivedfromsample;
ALTER TABLE analytederivedfromportion RENAME TO edge_analytederivedfromportion;
ALTER TABLE portionderivedfromsample RENAME TO edge_portionderivedfromsample;
ALTER TABLE samplederivedfromparticipant RENAME TO edge_samplederivedfromcase;
ALTER TABLE slidederivedfromportion RENAME TO edge_slidederivedfromportion;
ALTER TABLE filerelatedtofile RENAME TO edge_filerelatedtofile;
ALTER TABLE archiverelatedtofile RENAME TO edge_archiverelatedtofile;
ALTER TABLE participantmemberofproject RENAME TO edge_casememberofproject;
ALTER TABLE projectmemberofprogram RENAME TO edge_projectmemberofprogram;
ALTER TABLE archivememberofproject RENAME TO edge_archivememberofproject;
ALTER TABLE filememberofarchive RENAME TO edge_filememberofarchive;
ALTER TABLE filememberofexperimentalstrategy RENAME TO edge_filememberofexperimentalstrategy;
ALTER TABLE filememberofdatasubtype RENAME TO edge_filememberofdatasubtype;
ALTER TABLE filememberofdataformat RENAME TO edge_filememberofdataformat;
ALTER TABLE filememeberoftag RENAME TO edge_filememeberoftag;
ALTER TABLE datasubtypememberofdatatype RENAME TO edge_datasubtypememberofdatatype;
ALTER TABLE participantprocessedattissuesourcesite RENAME TO edge_caseprocessedattissuesourcesite;
ALTER TABLE filegeneratedfromplatform RENAME TO edge_filegeneratedfromplatform;
ALTER TABLE filedatafromaliquot RENAME TO edge_filedatafromaliquot;
ALTER TABLE filedatafromanalyte RENAME TO edge_filedatafromanalyte;
ALTER TABLE filedatafromportion RENAME TO edge_filedatafromportion;
ALTER TABLE filedatafromsample RENAME TO edge_filedatafromsample;
ALTER TABLE filedatafromparticipant RENAME TO edge_filedatafromcase;
ALTER TABLE filedatafromslide RENAME TO edge_filedatafromslide;
ALTER TABLE filedatafromfile RENAME TO edge_filedatafromfile;
ALTER TABLE filedescribescase RENAME TO edge_filedescribescase;
ALTER TABLE clinicaldescribesparticipant RENAME TO edge_clinicaldescribescase;
ALTER TABLE annotationannotatesparticipant RENAME TO edge_annotationannotatescase;
ALTER TABLE annotationannotatessample RENAME TO edge_annotationannotatessample;
ALTER TABLE annotationannotatesslide RENAME TO edge_annotationannotatesslide;
ALTER TABLE annotationannotatesportion RENAME TO edge_annotationannotatesportion;
ALTER TABLE annotationannotatesanalyte RENAME TO edge_annotationannotatesanalyte;
ALTER TABLE annotationannotatesaliquot RENAME TO edge_annotationannotatesaliquot;
ALTER TABLE annotationannotatesfile RENAME TO edge_annotationannotatesfile;
ALTER TABLE aliquotshippedtocenter RENAME TO edge_aliquotshippedtocenter;
ALTER TABLE portionshippedtocenter RENAME TO edge_portionshippedtocenter;
ALTER TABLE filesubmittedbycenter RENAME TO edge_filesubmittedbycenter;
ALTER TABLE publicationreferstofile RENAME TO edge_publicationreferstofile;
