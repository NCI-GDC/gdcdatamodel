from derived_from import (AliquotDerivedFromAnalyte, AliquotDerivedFromSample,
                          AnalyteDerivedFromPortion, PortionDerivedFromSample,
                          SampleDerivedFromCase,
                          SlideDerivedFromPortion)
from related_to import FileRelatedToFile, ArchiveRelatedToFile
from member_of import (CaseMemberOfProject, ProjectMemberOfProgram,
                       ArchiveMemberOfProject, FileMemberOfArchive,
                       FileMemberOfExperimentalStrategy,
                       FileMemberOfDataSubtype, FileMemberOfDataFormat,
                       FileMemeberOfTag, DataSubtypeMemberOfDataType)
from processed_at import CaseProcessedAtTissueSourceSite
from generated_from import FileGeneratedFromPlatform
from data_from import (FileDataFromAliquot, FileDataFromAnalyte,
                       FileDataFromPortion, FileDataFromSample,
                       FileDataFromCase, FileDataFromSlide,
                       FileDataFromFile)
from describes import FileDescribesCase, ClinicalDescribesCase
from annotates import (AnnotationAnnotatesCase,
                       AnnotationAnnotatesSample,
                       AnnotationAnnotatesSlide, AnnotationAnnotatesPortion,
                       AnnotationAnnotatesAnalyte,
                       AnnotationAnnotatesAliquot,
                       AnnotationAnnotatesFile)
from shipped_to import AliquotShippedToCenter, PortionShippedToCenter
from submitted_by import FileSubmittedByCenter
from refers_to import PublicationRefersToFile
