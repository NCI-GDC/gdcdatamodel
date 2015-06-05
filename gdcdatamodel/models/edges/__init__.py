from derived_from import (AliquotDerivedFromAnalyte, AliquotDerivedFromSample,
                          AnalyteDerivedFromPortion, PortionDerivedFromSample,
                          SampleDerivedFromParticipant,
                          SlideDerivedFromPortion)
from related_to import FileRelatedToFile, ArchiveRelatedToFile
from member_of import (ParticipantMemberOfProject, ProjectMemberOfProgram,
                       ArchiveMemberOfProject, FileMemberOfArchive,
                       FileMemberOfExperimentalStrategy,
                       FileMemberOfDataSubtype, FileMemberOfDataFormat,
                       FileMemeberOfTag, DataSubtypeMemberOfDataType)
from processed_at import ParticipantProcessedAtTissueSourceSite
from generated_from import FileGeneratedFromPlatform
from data_from import (FileDataFromAliquot, FileDataFromAnalyte,
                       FileDataFromPortion, FileDataFromSample,
                       FileDataFromParticipant, FileDataFromSlide)
from describes import FileDescribesParticipant, ClinicalDescribesParticipant
from annotates import (AnnotationAnnotatesParticipant,
                       AnnotationAnnotatesSample,
                       AnnotationAnnotatesSlide, AnnotationAnnotatesPortion,
                       AnnotationAnnotatesAnalyte,
                       AnnotationAnnotatesAliquot,
                       AnnotationAnnotatesFile)
from shipped_to import AliquotShippedToCenter, PortionShippedToCenter
from submitted_by import FileSubmittedByCenter
from refers_to import PublicationRefersToFile
