# P K12 Student Data Interchange

# Overview

?



Like all standard Ed-Fi interchanges, this schema references the Ed-Fi Core XSD and can be extended using the Ed-Fi Extensions Framework. See the [Ed-Fi Data Standard - Developers' Guide] for more information.



# Model Details

The following figure shows a logical view of the P K12 Student Data Interchange schema:  

![P K12 Student Data model details diagram](img/InterchangePK12StudentData-interchange-brief.png)


# Entities

The following table describes the primary entities of which the P K12 Student Data Interchange is composed.  

| Name | Description |
|----------|-----------------|
| AnonymizedStudent | Domain entity to collect data for indiviudal students with whom the teacher candidate is associated through field work or student teaching |
| AnonymizedStudentAcademicRecord | The academic record for an anonymized student |
| AnonymizedStudentAssessment | This entity represents the analysis or scoring of a Student's response on an assessment. The analysis results in a value that represents a Student's performance on a set of items on a test. |
| AnonymizedStudentCourseAssociation | Information about the association between an anonymized student and the course they are enrolled in |
| AnonymizedStudentCourseTranscript | The date for which the data element is relevant |
| AnonymizedStudentEducationOrganizationAssociation | Information about the association between an anonymized student and the Education Organziation they are enrolled in |
| AnonymizedStudentSectionAssociation | Information about the association between an anonymized student and the Section they are enrolled in |
| CourseCourseTranscriptFacts | Data about the final grade earned of the group. |
| CourseStudentAcademicRecordFacts | Complex type that provides data about a group of student and their academic record |
| CourseStudentAssessmentFacts | Complex type that provides data about a group of students and their assesssment score results and performance levels |
| CourseStudentFacts | This domain entity collects data for aggregated level students with whom the teacher candidate is associated through field work or student teaching	AggregatedStudentFact |
| EducationOrganizationCourseTranscriptFacts | Data about the final grade earned of the group. |
| EducationOrganizationStudentAcademicRecordFacts | Complex type that provides data about a group of student and their academic record |
| EducationOrganizationStudentAssessmentFacts | Complex type that provides data about a group of students and their assesssment score results and performance levels |
| EducationOrganizationStudentFacts | This domain entity collects data for aggregated level students. |
| SectionCourseTranscriptFacts | Data about the final grade earned of the group. |
| SectionStudentAcademicRecordFacts | Complex type that provides data about a group of students and their academic record. |
| SectionStudentAssessmentFacts | This domain entity collects data for aggregated level students with whom the teacher candidate is associated through field work or student teaching. |
| SectionStudentFacts | This domain entity collects data for aggregated level students. |
| StaffStudentGrowthMeasure | Complex type that provides data about a group of students and their student growth as it pertains to the Teacher Candidate |
| StaffStudentGrowthMeasureCourseAssociation | Any courses associated with the staff's student growth data, if applicable. |
| StaffStudentGrowthMeasureEducationOrganizationAssociation | Any education organizations associated with the staff's student growth data, if applicable. |
| StaffStudentGrowthMeasureSectionAssociation | Any sections associated with the staff's student growth data, if applicable. |
| TeacherCandidateStudentGrowthMeasure | Complex type that provides data about a group of students and their student growth as it pertains to the Teacher Candidate |
| TeacherCandidateStudentGrowthMeasureCourseAssociation | Any courses associated with the teacher candidate's student growth data, if applicable. |
| TeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation | Any education organizations associated with the teacher candidate's student growth data, if applicable. |
| TeacherCandidateStudentGrowthMeasureSectionAssociation | Any sections associated with the teacher candidate's student growth data, if applicable. |
| SchoolStudentGrowthMeasure | Complex type that provides data about a group of students and their student growth as it pertains to a school |



# Extended References


This interchange includes the following Extended References.  

| Extended Reference Name | Description |
|-----------------------------|-----------------|
| CourseReference (in AnonymizedStudent) | Optional.  The course associated with aggregated student data |
| CourseReference (in AnonymizedStudentAssessment) | Optional.  The course associated with aggregated student data |
| CourseReference (in AnonymizedStudentCourseAssociation) | Required.  The Course reference for the association. |
| CourseReference (in AnonymizedStudentCourseTranscript) | Required.  The course recorded in the course transcript entry. |
| CourseReference (in CourseStudentAcademicRecordFacts) | Required.  The course for which the data is associated |
| CourseReference (in CourseStudentAssessmentFacts) | Required.  The course associated with aggregated student data |
| CourseReference (in CourseStudentFacts) | Required.  The course associated with aggregated student data |
| CourseReference (in StaffStudentGrowthMeasureCourseAssociation) | Required.  The Course reference for the association |
| CourseReference (in TeacherCandidateStudentGrowthMeasureCourseAssociation) | Required.  The Course reference for the association |
| EducationOrganizationReference (in AnonymizedStudent) | Optional.  The education organization associated with aggregated student data |
| EducationOrganizationReference (in AnonymizedStudentAcademicRecord) | Required.  The education organization associated with aggregated student data |
| EducationOrganizationReference (in AnonymizedStudentEducationOrganizationAssociation) | Required.  The Education Organization reference for the association. |
| EducationOrganizationReference (in EducationOrganizationStudentAcademicRecordFacts) | Required.  The section associated with aggregated student data |
| EducationOrganizationReference (in EducationOrganizationStudentAssessmentFacts) | Required.  The education organization associated with aggregated student data |
| EducationOrganizationReference (in EducationOrganizationStudentFacts) | Required.  The education organization associated with aggregated student data |
| EducationOrganizationReference (in StaffStudentGrowthMeasureEducationOrganizationAssociation) | Required.  The EducationOrganization reference for the association |
| EducationOrganizationReference (in TeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation) | Required.  The EducationOrganization reference for the association |
| SchoolReference (in SchoolStudentGrowthMeasure) | Required.  School ID associated with a VAM |
| SectionReference (in AnonymizedStudent) | Optional.  The section associated with aggregated student data |
| SectionReference (in AnonymizedStudentAssessment) | Optional.  The section associated with aggregated student data |
| SectionReference (in AnonymizedStudentSectionAssociation) | Required.  The Section reference for the assocation |
| SectionReference (in SectionStudentAcademicRecordFacts) | Required.  The section associated with aggregated student data |
| SectionReference (in SectionStudentAssessmentFacts) | Required.  The section associated with aggregated student data |
| SectionReference (in SectionStudentFacts) | Required.  The section associated with aggregated student data |
| SectionReference (in StaffStudentGrowthMeasureSectionAssociation) | Required.  The Section reference for the association |
| SectionReference (in TeacherCandidateStudentGrowthMeasureSectionAssociation) | Required.  The Section reference for the association |
| StaffReference (in StaffStudentGrowthMeasure) | Required.  The staff associated with aggregated student growth data |
| TeacherCandidateReference (in TeacherCandidateStudentGrowthMeasure) | Required.  The teacher candidate associated with aggregated student growth data |



# Descriptor Dependencies

This interchange references the following Ed-Fi Descriptors, thus requiring them to have been defined using the Descriptors interchange prior to this interchange. For more information on the Ed-Fi Descriptor Pattern, see the [Ed-Fi Data Standard - Developers' Guide].  

| Descriptor Name | Description |
|---------------------|-----------------|
| AcademicSubjectDescriptor | Optional.  This descriptor holds the description of the content or subject area (e.g., arts, mathematics, reading, stenography, or a foreign language). |
| AssessmentCategoryDescriptor | Optional.  This descriptor holds the category of an assessment based on format and content. |
| DisabilityDescriptor | Optional.  This descriptor defines a student's impairment. |
| GradeLevelDescriptor | Optional.  This descriptor defines the set of grade levels. The map to known Ed-Fi enumeration values is required. |
| LanguageDescriptor | Optional.  This descriptor defines the language(s) that are spoken or written. |
| PerformanceLevelDescriptor | Optional.  This descriptor defines various levels or thresholds for performance on the assessment. |
| PopulationCategoryDescriptor | Required.  Section of the school that is associated with a VAM |
| PopulationSubCategoryDescriptor | Required.  Section of the school that is associated with a VAM |
| SchoolFoodServicesEligibilityDescriptor | Optional.  This descriptor defines indications of a student's level of eligibility for breakfast, lunch, snack, supper, and milk programs. |
| StudentGrowthTypeDescriptor | Optional.  Identification of the type of score that was used to determine student growth |
| TermDescriptor | Required.  This descriptor defines the term of a session during the school year (e.g., Fall Semester). |


