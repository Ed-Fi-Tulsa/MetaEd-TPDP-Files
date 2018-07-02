# Descriptors Interchange

# Overview

Descriptors provide states, districts, vendors, and other users of the Ed-Fi solution with the flexibility to use their own enumerations and code sets without modifying the Ed-Fi core schema.

The Descriptor interchange is used to describe metadata about the descriptors and their structure. It is used to define enumeration vocabularies that are not "fixed" within the XML schema, but are loaded in XML files and linked to their source.



Like all standard Ed-Fi interchanges, this schema references the Ed-Fi Core XSD and can be extended using the Ed-Fi Extensions Framework. See the [Ed-Fi Data Standard - Developers' Guide] for more information.


# Use Cases

The Descriptors Interchange can be used to:  

1. Exchange state, district, or vendor code sets in a way that Ed-Fi technology implementations can understand the semantics
2. Exchange code or enumeration values that change over time, but where longitudinal analysis is still important


# Model Details

The following figure shows a logical view of the Descriptors Interchange schema:  

![Descriptors model details diagram](img/InterchangeDescriptors-interchange-brief.png)


# Entities

The following table describes the primary entities of which the Descriptors Interchange is composed.  

| Name | Description |
|----------|-----------------|
| AcademicSubjectDescriptor | This descriptor holds the description of the content or subject area (e.g., arts, mathematics, reading, stenography, or a foreign language). |
| AccommodationDescriptor | This descriptor defines variations used in how an assessment is presented or taken. |
| AccountCodeDescriptor | This descriptor holds the set of account codes defined for the education accounting system organized by account code type (e.g., fund, function, object). |
| AchievementCategoryDescriptor | This descriptor defines the category of achievement attributed to the learner. |
| AdministrativeFundingControlDescriptor | This descriptor holds the type of education institution as classified by its funding source (e.g., public or private). |
| AssessmentCategoryDescriptor | This descriptor holds the category of an assessment based on format and content. |
| AssessmentIdentificationSystemDescriptor | This descriptor holds a coding scheme that is used for identification and record-keeping purposes by schools, social services or other agencies to refer to an assessment. |
| AssessmentPeriodDescriptor | This descriptor holds the period of time window in which an assessment is supposed to be administered (e.g., Beginning of Year, Middle of Year, End of Year). |
| AttendanceEventCategoryDescriptor | This descriptor holds the category of the attendance event (e.g., tardy). The map to known enumeration values is required. |
| BehaviorDescriptor | This descriptor holds the categories of behavior describing a discipline incident. |
| CalendarEventDescriptor | This descriptor holds the types of scheduled or unscheduled event for the day (e.g., Instructional day, Teacher only day, Holiday, Make-up day, Weather day, Student late arrival/early dismissal day). |
| ClassroomPositionDescriptor | This descriptor defines the type of position the staff member holds in a specific class/section. |
| CompetencyLevelDescriptor | This descriptor defines various levels for assessed competencies. |
| ContinuationOfServicesReasonDescriptor | In the Migrant Education program, a provision allows continuation of services after a child is no longer considered migratory for certain reasons. This descriptor holds the reasons prescribed in the statute. The mapping of descriptor values to known Ed-Fi enumeration values is required. |
| CountryDescriptor | This descriptor defines the name and code of the country. |
| CourseIdentificationSystemDescriptor | This descriptor defines a standard code that identifies the organization of subject matter and related learning experiences provided for the instruction of students. |
| CredentialFieldDescriptor | This descriptor defines the fields of certification that the state education agency offers to teachers. |
| DiagnosisDescriptor | This descriptor defines diagnoses that interventions are intended to target. |
| DisabilityDescriptor | This descriptor defines a student's impairment. |
| DisciplineDescriptor | This descriptor defines the type of action or removal from the classroom used to discipline the student involved as a perpetrator in a discipline incident. |
| EducationOrganizationIdentificationSystemDescriptor | This descriptor defines the originating record system and code that is used for record-keeping purposes by education organizations. |
| EmploymentStatusDescriptor | This descriptor defines the type of employment or contract. |
| EntryTypeDescriptor | This descriptor defines the process by which a student enters a school during a given academic session. |
| ExitWithdrawTypeDescriptor | This descriptor defines the circumstances under which the student exited from membership in an educational institution. |
| FederalLocaleCodeDescriptor | (TPDP Extension) The descriptor holds the federal locale code applicable to an education organization. |
| GradeLevelDescriptor | This descriptor defines the set of grade levels. The map to known Ed-Fi enumeration values is required. |
| GradingPeriodDescriptor | This descriptor defines the name of the period for which grades are reported. The mapping of descriptor values to known Ed-Fi enumeration values is required. |
| GraduationPlanTypeDescriptor | This descriptor defines the set of graduation plan types. |
| LanguageDescriptor | This descriptor defines the language(s) that are spoken or written. |
| LevelDescriptor | This descriptor defines the grade level(s) certified for teaching. |
| LevelOfEducationDescriptor | This descriptor defines the different levels of education achievable. |
| LimitedEnglishProficiencyDescriptor | This descriptor defines the indications that the student has been identified as limited English proficient by the Language Proficiency Assessment Committee (LPAC), or English proficient. The mapping of descriptor values to known Ed-Fi enumeration values is required. |
| PerformanceLevelDescriptor | This descriptor defines various levels or thresholds for performance on the assessment. |
| ProgramAssignmentDescriptor | This descriptor defines the name of the education program for which a teacher is assigned to a school. |
| ProgramCharacteristicDescriptor | This descriptor defines important characteristics of the Program, such as categories or particular indications. |
| ReasonExitedDescriptor | This descriptor defines the reason a student exited a program. |
| ReporterDescriptionDescriptor | This descriptor defines the type of individual who reported an incident. |
| ResidencyStatusDescriptor | This descriptor defines indications of the location of a person's legal residence relative to (within or outside of) the boundaries of the public school attended and its administrative unit. |
| ResponsibilityDescriptor | This descriptor defines types of responsibility an education organization may have for a student (e.g., accountability, attendance, funding). |
| SchoolFoodServicesEligibilityDescriptor | This descriptor defines indications of a student's level of eligibility for breakfast, lunch, snack, supper, and milk programs. |
| SectionCharacteristicDescriptor | This descriptor defines characteristics of a Section, such as whether attendance is taken and the Section is graded. |
| SeparationReasonDescriptor | This descriptor defines the reasons for terminating the employment. |
| ServiceDescriptor | This descriptor defines the services provided by an education organization to populations of students associated with a program. |
| SpecialEducationSettingDescriptor | This descriptor defines the major instructional setting (more than 50 percent of a student's special education program). |
| StaffClassificationDescriptor | This descriptor defines an individual's title of employment, official status or rank. |
| StaffIdentificationSystemDescriptor | This descriptor defines the originating record system and code that is used for record-keeping purposes of the staff. |
| StudentCharacteristicDescriptor | This descriptor defines the set of important characteristics of the student. |
| StudentIdentificationSystemDescriptor | This descriptor defines the originating record system and code that is used for record-keeping purposes of the student. |
| TeachingCredentialDescriptor | This descriptor defines an indication of the category of a legal document giving authorization to perform teaching assignment services. |
| TermDescriptor | This descriptor defines the term of a session during the school year (e.g., Fall Semester). |
| WeaponDescriptor | This descriptor defines the types of weapon used during an incident. |
| AbsenceEventCategoryDescriptor | The descriptor holds the code describing the type of leave taken, for example: Sick, Personal, Vacation. |
| AidTypeDescriptor | This descriptor defines the classification of financial aid awarded to a person for the academic term/year. |
| BackgroundCheckStatusDescriptor | This descriptor holds the  status of the background check (e.g., pending, under investigation, offense(s) found, etc.). |
| BackgroundCheckTypeDescriptor | This descriptor defines the classification of the background check a person receives. |
| BoardCertificationTypeDescriptor | The descriptor holds the type of board certification awarded to an individual. |
| CertificationExamTypeDescriptor | The descriptor holds the type of certification exam that was taken. |
| EnglishLanguageExamDescriptor | Indicates that a person passed, failed, or did not take an English Language assessment (e.g., TOEFFL). |
| FieldworkTypeDescriptor | The descriptor holds the type of fieldwork being executed by a teacher candidate. |
| LevelOfDegreeAwardedDescriptor | The descriptor holds the level of degree awarded by the teacher prep program to the person (e.g., Certificate Only, Bachelor's, Master's, etc.). |
| LevelTypeDescriptor | The descriptor holds the level (e.g., 1, 2, 3, etc.) of the rubric at which the data is captured. |
| PerformanceMeasureTypeDescriptor | The descriptor holds the type (e.g., walkthrough, summative) of observation conducted. |
| PopulationCategoryDescriptor | Section of the school that is associated with a VAM |
| PopulationSubCategoryDescriptor | Section of the school that is associated with a VAM |
| PreviousCareerDescriptor | The descriptor holds the previous career of an individual. |
| ProfessionalDevelopmentOfferedByDescriptor | The descriptor holds the organization that a professional development is offered by. |
| ProgramGatewayDescriptor | The descriptor holds the program gateway that is associated with continuation in a program. |
| RubricTypeDescriptor | The descriptor holds the rubric types. |
| SalaryTypeDescriptor | The descriptor holds the type of salary that a staff member is receiving. |
| SchoolStatusDescriptor | The descriptor holds the status of a school e.g. priority or focus. |
| StudentGrowthTypeDescriptor | Identification of the type of score that was used to determine student growth |
| SurveyCategoryDescriptor | The descriptor holds the category or type of survey. |
| TeacherCandidateCharacteristicDescriptor | The characteristic designated for the TeacherCandidate. |
| TeacherPreparationProgramTypeDescriptor | The descriptor holds the type of teacher prep program (e.g., college, alternative, TFA, etc.). |
| ThemeDescriptor | The descriptor holds the theme of the specified level of the rubric. |
| TPPDegreeTypeDescriptor | The descriptor holds the degree that a teacher candidate accomplishes. |
| TPPProgramPathwayDescriptor | The descriptor holds the program pathways available at TPP's. |



# Extended References


This interchange contains no external references.


