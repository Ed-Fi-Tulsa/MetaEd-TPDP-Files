# Staff
---
The Staff domain represents a wide variety of staff information:
* Staff captures information about experience and credentials.
* StaffSchoolAssociation captures additional information and associations relating to instructional duties.
* StaffEducationOrganizationEmploymentAssociation represents employment information.
* StaffEducationOrganizationAssignmentAssociation represents position assignments and roles. The employment and assignment associations are separated to accommodate a number of different types of situations. Staff may have multiple employment and assignment associations. Staff may be employed with one organization,like an LEA, whereas their assignments are at other organizations, specifically schools. If an assignment is directly tied to employment (i.e., the staff member's pay would differ based upon the assignments), then an association is used from theStaffEducationOrganizationAssignmentAssociation to the StaffEducationOrganizationEmploymentAssociation.
* LeaveEvent tracks staff leave history.
* OpenStaffPosition indicates the employment postings for an education organization and captures the result of that posting.



#### Staff Model Entities

| Name        | Description  |
|-----------------|------------------|
| EducationOrganization | This entity represents any public or private institution, organization, or agency that provides instructional or support services to students or staff at any level. |
| EducationServiceCenter | This entity represents a regional, multi-services public agency authorized by state law to develop, manage and provide services, programs, or other support options (e.g., construction, food services, and technology services) to LEAs. |
| LeaveEvent |  |
| LocalEducationAgency | This entity represents an administrative unit at the local level which exists primarily to operate schools or to contract for educational services. It includes school districts, charter schools, charter management organizations, or other local administrative organizations. |
| OpenStaffPosition | This entity represents an open staff position that the education organization is seeking to fill. |
| School |  |
| Staff |  |
| StaffEducationOrganizationAssignmentAssociation | This association indicates the education organization to which a staff member provides services; also known as school of service. |
| StaffEducationOrganizationEmploymentAssociation | This association indicates the EducationOrganization an employee, contractor, volunteer, or other service provider is formally associated with typically indicated by which organization the staff member has a services contract with or receives compensation from. |
| StateEducationAgency | This entity represents the agency of the state charged with the primary responsibility for coordinating and supervising public instruction, including the setting of standards for elementary and secondary instructional programs. |
| StaffSchoolAssociation | This association indicates the School(s) to which a staff member provides instructional services. |


![Staff Model Diagram](/path/to/domain-model.png)
#### Staff Model  

