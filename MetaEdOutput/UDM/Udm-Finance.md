# Finance
---
The Finance domain is designed to represent relevant financial information, including:
* Budgets and actuals
* Federal Fund allocations and expenses
* Staff and contracted staff actuals

Core to the Finance model is the Account entity, representing the chart of accounts, or an organization of aggregate accounts. The various codes (e.g., Object, Function,Fund) specific to the EducationOrganization are represented by AccountCodeDescriptor.



#### Finance Model Entities

| Name        | Description  |
|-----------------|------------------|
| Account |  |
| Actual | This financial entity represents the sum of the financial transactions to date relating to a specific account. |
| Budget | This financial entity represents the amount of monies allocated to be spent or received by an education organization as related to a specific account. |
| ContractedStaff | This financial entity represents the sum of the financial transactions to date for contracted staff. ContractedStaff includes "contractors" or "consultants" who perform services for an agreed upon fee, or an employee of a management service contracted to work on site. |
| EducationOrganization | This entity represents any public or private institution, organization, or agency that provides instructional or support services to students or staff at any level. |
| EducationServiceCenter | This entity represents a regional, multi-services public agency authorized by state law to develop, manage and provide services, programs, or other support options (e.g., construction, food services, and technology services) to LEAs. |
| LocalEducationAgency | This entity represents an administrative unit at the local level which exists primarily to operate schools or to contract for educational services. It includes school districts, charter schools, charter management organizations, or other local administrative organizations. |
| Payroll | This financial entity represents the sum of the financial transactions to date for employee compensation. An "employee" who performs services under the direction of the employing institution or agency, is compensated for such services by the employer and is eligible for employee benefits and wage or salary tax withholdings. |
| Staff |  |
| StateEducationAgency | This entity represents the agency of the state charged with the primary responsibility for coordinating and supervising public instruction, including the setting of standards for elementary and secondary instructional programs. |


![Finance Model Diagram](/path/to/domain-model.png)
#### Finance Model  

