/******Procedure [t_edfi].[usp_eAcademicWeek]******/

CREATE PROCEDURE [t_edfi].[usp_eAcademicWeek]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AcademicWeekSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AcademicWeek]' AS DataTableName
        ,P.[tid_AcademicWeek]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAcademicWeekBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCalendarDateBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        	AND P.[BeginDate] = F.[Date]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[Date] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AcademicWeekSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AcademicWeek]' AS DataTableName
        ,P.[tid_AcademicWeek]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'EndDate = ' + COALESCE(CAST(P.EndDate AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAcademicWeekBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCalendarDateBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        	AND P.[EndDate] = F.[Date]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[Date] IS NULL 
        AND P.[EndDate] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AcademicWeekSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AcademicWeek]' AS DataTableName
        ,P.[tid_AcademicWeek]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAcademicWeekBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eAccount]******/

CREATE PROCEDURE [t_edfi].[usp_eAccount]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AccountSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Account]' AS DataTableName
        ,P.[tid_Account]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAccountBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AccountCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AccountCode]' AS DataTableName
        ,P.[tid_AccountCode]  AS Tid
        ,@Msg +  'AccountCodeDescriptorCodeValue = ' + COALESCE(CAST(P.AccountCodeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AccountCodeDescriptorNamespace = ' + COALESCE(CAST(P.AccountCodeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAccountCodeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAccountCodeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AccountCodeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AccountCodeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AccountCodeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AccountCodeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eAccountabilityRating]******/

CREATE PROCEDURE [t_edfi].[usp_eAccountabilityRating]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AccountabilityRatingSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AccountabilityRating]' AS DataTableName
        ,P.[tid_AccountabilityRating]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAccountabilityRatingBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AccountabilityRatingSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AccountabilityRating]' AS DataTableName
        ,P.[tid_AccountabilityRating]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAccountabilityRatingBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eActual]******/

CREATE PROCEDURE [t_edfi].[usp_eActual]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ActualSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Actual]' AS DataTableName
        ,P.[tid_Actual]  AS Tid
        ,@Msg +  'AccountNumber = ' + COALESCE(CAST(P.AccountNumber AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FiscalYear = ' + COALESCE(CAST(P.FiscalYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetActualBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAccountBySnapshotId](@Ids) F
        ON
        	P.[AccountNumber] = F.[AccountNumber]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FiscalYear] = F.[FiscalYear]
        WHERE F.[AccountNumber] IS NULL 
        AND P.[AccountNumber] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FiscalYear] IS NULL 
        AND P.[FiscalYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eAssessment]******/

CREATE PROCEDURE [t_edfi].[usp_eAssessment]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Assessment]' AS DataTableName
        ,P.[tid_Assessment]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Assessment]' AS DataTableName
        ,P.[tid_Assessment]  AS Tid
        ,@Msg +  'AssessmentCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AssessmentCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AssessmentCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AssessmentCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AssessmentCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Assessment]' AS DataTableName
        ,P.[tid_Assessment]  AS Tid
        ,@Msg +  'AssessmentFamilyTitle = ' + COALESCE(CAST(P.AssessmentFamilyTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentFamilyBySnapshotId](@Ids) F
        ON
        	P.[AssessmentFamilyTitle] = F.[AssessmentFamilyTitle]
        WHERE F.[AssessmentFamilyTitle] IS NULL 
        AND P.[AssessmentFamilyTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Assessment]' AS DataTableName
        ,P.[tid_Assessment]  AS Tid
        ,@Msg +  'AssessmentPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentPeriodDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentPeriodDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentPeriodDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AssessmentPeriodDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AssessmentPeriodDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AssessmentPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AssessmentPeriodDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Assessment]' AS DataTableName
        ,P.[tid_Assessment]  AS Tid
        ,@Msg +  'AssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AssessedGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AssessedGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AssessedGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Assessment]' AS DataTableName
        ,P.[tid_Assessment]  AS Tid
        ,@Msg +  'LowestAssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.LowestAssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LowestAssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.LowestAssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LowestAssessedGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LowestAssessedGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LowestAssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LowestAssessedGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentIdentificationCode]' AS DataTableName
        ,P.[tid_AssessmentIdentificationCode]  AS Tid
        ,@Msg +  'AssessmentIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentIdentificationCodeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentIdentificationSystemDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AssessmentIdentificationSystemDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AssessmentIdentificationSystemDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AssessmentIdentificationSystemDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AssessmentIdentificationSystemDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentScoreSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentScore]' AS DataTableName
        ,P.[tid_AssessmentScore]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentScoreBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentReportingMethodType] F
        ON
        	P.[AssessmentReportingMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentReportingMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentScoreSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentScore]' AS DataTableName
        ,P.[tid_AssessmentScore]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentScoreBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[ResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_AssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentReportingMethodType] F
        ON
        	P.[AssessmentReportingMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentReportingMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_AssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetPerformanceLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PerformanceLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PerformanceLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PerformanceLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PerformanceLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_AssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[ResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentContentStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentContentStandard]' AS DataTableName
        ,P.[tid_AssessmentContentStandard]  AS Tid
        ,@Msg +  'MandatingEducationOrganizationId = ' + COALESCE(CAST(P.MandatingEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentContentStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[MandatingEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[MandatingEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentContentStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentContentStandard]' AS DataTableName
        ,P.[tid_AssessmentContentStandard]  AS Tid
        ,@Msg +  'PublicationStatusType = ' + COALESCE(CAST(P.PublicationStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentContentStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PublicationStatusType] F
        ON
        	P.[PublicationStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PublicationStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentLanguage]' AS DataTableName
        ,P.[tid_AssessmentLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentLanguageBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLanguageDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LanguageDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LanguageDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LanguageDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LanguageDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentSectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentSection]' AS DataTableName
        ,P.[tid_AssessmentSection]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentProgram]' AS DataTableName
        ,P.[tid_AssessmentProgram]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[AssessmentExtensionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AssessmentExtension]' AS DataTableName
        ,P.[tid_AssessmentExtension]  AS Tid
        ,@Msg +  'ProgramGatewayDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramGatewayDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramGatewayDescriptorNamespace = ' + COALESCE(CAST(P.ProgramGatewayDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAssessmentExtensionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProgramGatewayDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ProgramGatewayDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ProgramGatewayDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ProgramGatewayDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ProgramGatewayDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eAssessmentFamily]******/

CREATE PROCEDURE [t_edfi].[usp_eAssessmentFamily]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamily]' AS DataTableName
        ,P.[tid_AssessmentFamily]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamily]' AS DataTableName
        ,P.[tid_AssessmentFamily]  AS Tid
        ,@Msg +  'AssessmentCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AssessmentCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AssessmentCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AssessmentCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AssessmentCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamily]' AS DataTableName
        ,P.[tid_AssessmentFamily]  AS Tid
        ,@Msg +  'ParentAssessmentFamilyTitle = ' + COALESCE(CAST(P.ParentAssessmentFamilyTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentFamilyBySnapshotId](@Ids) F
        ON
        	P.[ParentAssessmentFamilyTitle] = F.[AssessmentFamilyTitle]
        WHERE F.[AssessmentFamilyTitle] IS NULL 
        AND P.[ParentAssessmentFamilyTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamily]' AS DataTableName
        ,P.[tid_AssessmentFamily]  AS Tid
        ,@Msg +  'AssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AssessedGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AssessedGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AssessedGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamily]' AS DataTableName
        ,P.[tid_AssessmentFamily]  AS Tid
        ,@Msg +  'LowestAssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.LowestAssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LowestAssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.LowestAssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LowestAssessedGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LowestAssessedGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LowestAssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LowestAssessedGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilyIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamilyIdentificationCode]' AS DataTableName
        ,P.[tid_AssessmentFamilyIdentificationCode]  AS Tid
        ,@Msg +  'AssessmentIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyIdentificationCodeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentIdentificationSystemDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AssessmentIdentificationSystemDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AssessmentIdentificationSystemDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AssessmentIdentificationSystemDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AssessmentIdentificationSystemDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilyContentStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamilyContentStandard]' AS DataTableName
        ,P.[tid_AssessmentFamilyContentStandard]  AS Tid
        ,@Msg +  'MandatingEducationOrganizationId = ' + COALESCE(CAST(P.MandatingEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyContentStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[MandatingEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[MandatingEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilyContentStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamilyContentStandard]' AS DataTableName
        ,P.[tid_AssessmentFamilyContentStandard]  AS Tid
        ,@Msg +  'PublicationStatusType = ' + COALESCE(CAST(P.PublicationStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyContentStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PublicationStatusType] F
        ON
        	P.[PublicationStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PublicationStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilyLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamilyLanguage]' AS DataTableName
        ,P.[tid_AssessmentFamilyLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyLanguageBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLanguageDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LanguageDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LanguageDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LanguageDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LanguageDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentFamilyAssessmentPeriodSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentFamilyAssessmentPeriod]' AS DataTableName
        ,P.[tid_AssessmentFamilyAssessmentPeriod]  AS Tid
        ,@Msg +  'AssessmentPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentPeriodDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentPeriodDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentFamilyAssessmentPeriodBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentPeriodDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AssessmentPeriodDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AssessmentPeriodDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AssessmentPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AssessmentPeriodDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eAssessmentItem]******/

CREATE PROCEDURE [t_edfi].[usp_eAssessmentItem]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentItemSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentItem]' AS DataTableName
        ,P.[tid_AssessmentItem]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessmentTitle = ' + COALESCE(CAST(P.AssessmentTitle AS NVARCHAR(128)), '') +  '|'+'Version = ' + COALESCE(CAST(P.Version AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentItemBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[AssessedGradeLevelDescriptorCodeValue] = F.[AssessedGradeLevelDescriptorCodeValue]
        	AND P.[AssessedGradeLevelDescriptorNamespace] = F.[AssessedGradeLevelDescriptorNamespace]
        	AND P.[AssessmentTitle] = F.[AssessmentTitle]
        	AND P.[Version] = F.[Version]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[AssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorNamespace] IS NULL 
        AND P.[AssessedGradeLevelDescriptorNamespace] IS NOT NULL
        AND F.[AssessmentTitle] IS NULL 
        AND P.[AssessmentTitle] IS NOT NULL
        AND F.[Version] IS NULL 
        AND P.[Version] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentItemSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentItem]' AS DataTableName
        ,P.[tid_AssessmentItem]  AS Tid
        ,@Msg +  'AssessmentItemCategoryType = ' + COALESCE(CAST(P.AssessmentItemCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentItemBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentItemCategoryType] F
        ON
        	P.[AssessmentItemCategoryType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentItemCategoryType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[AssessmentItemLearningStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[AssessmentItemLearningStandard]' AS DataTableName
        ,P.[tid_AssessmentItemLearningStandard]  AS Tid
        ,@Msg +  'LearningStandardId = ' + COALESCE(CAST(P.LearningStandardId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetAssessmentItemLearningStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) F
        ON
        	P.[LearningStandardId] = F.[LearningStandardId]
        WHERE F.[LearningStandardId] IS NULL 
        AND P.[LearningStandardId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eBellSchedule]******/

CREATE PROCEDURE [t_edfi].[usp_eBellSchedule]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[BellScheduleSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[BellSchedule]' AS DataTableName
        ,P.[tid_BellSchedule]  AS Tid
        ,@Msg +  'Date = ' + COALESCE(CAST(P.Date AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetBellScheduleBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCalendarDateBySnapshotId](@Ids) F
        ON
        	P.[Date] = F.[Date]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[Date] IS NULL 
        AND P.[Date] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[BellScheduleSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[BellSchedule]' AS DataTableName
        ,P.[tid_BellSchedule]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetBellScheduleBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[BellScheduleMeetingTimeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[BellScheduleMeetingTime]' AS DataTableName
        ,P.[tid_BellScheduleMeetingTime]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetBellScheduleMeetingTimeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetClassPeriodBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eBudget]******/

CREATE PROCEDURE [t_edfi].[usp_eBudget]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[BudgetSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Budget]' AS DataTableName
        ,P.[tid_Budget]  AS Tid
        ,@Msg +  'AccountNumber = ' + COALESCE(CAST(P.AccountNumber AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FiscalYear = ' + COALESCE(CAST(P.FiscalYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetBudgetBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAccountBySnapshotId](@Ids) F
        ON
        	P.[AccountNumber] = F.[AccountNumber]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FiscalYear] = F.[FiscalYear]
        WHERE F.[AccountNumber] IS NULL 
        AND P.[AccountNumber] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FiscalYear] IS NULL 
        AND P.[FiscalYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eCalendarDate]******/

CREATE PROCEDURE [t_edfi].[usp_eCalendarDate]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CalendarDateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CalendarDate]' AS DataTableName
        ,P.[tid_CalendarDate]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCalendarDateBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CalendarDateCalendarEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CalendarDateCalendarEvent]' AS DataTableName
        ,P.[tid_CalendarDateCalendarEvent]  AS Tid
        ,@Msg +  'CalendarEventDescriptorCodeValue = ' + COALESCE(CAST(P.CalendarEventDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CalendarEventDescriptorNamespace = ' + COALESCE(CAST(P.CalendarEventDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCalendarDateCalendarEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCalendarEventDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CalendarEventDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CalendarEventDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CalendarEventDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CalendarEventDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eClassPeriod]******/

CREATE PROCEDURE [t_edfi].[usp_eClassPeriod]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ClassPeriodSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ClassPeriod]' AS DataTableName
        ,P.[tid_ClassPeriod]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetClassPeriodBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eCohort]******/

CREATE PROCEDURE [t_edfi].[usp_eCohort]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CohortSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Cohort]' AS DataTableName
        ,P.[tid_Cohort]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCohortBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CohortSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Cohort]' AS DataTableName
        ,P.[tid_Cohort]  AS Tid
        ,@Msg +  'CohortScopeType = ' + COALESCE(CAST(P.CohortScopeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCohortBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CohortScopeType] F
        ON
        	P.[CohortScopeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CohortScopeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CohortSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Cohort]' AS DataTableName
        ,P.[tid_Cohort]  AS Tid
        ,@Msg +  'CohortType = ' + COALESCE(CAST(P.CohortType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCohortBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CohortType] F
        ON
        	P.[CohortType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CohortType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CohortSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Cohort]' AS DataTableName
        ,P.[tid_Cohort]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCohortBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CohortProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CohortProgram]' AS DataTableName
        ,P.[tid_CohortProgram]  AS Tid
        ,@Msg +  'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCohortProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) F
        ON
        	P.[ProgramEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eCompetencyObjective]******/

CREATE PROCEDURE [t_edfi].[usp_eCompetencyObjective]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CompetencyObjective]' AS DataTableName
        ,P.[tid_CompetencyObjective]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CompetencyObjective]' AS DataTableName
        ,P.[tid_CompetencyObjective]  AS Tid
        ,@Msg +  'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eContractedStaff]******/

CREATE PROCEDURE [t_edfi].[usp_eContractedStaff]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ContractedStaffSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ContractedStaff]' AS DataTableName
        ,P.[tid_ContractedStaff]  AS Tid
        ,@Msg +  'AccountNumber = ' + COALESCE(CAST(P.AccountNumber AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FiscalYear = ' + COALESCE(CAST(P.FiscalYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetContractedStaffBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAccountBySnapshotId](@Ids) F
        ON
        	P.[AccountNumber] = F.[AccountNumber]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FiscalYear] = F.[FiscalYear]
        WHERE F.[AccountNumber] IS NULL 
        AND P.[AccountNumber] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FiscalYear] IS NULL 
        AND P.[FiscalYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ContractedStaffSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ContractedStaff]' AS DataTableName
        ,P.[tid_ContractedStaff]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetContractedStaffBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eCourse]******/

CREATE PROCEDURE [t_edfi].[usp_eCourse]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Course]' AS DataTableName
        ,P.[tid_Course]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Course]' AS DataTableName
        ,P.[tid_Course]  AS Tid
        ,@Msg +  'CareerPathwayType = ' + COALESCE(CAST(P.CareerPathwayType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CareerPathwayType] F
        ON
        	P.[CareerPathwayType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CareerPathwayType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Course]' AS DataTableName
        ,P.[tid_Course]  AS Tid
        ,@Msg +  'CourseDefinedByType = ' + COALESCE(CAST(P.CourseDefinedByType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CourseDefinedByType] F
        ON
        	P.[CourseDefinedByType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CourseDefinedByType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Course]' AS DataTableName
        ,P.[tid_Course]  AS Tid
        ,@Msg +  'CourseGPAApplicabilityType = ' + COALESCE(CAST(P.CourseGPAApplicabilityType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CourseGPAApplicabilityType] F
        ON
        	P.[CourseGPAApplicabilityType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CourseGPAApplicabilityType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Course]' AS DataTableName
        ,P.[tid_Course]  AS Tid
        ,@Msg +  'MinimumAvailableCreditType = ' + COALESCE(CAST(P.MinimumAvailableCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[MinimumAvailableCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[MinimumAvailableCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Course]' AS DataTableName
        ,P.[tid_Course]  AS Tid
        ,@Msg +  'MaximumAvailableCreditType = ' + COALESCE(CAST(P.MaximumAvailableCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[MaximumAvailableCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[MaximumAvailableCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Course]' AS DataTableName
        ,P.[tid_Course]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseIdentificationCode]' AS DataTableName
        ,P.[tid_CourseIdentificationCode]  AS Tid
        ,@Msg +  'CourseIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.CourseIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CourseIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.CourseIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseIdentificationCodeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCourseIdentificationSystemDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CourseIdentificationSystemDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CourseIdentificationSystemDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CourseIdentificationSystemDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CourseIdentificationSystemDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseLevelCharacteristicSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseLevelCharacteristic]' AS DataTableName
        ,P.[tid_CourseLevelCharacteristic]  AS Tid
        ,@Msg +  'CourseLevelCharacteristicType = ' + COALESCE(CAST(P.CourseLevelCharacteristicType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseLevelCharacteristicBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CourseLevelCharacteristicType] F
        ON
        	P.[CourseLevelCharacteristicType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CourseLevelCharacteristicType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseOfferedGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseOfferedGradeLevel]' AS DataTableName
        ,P.[tid_CourseOfferedGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseOfferedGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseCompetencyLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseCompetencyLevel]' AS DataTableName
        ,P.[tid_CourseCompetencyLevel]  AS Tid
        ,@Msg +  'CompetencyLevelDescriptorCodeValue = ' + COALESCE(CAST(P.CompetencyLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CompetencyLevelDescriptorNamespace = ' + COALESCE(CAST(P.CompetencyLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseCompetencyLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCompetencyLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CompetencyLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CompetencyLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CompetencyLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CompetencyLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseLearningStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseLearningStandard]' AS DataTableName
        ,P.[tid_CourseLearningStandard]  AS Tid
        ,@Msg +  'LearningStandardId = ' + COALESCE(CAST(P.LearningStandardId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseLearningStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) F
        ON
        	P.[LearningStandardId] = F.[LearningStandardId]
        WHERE F.[LearningStandardId] IS NULL 
        AND P.[LearningStandardId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseLearningObjective]' AS DataTableName
        ,P.[tid_CourseLearningObjective]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'Objective = ' + COALESCE(CAST(P.Objective AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[Objective] = F.[Objective]
        	AND P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[ObjectiveGradeLevelDescriptorCodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[ObjectiveGradeLevelDescriptorNamespace]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[Objective] IS NULL 
        AND P.[Objective] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorNamespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eCourseOffering]******/

CREATE PROCEDURE [t_edfi].[usp_eCourseOffering]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseOfferingSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseOffering]' AS DataTableName
        ,P.[tid_CourseOffering]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseOfferingBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) F
        ON
        	P.[CourseCode] = F.[CourseCode]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[CourseCode] IS NULL 
        AND P.[CourseCode] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseOfferingSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseOffering]' AS DataTableName
        ,P.[tid_CourseOffering]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseOfferingBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseOfferingSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseOffering]' AS DataTableName
        ,P.[tid_CourseOffering]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseOfferingBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSessionBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseOfferingCurriculumUsedSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseOfferingCurriculumUsed]' AS DataTableName
        ,P.[tid_CourseOfferingCurriculumUsed]  AS Tid
        ,@Msg +  'CurriculumUsedType = ' + COALESCE(CAST(P.CurriculumUsedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseOfferingCurriculumUsedBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CurriculumUsedType] F
        ON
        	P.[CurriculumUsedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CurriculumUsedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eCourseTranscript]******/

CREATE PROCEDURE [t_edfi].[usp_eCourseTranscript]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'CourseEducationOrganizationId = ' + COALESCE(CAST(P.CourseEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) F
        ON
        	P.[CourseCode] = F.[CourseCode]
        	AND P.[CourseEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[CourseCode] IS NULL 
        AND P.[CourseCode] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[CourseEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'CourseAttemptResultType = ' + COALESCE(CAST(P.CourseAttemptResultType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CourseAttemptResultType] F
        ON
        	P.[CourseAttemptResultType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CourseAttemptResultType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'CourseRepeatCodeType = ' + COALESCE(CAST(P.CourseRepeatCodeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CourseRepeatCodeType] F
        ON
        	P.[CourseRepeatCodeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CourseRepeatCodeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'AttemptedCreditType = ' + COALESCE(CAST(P.AttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[AttemptedCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AttemptedCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'EarnedCreditType = ' + COALESCE(CAST(P.EarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[EarnedCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EarnedCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'WhenTakenGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.WhenTakenGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'WhenTakenGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.WhenTakenGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[WhenTakenGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[WhenTakenGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[WhenTakenGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[WhenTakenGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'MethodCreditEarnedType = ' + COALESCE(CAST(P.MethodCreditEarnedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[MethodCreditEarnedType] F
        ON
        	P.[MethodCreditEarnedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[MethodCreditEarnedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscript]' AS DataTableName
        ,P.[tid_CourseTranscript]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CourseTranscriptEarnedAdditionalCreditsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[CourseTranscriptEarnedAdditionalCredits]' AS DataTableName
        ,P.[tid_CourseTranscriptEarnedAdditionalCredits]  AS Tid
        ,@Msg +  'AdditionalCreditType = ' + COALESCE(CAST(P.AdditionalCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCourseTranscriptEarnedAdditionalCreditsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AdditionalCreditType] F
        ON
        	P.[AdditionalCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AdditionalCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eCredential]******/

CREATE PROCEDURE [t_edfi].[usp_eCredential]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Credential]' AS DataTableName
        ,P.[tid_Credential]  AS Tid
        ,@Msg +  'CredentialFieldDescriptorCodeValue = ' + COALESCE(CAST(P.CredentialFieldDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CredentialFieldDescriptorNamespace = ' + COALESCE(CAST(P.CredentialFieldDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCredentialBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCredentialFieldDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CredentialFieldDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CredentialFieldDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CredentialFieldDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CredentialFieldDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Credential]' AS DataTableName
        ,P.[tid_Credential]  AS Tid
        ,@Msg +  'CredentialType = ' + COALESCE(CAST(P.CredentialType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCredentialBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CredentialType] F
        ON
        	P.[CredentialType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CredentialType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Credential]' AS DataTableName
        ,P.[tid_Credential]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCredentialBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Credential]' AS DataTableName
        ,P.[tid_Credential]  AS Tid
        ,@Msg +  'StateOfIssueStateAbbreviationType = ' + COALESCE(CAST(P.StateOfIssueStateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCredentialBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[StateOfIssueStateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[StateOfIssueStateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Credential]' AS DataTableName
        ,P.[tid_Credential]  AS Tid
        ,@Msg +  'TeachingCredentialBasisType = ' + COALESCE(CAST(P.TeachingCredentialBasisType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCredentialBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[TeachingCredentialBasisType] F
        ON
        	P.[TeachingCredentialBasisType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[TeachingCredentialBasisType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[CredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Credential]' AS DataTableName
        ,P.[tid_Credential]  AS Tid
        ,@Msg +  'TeachingCredentialDescriptorCodeValue = ' + COALESCE(CAST(P.TeachingCredentialDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TeachingCredentialDescriptorNamespace = ' + COALESCE(CAST(P.TeachingCredentialDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetCredentialBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetTeachingCredentialDescriptorBySnapshotId](@Ids) F
        ON
        	P.[TeachingCredentialDescriptorCodeValue] = F.[CodeValue]
        	AND P.[TeachingCredentialDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[TeachingCredentialDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[TeachingCredentialDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CredentialExtensionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CredentialExtension]' AS DataTableName
        ,P.[tid_CredentialExtension]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCredentialExtensionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ValueType] F
        ON
        	P.[ValueType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ValueType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CredentialBoardCertificationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CredentialBoardCertification]' AS DataTableName
        ,P.[tid_CredentialBoardCertification]  AS Tid
        ,@Msg +  'BoardCertificationTypeDescriptorCodeValue = ' + COALESCE(CAST(P.BoardCertificationTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BoardCertificationTypeDescriptorNamespace = ' + COALESCE(CAST(P.BoardCertificationTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCredentialBoardCertificationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetBoardCertificationTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[BoardCertificationTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[BoardCertificationTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[BoardCertificationTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[BoardCertificationTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CredentialCertificationExamSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CredentialCertificationExam]' AS DataTableName
        ,P.[tid_CredentialCertificationExam]  AS Tid
        ,@Msg +  'CertificationExamTypeDescriptorCodeValue = ' + COALESCE(CAST(P.CertificationExamTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CertificationExamTypeDescriptorNamespace = ' + COALESCE(CAST(P.CertificationExamTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCredentialCertificationExamBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetCertificationExamTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CertificationExamTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CertificationExamTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CertificationExamTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CertificationExamTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CredentialRecommendationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CredentialRecommendation]' AS DataTableName
        ,P.[tid_CredentialRecommendation]  AS Tid
        ,@Msg +  'CredentialFieldDescriptorCodeValue = ' + COALESCE(CAST(P.CredentialFieldDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CredentialFieldDescriptorNamespace = ' + COALESCE(CAST(P.CredentialFieldDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCredentialRecommendationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCredentialFieldDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CredentialFieldDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CredentialFieldDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CredentialFieldDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CredentialFieldDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CredentialRecommendationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CredentialRecommendation]' AS DataTableName
        ,P.[tid_CredentialRecommendation]  AS Tid
        ,@Msg +  'LevelDescriptorCodeValue = ' + COALESCE(CAST(P.LevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LevelDescriptorNamespace = ' + COALESCE(CAST(P.LevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCredentialRecommendationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CredentialRecommendationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CredentialRecommendation]' AS DataTableName
        ,P.[tid_CredentialRecommendation]  AS Tid
        ,@Msg +  'TeachingCredentialDescriptorCodeValue = ' + COALESCE(CAST(P.TeachingCredentialDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TeachingCredentialDescriptorNamespace = ' + COALESCE(CAST(P.TeachingCredentialDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCredentialRecommendationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetTeachingCredentialDescriptorBySnapshotId](@Ids) F
        ON
        	P.[TeachingCredentialDescriptorCodeValue] = F.[CodeValue]
        	AND P.[TeachingCredentialDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[TeachingCredentialDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[TeachingCredentialDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CredentialRecommendingInstitutionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CredentialRecommendingInstitution]' AS DataTableName
        ,P.[tid_CredentialRecommendingInstitution]  AS Tid
        ,@Msg +  'RecommendingInstitutionCountryDescriptorCodeValue = ' + COALESCE(CAST(P.RecommendingInstitutionCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'RecommendingInstitutionCountryDescriptorNamespace = ' + COALESCE(CAST(P.RecommendingInstitutionCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCredentialRecommendingInstitutionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[RecommendingInstitutionCountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[RecommendingInstitutionCountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[RecommendingInstitutionCountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[RecommendingInstitutionCountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CredentialRecommendingInstitutionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CredentialRecommendingInstitution]' AS DataTableName
        ,P.[tid_CredentialRecommendingInstitution]  AS Tid
        ,@Msg +  'RecommendingInstitutionStateAbbreviationType = ' + COALESCE(CAST(P.RecommendingInstitutionStateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCredentialRecommendingInstitutionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[RecommendingInstitutionStateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RecommendingInstitutionStateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eDisciplineAction]******/

CREATE PROCEDURE [t_edfi].[usp_eDisciplineAction]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineActionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineAction]' AS DataTableName
        ,P.[tid_DisciplineAction]  AS Tid
        ,@Msg +  'DisciplineActionLengthDifferenceReasonType = ' + COALESCE(CAST(P.DisciplineActionLengthDifferenceReasonType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineActionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[DisciplineActionLengthDifferenceReasonType] F
        ON
        	P.[DisciplineActionLengthDifferenceReasonType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[DisciplineActionLengthDifferenceReasonType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineActionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineAction]' AS DataTableName
        ,P.[tid_DisciplineAction]  AS Tid
        ,@Msg +  'ResponsibilitySchoolId = ' + COALESCE(CAST(P.ResponsibilitySchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineActionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[ResponsibilitySchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[ResponsibilitySchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineActionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineAction]' AS DataTableName
        ,P.[tid_DisciplineAction]  AS Tid
        ,@Msg +  'AssignmentSchoolId = ' + COALESCE(CAST(P.AssignmentSchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineActionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[AssignmentSchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[AssignmentSchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineActionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineAction]' AS DataTableName
        ,P.[tid_DisciplineAction]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineActionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineActionDisciplineSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineActionDiscipline]' AS DataTableName
        ,P.[tid_DisciplineActionDiscipline]  AS Tid
        ,@Msg +  'DisciplineDescriptorCodeValue = ' + COALESCE(CAST(P.DisciplineDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisciplineDescriptorNamespace = ' + COALESCE(CAST(P.DisciplineDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineActionDisciplineBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetDisciplineDescriptorBySnapshotId](@Ids) F
        ON
        	P.[DisciplineDescriptorCodeValue] = F.[CodeValue]
        	AND P.[DisciplineDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[DisciplineDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[DisciplineDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineActionDisciplineIncidentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineActionDisciplineIncident]' AS DataTableName
        ,P.[tid_DisciplineActionDisciplineIncident]  AS Tid
        ,@Msg +  'IncidentIdentifier = ' + COALESCE(CAST(P.IncidentIdentifier AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineActionDisciplineIncidentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetDisciplineIncidentBySnapshotId](@Ids) F
        ON
        	P.[IncidentIdentifier] = F.[IncidentIdentifier]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[IncidentIdentifier] IS NULL 
        AND P.[IncidentIdentifier] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineActionStaffSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineActionStaff]' AS DataTableName
        ,P.[tid_DisciplineActionStaff]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineActionStaffBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eDisciplineIncident]******/

CREATE PROCEDURE [t_edfi].[usp_eDisciplineIncident]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineIncidentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineIncident]' AS DataTableName
        ,P.[tid_DisciplineIncident]  AS Tid
        ,@Msg +  'IncidentLocationType = ' + COALESCE(CAST(P.IncidentLocationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineIncidentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[IncidentLocationType] F
        ON
        	P.[IncidentLocationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[IncidentLocationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineIncidentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineIncident]' AS DataTableName
        ,P.[tid_DisciplineIncident]  AS Tid
        ,@Msg +  'ReporterDescriptionDescriptorCodeValue = ' + COALESCE(CAST(P.ReporterDescriptionDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ReporterDescriptionDescriptorNamespace = ' + COALESCE(CAST(P.ReporterDescriptionDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineIncidentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetReporterDescriptionDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ReporterDescriptionDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ReporterDescriptionDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ReporterDescriptionDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ReporterDescriptionDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineIncidentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineIncident]' AS DataTableName
        ,P.[tid_DisciplineIncident]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineIncidentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineIncidentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineIncident]' AS DataTableName
        ,P.[tid_DisciplineIncident]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineIncidentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineIncidentBehaviorSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineIncidentBehavior]' AS DataTableName
        ,P.[tid_DisciplineIncidentBehavior]  AS Tid
        ,@Msg +  'BehaviorDescriptorCodeValue = ' + COALESCE(CAST(P.BehaviorDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BehaviorDescriptorNamespace = ' + COALESCE(CAST(P.BehaviorDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineIncidentBehaviorBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetBehaviorDescriptorBySnapshotId](@Ids) F
        ON
        	P.[BehaviorDescriptorCodeValue] = F.[CodeValue]
        	AND P.[BehaviorDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[BehaviorDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[BehaviorDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[DisciplineIncidentWeaponSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[DisciplineIncidentWeapon]' AS DataTableName
        ,P.[tid_DisciplineIncidentWeapon]  AS Tid
        ,@Msg +  'WeaponDescriptorCodeValue = ' + COALESCE(CAST(P.WeaponDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'WeaponDescriptorNamespace = ' + COALESCE(CAST(P.WeaponDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetDisciplineIncidentWeaponBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetWeaponDescriptorBySnapshotId](@Ids) F
        ON
        	P.[WeaponDescriptorCodeValue] = F.[CodeValue]
        	AND P.[WeaponDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[WeaponDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[WeaponDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eEducationContent]******/

CREATE PROCEDURE [t_edfi].[usp_eEducationContent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationContentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationContent]' AS DataTableName
        ,P.[tid_EducationContent]  AS Tid
        ,@Msg +  'ContentClassType = ' + COALESCE(CAST(P.ContentClassType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationContentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ContentClassType] F
        ON
        	P.[ContentClassType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ContentClassType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationContentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationContent]' AS DataTableName
        ,P.[tid_EducationContent]  AS Tid
        ,@Msg +  'CostRateType = ' + COALESCE(CAST(P.CostRateType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationContentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CostRateType] F
        ON
        	P.[CostRateType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CostRateType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationContentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationContent]' AS DataTableName
        ,P.[tid_EducationContent]  AS Tid
        ,@Msg +  'InteractivityStyleType = ' + COALESCE(CAST(P.InteractivityStyleType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationContentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[InteractivityStyleType] F
        ON
        	P.[InteractivityStyleType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[InteractivityStyleType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationContentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationContent]' AS DataTableName
        ,P.[tid_EducationContent]  AS Tid
        ,@Msg +  'LearningStandardId = ' + COALESCE(CAST(P.LearningStandardId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationContentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) F
        ON
        	P.[LearningStandardId] = F.[LearningStandardId]
        WHERE F.[LearningStandardId] IS NULL 
        AND P.[LearningStandardId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationContentAppropriateSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationContentAppropriateSex]' AS DataTableName
        ,P.[tid_EducationContentAppropriateSex]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationContentAppropriateSexBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SexType] F
        ON
        	P.[SexType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SexType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationContentAppropriateGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationContentAppropriateGradeLevel]' AS DataTableName
        ,P.[tid_EducationContentAppropriateGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationContentAppropriateGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationContentLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationContentLanguage]' AS DataTableName
        ,P.[tid_EducationContentLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationContentLanguageBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLanguageDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LanguageDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LanguageDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LanguageDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LanguageDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eEducationOrganization]******/

CREATE PROCEDURE [t_edfi].[usp_eEducationOrganization]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganization]' AS DataTableName
        ,P.[tid_EducationOrganization]  AS Tid
        ,@Msg +  'FederalLocaleCodeDescriptorCodeValue = ' + COALESCE(CAST(P.FederalLocaleCodeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'FederalLocaleCodeDescriptorNamespace = ' + COALESCE(CAST(P.FederalLocaleCodeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetFederalLocaleCodeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[FederalLocaleCodeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[FederalLocaleCodeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[FederalLocaleCodeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[FederalLocaleCodeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganization]' AS DataTableName
        ,P.[tid_EducationOrganization]  AS Tid
        ,@Msg +  'OperationalStatusType = ' + COALESCE(CAST(P.OperationalStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[OperationalStatusType] F
        ON
        	P.[OperationalStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[OperationalStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationIdentificationCode]' AS DataTableName
        ,P.[tid_EducationOrganizationIdentificationCode]  AS Tid
        ,@Msg +  'EducationOrganizationIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.EducationOrganizationIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.EducationOrganizationIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationIdentificationCodeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationIdentificationSystemDescriptorBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationIdentificationSystemDescriptorCodeValue] = F.[CodeValue]
        	AND P.[EducationOrganizationIdentificationSystemDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[EducationOrganizationIdentificationSystemDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[EducationOrganizationIdentificationSystemDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationCategorySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationCategory]' AS DataTableName
        ,P.[tid_EducationOrganizationCategory]  AS Tid
        ,@Msg +  'EducationOrganizationCategoryType = ' + COALESCE(CAST(P.EducationOrganizationCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationCategoryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EducationOrganizationCategoryType] F
        ON
        	P.[EducationOrganizationCategoryType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EducationOrganizationCategoryType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationAddress]' AS DataTableName
        ,P.[tid_EducationOrganizationAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationAddress]' AS DataTableName
        ,P.[tid_EducationOrganizationAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[StateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[StateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationInternationalAddress]' AS DataTableName
        ,P.[tid_EducationOrganizationInternationalAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationInternationalAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationInternationalAddress]' AS DataTableName
        ,P.[tid_EducationOrganizationInternationalAddress]  AS Tid
        ,@Msg +  'CountryDescriptorCodeValue = ' + COALESCE(CAST(P.CountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CountryDescriptorNamespace = ' + COALESCE(CAST(P.CountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationInternationalAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationInstitutionTelephoneSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationInstitutionTelephone]' AS DataTableName
        ,P.[tid_EducationOrganizationInstitutionTelephone]  AS Tid
        ,@Msg +  'InstitutionTelephoneNumberType = ' + COALESCE(CAST(P.InstitutionTelephoneNumberType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationInstitutionTelephoneBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[InstitutionTelephoneNumberType] F
        ON
        	P.[InstitutionTelephoneNumberType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[InstitutionTelephoneNumberType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eEducationOrganizationInterventionPrescriptionAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eEducationOrganizationInterventionPrescriptionAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationInterventionPrescriptionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationInterventionPrescriptionAssociation]' AS DataTableName
        ,P.[tid_EducationOrganizationInterventionPrescriptionAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationInterventionPrescriptionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationInterventionPrescriptionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationInterventionPrescriptionAssociation]' AS DataTableName
        ,P.[tid_EducationOrganizationInterventionPrescriptionAssociation]  AS Tid
        ,@Msg +  'InterventionPrescriptionEducationOrganizationId = ' + COALESCE(CAST(P.InterventionPrescriptionEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'InterventionPrescriptionIdentificationCode = ' + COALESCE(CAST(P.InterventionPrescriptionIdentificationCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationInterventionPrescriptionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetInterventionPrescriptionBySnapshotId](@Ids) F
        ON
        	P.[InterventionPrescriptionEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[InterventionPrescriptionIdentificationCode] = F.[InterventionPrescriptionIdentificationCode]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[InterventionPrescriptionEducationOrganizationId] IS NOT NULL
        AND F.[InterventionPrescriptionIdentificationCode] IS NULL 
        AND P.[InterventionPrescriptionIdentificationCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eEducationOrganizationNetwork]******/

CREATE PROCEDURE [t_edfi].[usp_eEducationOrganizationNetwork]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationNetworkSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationNetwork]' AS DataTableName
        ,P.[tid_EducationOrganizationNetwork]  AS Tid
        ,@Msg +  'EducationOrganizationNetworkId = ' + COALESCE(CAST(P.EducationOrganizationNetworkId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationNetworkBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationNetworkId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationNetworkId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationNetworkSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationNetwork]' AS DataTableName
        ,P.[tid_EducationOrganizationNetwork]  AS Tid
        ,@Msg +  'NetworkPurposeType = ' + COALESCE(CAST(P.NetworkPurposeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationNetworkBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[NetworkPurposeType] F
        ON
        	P.[NetworkPurposeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[NetworkPurposeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eEducationOrganizationNetworkAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eEducationOrganizationNetworkAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationNetworkAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationNetworkAssociation]' AS DataTableName
        ,P.[tid_EducationOrganizationNetworkAssociation]  AS Tid
        ,@Msg +  'MemberEducationOrganizationId = ' + COALESCE(CAST(P.MemberEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationNetworkAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[MemberEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[MemberEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationNetworkAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationNetworkAssociation]' AS DataTableName
        ,P.[tid_EducationOrganizationNetworkAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationNetworkId = ' + COALESCE(CAST(P.EducationOrganizationNetworkId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationNetworkAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationNetworkBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationNetworkId] = F.[EducationOrganizationNetworkId]
        WHERE F.[EducationOrganizationNetworkId] IS NULL 
        AND P.[EducationOrganizationNetworkId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eEducationOrganizationPeerAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eEducationOrganizationPeerAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationPeerAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationPeerAssociation]' AS DataTableName
        ,P.[tid_EducationOrganizationPeerAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationPeerAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationOrganizationPeerAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationOrganizationPeerAssociation]' AS DataTableName
        ,P.[tid_EducationOrganizationPeerAssociation]  AS Tid
        ,@Msg +  'PeerEducationOrganizationId = ' + COALESCE(CAST(P.PeerEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationOrganizationPeerAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[PeerEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[PeerEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eEducationServiceCenter]******/

CREATE PROCEDURE [t_edfi].[usp_eEducationServiceCenter]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationServiceCenterSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationServiceCenter]' AS DataTableName
        ,P.[tid_EducationServiceCenter]  AS Tid
        ,@Msg +  'EducationServiceCenterId = ' + COALESCE(CAST(P.EducationServiceCenterId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationServiceCenterBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationServiceCenterId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationServiceCenterId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[EducationServiceCenterSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[EducationServiceCenter]' AS DataTableName
        ,P.[tid_EducationServiceCenter]  AS Tid
        ,@Msg +  'StateEducationAgencyId = ' + COALESCE(CAST(P.StateEducationAgencyId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetEducationServiceCenterBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStateEducationAgencyBySnapshotId](@Ids) F
        ON
        	P.[StateEducationAgencyId] = F.[StateEducationAgencyId]
        WHERE F.[StateEducationAgencyId] IS NULL 
        AND P.[StateEducationAgencyId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eFeederSchoolAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eFeederSchoolAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[FeederSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[FeederSchoolAssociation]' AS DataTableName
        ,P.[tid_FeederSchoolAssociation]  AS Tid
        ,@Msg +  'FeederSchoolId = ' + COALESCE(CAST(P.FeederSchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetFeederSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[FeederSchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[FeederSchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[FeederSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[FeederSchoolAssociation]' AS DataTableName
        ,P.[tid_FeederSchoolAssociation]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetFeederSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eGrade]******/

CREATE PROCEDURE [t_edfi].[usp_eGrade]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Grade]' AS DataTableName
        ,P.[tid_Grade]  AS Tid
        ,@Msg +  'GradeType = ' + COALESCE(CAST(P.GradeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[GradeType] F
        ON
        	P.[GradeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[GradeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Grade]' AS DataTableName
        ,P.[tid_Grade]  AS Tid
        ,@Msg +  'GradingPeriodBeginDate = ' + COALESCE(CAST(P.GradingPeriodBeginDate AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.GradingPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorNamespace = ' + COALESCE(CAST(P.GradingPeriodDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradingPeriodBySnapshotId](@Ids) F
        ON
        	P.[GradingPeriodBeginDate] = F.[BeginDate]
        	AND P.[GradingPeriodDescriptorCodeValue] = F.[GradingPeriodDescriptorCodeValue]
        	AND P.[GradingPeriodDescriptorNamespace] = F.[GradingPeriodDescriptorNamespace]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[GradingPeriodBeginDate] IS NOT NULL
        AND F.[GradingPeriodDescriptorCodeValue] IS NULL 
        AND P.[GradingPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[GradingPeriodDescriptorNamespace] IS NULL 
        AND P.[GradingPeriodDescriptorNamespace] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Grade]' AS DataTableName
        ,P.[tid_Grade]  AS Tid
        ,@Msg +  'PerformanceBaseConversionType = ' + COALESCE(CAST(P.PerformanceBaseConversionType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PerformanceBaseConversionType] F
        ON
        	P.[PerformanceBaseConversionType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PerformanceBaseConversionType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Grade]' AS DataTableName
        ,P.[tid_Grade]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentSectionAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eGradebookEntry]******/

CREATE PROCEDURE [t_edfi].[usp_eGradebookEntry]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradebookEntrySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GradebookEntry]' AS DataTableName
        ,P.[tid_GradebookEntry]  AS Tid
        ,@Msg +  'GradebookEntryType = ' + COALESCE(CAST(P.GradebookEntryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradebookEntryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[GradebookEntryType] F
        ON
        	P.[GradebookEntryType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[GradebookEntryType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradebookEntrySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GradebookEntry]' AS DataTableName
        ,P.[tid_GradebookEntry]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.GradingPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorNamespace = ' + COALESCE(CAST(P.GradingPeriodDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradebookEntryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradingPeriodBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[GradingPeriodDescriptorCodeValue] = F.[GradingPeriodDescriptorCodeValue]
        	AND P.[GradingPeriodDescriptorNamespace] = F.[GradingPeriodDescriptorNamespace]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[GradingPeriodDescriptorCodeValue] IS NULL 
        AND P.[GradingPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[GradingPeriodDescriptorNamespace] IS NULL 
        AND P.[GradingPeriodDescriptorNamespace] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradebookEntrySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GradebookEntry]' AS DataTableName
        ,P.[tid_GradebookEntry]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradebookEntryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradebookEntryLearningStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GradebookEntryLearningStandard]' AS DataTableName
        ,P.[tid_GradebookEntryLearningStandard]  AS Tid
        ,@Msg +  'LearningStandardId = ' + COALESCE(CAST(P.LearningStandardId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradebookEntryLearningStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) F
        ON
        	P.[LearningStandardId] = F.[LearningStandardId]
        WHERE F.[LearningStandardId] IS NULL 
        AND P.[LearningStandardId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradebookEntryLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GradebookEntryLearningObjective]' AS DataTableName
        ,P.[tid_GradebookEntryLearningObjective]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'Objective = ' + COALESCE(CAST(P.Objective AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradebookEntryLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[Objective] = F.[Objective]
        	AND P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[ObjectiveGradeLevelDescriptorCodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[ObjectiveGradeLevelDescriptorNamespace]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[Objective] IS NULL 
        AND P.[Objective] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorNamespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[GradebookEntryExtensionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[GradebookEntryExtension]' AS DataTableName
        ,P.[tid_GradebookEntryExtension]  AS Tid
        ,@Msg +  'ProgramGatewayDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramGatewayDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramGatewayDescriptorNamespace = ' + COALESCE(CAST(P.ProgramGatewayDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetGradebookEntryExtensionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProgramGatewayDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ProgramGatewayDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ProgramGatewayDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ProgramGatewayDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ProgramGatewayDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eGradingPeriod]******/

CREATE PROCEDURE [t_edfi].[usp_eGradingPeriod]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradingPeriodSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GradingPeriod]' AS DataTableName
        ,P.[tid_GradingPeriod]  AS Tid
        ,@Msg +  'GradingPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.GradingPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorNamespace = ' + COALESCE(CAST(P.GradingPeriodDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradingPeriodBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradingPeriodDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradingPeriodDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradingPeriodDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradingPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradingPeriodDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GradingPeriodSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GradingPeriod]' AS DataTableName
        ,P.[tid_GradingPeriod]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGradingPeriodBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eGraduationPlan]******/

CREATE PROCEDURE [t_edfi].[usp_eGraduationPlan]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlan]' AS DataTableName
        ,P.[tid_GraduationPlan]  AS Tid
        ,@Msg +  'TotalRequiredCreditType = ' + COALESCE(CAST(P.TotalRequiredCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[TotalRequiredCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[TotalRequiredCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlan]' AS DataTableName
        ,P.[tid_GraduationPlan]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlan]' AS DataTableName
        ,P.[tid_GraduationPlan]  AS Tid
        ,@Msg +  'GraduationPlanTypeDescriptorCodeValue = ' + COALESCE(CAST(P.GraduationPlanTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GraduationPlanTypeDescriptorNamespace = ' + COALESCE(CAST(P.GraduationPlanTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGraduationPlanTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GraduationPlanTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GraduationPlanTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GraduationPlanTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GraduationPlanTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlan]' AS DataTableName
        ,P.[tid_GraduationPlan]  AS Tid
        ,@Msg +  'GraduationSchoolYear = ' + COALESCE(CAST(P.GraduationSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[GraduationSchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[GraduationSchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanCreditsBySubjectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanCreditsBySubject]' AS DataTableName
        ,P.[tid_GraduationPlanCreditsBySubject]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanCreditsBySubjectBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanCreditsBySubjectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanCreditsBySubject]' AS DataTableName
        ,P.[tid_GraduationPlanCreditsBySubject]  AS Tid
        ,@Msg +  'CreditType = ' + COALESCE(CAST(P.CreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanCreditsBySubjectBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[CreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanCreditsByCourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanCreditsByCourse]' AS DataTableName
        ,P.[tid_GraduationPlanCreditsByCourse]  AS Tid
        ,@Msg +  'CreditType = ' + COALESCE(CAST(P.CreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanCreditsByCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[CreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanCreditsByCourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanCreditsByCourse]' AS DataTableName
        ,P.[tid_GraduationPlanCreditsByCourse]  AS Tid
        ,@Msg +  'WhenTakenGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.WhenTakenGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'WhenTakenGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.WhenTakenGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanCreditsByCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[WhenTakenGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[WhenTakenGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[WhenTakenGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[WhenTakenGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanCreditsByCourseCourseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanCreditsByCourseCourse]' AS DataTableName
        ,P.[tid_GraduationPlanCreditsByCourseCourse]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'CourseEducationOrganizationId = ' + COALESCE(CAST(P.CourseEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanCreditsByCourseCourseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCourseBySnapshotId](@Ids) F
        ON
        	P.[CourseCode] = F.[CourseCode]
        	AND P.[CourseEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[CourseCode] IS NULL 
        AND P.[CourseCode] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[CourseEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanRequiredAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanRequiredAssessment]' AS DataTableName
        ,P.[tid_GraduationPlanRequiredAssessment]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessmentTitle = ' + COALESCE(CAST(P.AssessmentTitle AS NVARCHAR(128)), '') +  '|'+'Version = ' + COALESCE(CAST(P.Version AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanRequiredAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[AssessedGradeLevelDescriptorCodeValue] = F.[AssessedGradeLevelDescriptorCodeValue]
        	AND P.[AssessedGradeLevelDescriptorNamespace] = F.[AssessedGradeLevelDescriptorNamespace]
        	AND P.[AssessmentTitle] = F.[AssessmentTitle]
        	AND P.[Version] = F.[Version]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[AssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorNamespace] IS NULL 
        AND P.[AssessedGradeLevelDescriptorNamespace] IS NOT NULL
        AND F.[AssessmentTitle] IS NULL 
        AND P.[AssessmentTitle] IS NOT NULL
        AND F.[Version] IS NULL 
        AND P.[Version] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanRequiredAssessmentScoreSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanRequiredAssessmentScore]' AS DataTableName
        ,P.[tid_GraduationPlanRequiredAssessmentScore]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanRequiredAssessmentScoreBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentReportingMethodType] F
        ON
        	P.[AssessmentReportingMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentReportingMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanRequiredAssessmentScoreSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanRequiredAssessmentScore]' AS DataTableName
        ,P.[tid_GraduationPlanRequiredAssessmentScore]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanRequiredAssessmentScoreBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[ResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanRequiredAssessmentAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanRequiredAssessmentAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_GraduationPlanRequiredAssessmentAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanRequiredAssessmentAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentReportingMethodType] F
        ON
        	P.[AssessmentReportingMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentReportingMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanRequiredAssessmentAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanRequiredAssessmentAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_GraduationPlanRequiredAssessmentAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanRequiredAssessmentAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetPerformanceLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PerformanceLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PerformanceLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PerformanceLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PerformanceLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[GraduationPlanRequiredAssessmentAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[GraduationPlanRequiredAssessmentAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_GraduationPlanRequiredAssessmentAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetGraduationPlanRequiredAssessmentAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[ResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eIntervention]******/

CREATE PROCEDURE [t_edfi].[usp_eIntervention]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Intervention]' AS DataTableName
        ,P.[tid_Intervention]  AS Tid
        ,@Msg +  'DeliveryMethodType = ' + COALESCE(CAST(P.DeliveryMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[DeliveryMethodType] F
        ON
        	P.[DeliveryMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[DeliveryMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Intervention]' AS DataTableName
        ,P.[tid_Intervention]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Intervention]' AS DataTableName
        ,P.[tid_Intervention]  AS Tid
        ,@Msg +  'InterventionClassType = ' + COALESCE(CAST(P.InterventionClassType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[InterventionClassType] F
        ON
        	P.[InterventionClassType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[InterventionClassType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionEducationContentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionEducationContent]' AS DataTableName
        ,P.[tid_InterventionEducationContent]  AS Tid
        ,@Msg +  'ContentIdentifier = ' + COALESCE(CAST(P.ContentIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionEducationContentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationContentBySnapshotId](@Ids) F
        ON
        	P.[ContentIdentifier] = F.[ContentIdentifier]
        WHERE F.[ContentIdentifier] IS NULL 
        AND P.[ContentIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionDiagnosisSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionDiagnosis]' AS DataTableName
        ,P.[tid_InterventionDiagnosis]  AS Tid
        ,@Msg +  'DiagnosisDescriptorCodeValue = ' + COALESCE(CAST(P.DiagnosisDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DiagnosisDescriptorNamespace = ' + COALESCE(CAST(P.DiagnosisDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionDiagnosisBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetDiagnosisDescriptorBySnapshotId](@Ids) F
        ON
        	P.[DiagnosisDescriptorCodeValue] = F.[CodeValue]
        	AND P.[DiagnosisDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[DiagnosisDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[DiagnosisDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPopulationServedSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPopulationServed]' AS DataTableName
        ,P.[tid_InterventionPopulationServed]  AS Tid
        ,@Msg +  'PopulationServedType = ' + COALESCE(CAST(P.PopulationServedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPopulationServedBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PopulationServedType] F
        ON
        	P.[PopulationServedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PopulationServedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionAppropriateSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionAppropriateSex]' AS DataTableName
        ,P.[tid_InterventionAppropriateSex]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionAppropriateSexBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SexType] F
        ON
        	P.[SexType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SexType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionAppropriateGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionAppropriateGradeLevel]' AS DataTableName
        ,P.[tid_InterventionAppropriateGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionAppropriateGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionInterventionPrescriptionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionInterventionPrescription]' AS DataTableName
        ,P.[tid_InterventionInterventionPrescription]  AS Tid
        ,@Msg +  'InterventionPrescriptionEducationOrganizationId = ' + COALESCE(CAST(P.InterventionPrescriptionEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'InterventionPrescriptionIdentificationCode = ' + COALESCE(CAST(P.InterventionPrescriptionIdentificationCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionInterventionPrescriptionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetInterventionPrescriptionBySnapshotId](@Ids) F
        ON
        	P.[InterventionPrescriptionEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[InterventionPrescriptionIdentificationCode] = F.[InterventionPrescriptionIdentificationCode]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[InterventionPrescriptionEducationOrganizationId] IS NOT NULL
        AND F.[InterventionPrescriptionIdentificationCode] IS NULL 
        AND P.[InterventionPrescriptionIdentificationCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionMeetingTimeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionMeetingTime]' AS DataTableName
        ,P.[tid_InterventionMeetingTime]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionMeetingTimeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetClassPeriodBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStaffSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStaff]' AS DataTableName
        ,P.[tid_InterventionStaff]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStaffBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eInterventionPrescription]******/

CREATE PROCEDURE [t_edfi].[usp_eInterventionPrescription]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPrescriptionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPrescription]' AS DataTableName
        ,P.[tid_InterventionPrescription]  AS Tid
        ,@Msg +  'DeliveryMethodType = ' + COALESCE(CAST(P.DeliveryMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPrescriptionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[DeliveryMethodType] F
        ON
        	P.[DeliveryMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[DeliveryMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPrescriptionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPrescription]' AS DataTableName
        ,P.[tid_InterventionPrescription]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPrescriptionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPrescriptionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPrescription]' AS DataTableName
        ,P.[tid_InterventionPrescription]  AS Tid
        ,@Msg +  'InterventionClassType = ' + COALESCE(CAST(P.InterventionClassType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPrescriptionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[InterventionClassType] F
        ON
        	P.[InterventionClassType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[InterventionClassType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPrescriptionEducationContentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPrescriptionEducationContent]' AS DataTableName
        ,P.[tid_InterventionPrescriptionEducationContent]  AS Tid
        ,@Msg +  'ContentIdentifier = ' + COALESCE(CAST(P.ContentIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPrescriptionEducationContentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationContentBySnapshotId](@Ids) F
        ON
        	P.[ContentIdentifier] = F.[ContentIdentifier]
        WHERE F.[ContentIdentifier] IS NULL 
        AND P.[ContentIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPrescriptionDiagnosisSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPrescriptionDiagnosis]' AS DataTableName
        ,P.[tid_InterventionPrescriptionDiagnosis]  AS Tid
        ,@Msg +  'DiagnosisDescriptorCodeValue = ' + COALESCE(CAST(P.DiagnosisDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DiagnosisDescriptorNamespace = ' + COALESCE(CAST(P.DiagnosisDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPrescriptionDiagnosisBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetDiagnosisDescriptorBySnapshotId](@Ids) F
        ON
        	P.[DiagnosisDescriptorCodeValue] = F.[CodeValue]
        	AND P.[DiagnosisDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[DiagnosisDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[DiagnosisDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPrescriptionPopulationServedSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPrescriptionPopulationServed]' AS DataTableName
        ,P.[tid_InterventionPrescriptionPopulationServed]  AS Tid
        ,@Msg +  'PopulationServedType = ' + COALESCE(CAST(P.PopulationServedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPrescriptionPopulationServedBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PopulationServedType] F
        ON
        	P.[PopulationServedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PopulationServedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPrescriptionAppropriateSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPrescriptionAppropriateSex]' AS DataTableName
        ,P.[tid_InterventionPrescriptionAppropriateSex]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPrescriptionAppropriateSexBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SexType] F
        ON
        	P.[SexType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SexType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionPrescriptionAppropriateGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionPrescriptionAppropriateGradeLevel]' AS DataTableName
        ,P.[tid_InterventionPrescriptionAppropriateGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionPrescriptionAppropriateGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eInterventionStudy]******/

CREATE PROCEDURE [t_edfi].[usp_eInterventionStudy]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudy]' AS DataTableName
        ,P.[tid_InterventionStudy]  AS Tid
        ,@Msg +  'DeliveryMethodType = ' + COALESCE(CAST(P.DeliveryMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[DeliveryMethodType] F
        ON
        	P.[DeliveryMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[DeliveryMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudy]' AS DataTableName
        ,P.[tid_InterventionStudy]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudy]' AS DataTableName
        ,P.[tid_InterventionStudy]  AS Tid
        ,@Msg +  'InterventionClassType = ' + COALESCE(CAST(P.InterventionClassType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[InterventionClassType] F
        ON
        	P.[InterventionClassType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[InterventionClassType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudy]' AS DataTableName
        ,P.[tid_InterventionStudy]  AS Tid
        ,@Msg +  'InterventionPrescriptionEducationOrganizationId = ' + COALESCE(CAST(P.InterventionPrescriptionEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'InterventionPrescriptionIdentificationCode = ' + COALESCE(CAST(P.InterventionPrescriptionIdentificationCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetInterventionPrescriptionBySnapshotId](@Ids) F
        ON
        	P.[InterventionPrescriptionEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[InterventionPrescriptionIdentificationCode] = F.[InterventionPrescriptionIdentificationCode]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[InterventionPrescriptionEducationOrganizationId] IS NOT NULL
        AND F.[InterventionPrescriptionIdentificationCode] IS NULL 
        AND P.[InterventionPrescriptionIdentificationCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyEducationContentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyEducationContent]' AS DataTableName
        ,P.[tid_InterventionStudyEducationContent]  AS Tid
        ,@Msg +  'ContentIdentifier = ' + COALESCE(CAST(P.ContentIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyEducationContentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationContentBySnapshotId](@Ids) F
        ON
        	P.[ContentIdentifier] = F.[ContentIdentifier]
        WHERE F.[ContentIdentifier] IS NULL 
        AND P.[ContentIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyInterventionEffectivenessSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyInterventionEffectiveness]' AS DataTableName
        ,P.[tid_InterventionStudyInterventionEffectiveness]  AS Tid
        ,@Msg +  'DiagnosisDescriptorCodeValue = ' + COALESCE(CAST(P.DiagnosisDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DiagnosisDescriptorNamespace = ' + COALESCE(CAST(P.DiagnosisDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyInterventionEffectivenessBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetDiagnosisDescriptorBySnapshotId](@Ids) F
        ON
        	P.[DiagnosisDescriptorCodeValue] = F.[CodeValue]
        	AND P.[DiagnosisDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[DiagnosisDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[DiagnosisDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyInterventionEffectivenessSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyInterventionEffectiveness]' AS DataTableName
        ,P.[tid_InterventionStudyInterventionEffectiveness]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyInterventionEffectivenessBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyInterventionEffectivenessSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyInterventionEffectiveness]' AS DataTableName
        ,P.[tid_InterventionStudyInterventionEffectiveness]  AS Tid
        ,@Msg +  'InterventionEffectivenessRatingType = ' + COALESCE(CAST(P.InterventionEffectivenessRatingType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyInterventionEffectivenessBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[InterventionEffectivenessRatingType] F
        ON
        	P.[InterventionEffectivenessRatingType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[InterventionEffectivenessRatingType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyInterventionEffectivenessSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyInterventionEffectiveness]' AS DataTableName
        ,P.[tid_InterventionStudyInterventionEffectiveness]  AS Tid
        ,@Msg +  'PopulationServedType = ' + COALESCE(CAST(P.PopulationServedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyInterventionEffectivenessBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PopulationServedType] F
        ON
        	P.[PopulationServedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PopulationServedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyAppropriateGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyAppropriateGradeLevel]' AS DataTableName
        ,P.[tid_InterventionStudyAppropriateGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyAppropriateGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyPopulationServedSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyPopulationServed]' AS DataTableName
        ,P.[tid_InterventionStudyPopulationServed]  AS Tid
        ,@Msg +  'PopulationServedType = ' + COALESCE(CAST(P.PopulationServedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyPopulationServedBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PopulationServedType] F
        ON
        	P.[PopulationServedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PopulationServedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyAppropriateSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyAppropriateSex]' AS DataTableName
        ,P.[tid_InterventionStudyAppropriateSex]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyAppropriateSexBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SexType] F
        ON
        	P.[SexType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SexType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[InterventionStudyStateAbbreviationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[InterventionStudyStateAbbreviation]' AS DataTableName
        ,P.[tid_InterventionStudyStateAbbreviation]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetInterventionStudyStateAbbreviationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[StateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[StateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eLearningObjective]******/

CREATE PROCEDURE [t_edfi].[usp_eLearningObjective]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningObjective]' AS DataTableName
        ,P.[tid_LearningObjective]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningObjective]' AS DataTableName
        ,P.[tid_LearningObjective]  AS Tid
        ,@Msg +  'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningObjective]' AS DataTableName
        ,P.[tid_LearningObjective]  AS Tid
        ,@Msg +  'ParentAcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.ParentAcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ParentAcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.ParentAcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'ParentObjective = ' + COALESCE(CAST(P.ParentObjective AS NVARCHAR(128)), '') +  '|'+'ParentObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ParentObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ParentObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ParentObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) F
        ON
        	P.[ParentAcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[ParentAcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[ParentObjective] = F.[Objective]
        	AND P.[ParentObjectiveGradeLevelDescriptorCodeValue] = F.[ObjectiveGradeLevelDescriptorCodeValue]
        	AND P.[ParentObjectiveGradeLevelDescriptorNamespace] = F.[ObjectiveGradeLevelDescriptorNamespace]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[ParentAcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[ParentAcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[Objective] IS NULL 
        AND P.[ParentObjective] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[ParentObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorNamespace] IS NULL 
        AND P.[ParentObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningObjectiveContentStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningObjectiveContentStandard]' AS DataTableName
        ,P.[tid_LearningObjectiveContentStandard]  AS Tid
        ,@Msg +  'MandatingEducationOrganizationId = ' + COALESCE(CAST(P.MandatingEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningObjectiveContentStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[MandatingEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[MandatingEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningObjectiveContentStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningObjectiveContentStandard]' AS DataTableName
        ,P.[tid_LearningObjectiveContentStandard]  AS Tid
        ,@Msg +  'PublicationStatusType = ' + COALESCE(CAST(P.PublicationStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningObjectiveContentStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PublicationStatusType] F
        ON
        	P.[PublicationStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PublicationStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningObjectiveLearningStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningObjectiveLearningStandard]' AS DataTableName
        ,P.[tid_LearningObjectiveLearningStandard]  AS Tid
        ,@Msg +  'LearningStandardId = ' + COALESCE(CAST(P.LearningStandardId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningObjectiveLearningStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) F
        ON
        	P.[LearningStandardId] = F.[LearningStandardId]
        WHERE F.[LearningStandardId] IS NULL 
        AND P.[LearningStandardId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eLearningStandard]******/

CREATE PROCEDURE [t_edfi].[usp_eLearningStandard]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningStandard]' AS DataTableName
        ,P.[tid_LearningStandard]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningStandard]' AS DataTableName
        ,P.[tid_LearningStandard]  AS Tid
        ,@Msg +  'ParentLearningStandardId = ' + COALESCE(CAST(P.ParentLearningStandardId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) F
        ON
        	P.[ParentLearningStandardId] = F.[LearningStandardId]
        WHERE F.[LearningStandardId] IS NULL 
        AND P.[ParentLearningStandardId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningStandardContentStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningStandardContentStandard]' AS DataTableName
        ,P.[tid_LearningStandardContentStandard]  AS Tid
        ,@Msg +  'MandatingEducationOrganizationId = ' + COALESCE(CAST(P.MandatingEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningStandardContentStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[MandatingEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[MandatingEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningStandardContentStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningStandardContentStandard]' AS DataTableName
        ,P.[tid_LearningStandardContentStandard]  AS Tid
        ,@Msg +  'PublicationStatusType = ' + COALESCE(CAST(P.PublicationStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningStandardContentStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PublicationStatusType] F
        ON
        	P.[PublicationStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PublicationStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LearningStandardGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LearningStandardGradeLevel]' AS DataTableName
        ,P.[tid_LearningStandardGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLearningStandardGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eLeaveEvent]******/

CREATE PROCEDURE [t_edfi].[usp_eLeaveEvent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LeaveEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LeaveEvent]' AS DataTableName
        ,P.[tid_LeaveEvent]  AS Tid
        ,@Msg +  'LeaveEventCategoryType = ' + COALESCE(CAST(P.LeaveEventCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLeaveEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[LeaveEventCategoryType] F
        ON
        	P.[LeaveEventCategoryType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[LeaveEventCategoryType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LeaveEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LeaveEvent]' AS DataTableName
        ,P.[tid_LeaveEvent]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLeaveEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eLocalEducationAgency]******/

CREATE PROCEDURE [t_edfi].[usp_eLocalEducationAgency]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgency]' AS DataTableName
        ,P.[tid_LocalEducationAgency]  AS Tid
        ,@Msg +  'CharterStatusType = ' + COALESCE(CAST(P.CharterStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CharterStatusType] F
        ON
        	P.[CharterStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CharterStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgency]' AS DataTableName
        ,P.[tid_LocalEducationAgency]  AS Tid
        ,@Msg +  'LocalEducationAgencyId = ' + COALESCE(CAST(P.LocalEducationAgencyId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[LocalEducationAgencyId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[LocalEducationAgencyId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgency]' AS DataTableName
        ,P.[tid_LocalEducationAgency]  AS Tid
        ,@Msg +  'EducationServiceCenterId = ' + COALESCE(CAST(P.EducationServiceCenterId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationServiceCenterBySnapshotId](@Ids) F
        ON
        	P.[EducationServiceCenterId] = F.[EducationServiceCenterId]
        WHERE F.[EducationServiceCenterId] IS NULL 
        AND P.[EducationServiceCenterId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgency]' AS DataTableName
        ,P.[tid_LocalEducationAgency]  AS Tid
        ,@Msg +  'ParentLocalEducationAgencyId = ' + COALESCE(CAST(P.ParentLocalEducationAgencyId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLocalEducationAgencyBySnapshotId](@Ids) F
        ON
        	P.[ParentLocalEducationAgencyId] = F.[LocalEducationAgencyId]
        WHERE F.[LocalEducationAgencyId] IS NULL 
        AND P.[ParentLocalEducationAgencyId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgency]' AS DataTableName
        ,P.[tid_LocalEducationAgency]  AS Tid
        ,@Msg +  'LocalEducationAgencyCategoryType = ' + COALESCE(CAST(P.LocalEducationAgencyCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[LocalEducationAgencyCategoryType] F
        ON
        	P.[LocalEducationAgencyCategoryType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[LocalEducationAgencyCategoryType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgency]' AS DataTableName
        ,P.[tid_LocalEducationAgency]  AS Tid
        ,@Msg +  'StateEducationAgencyId = ' + COALESCE(CAST(P.StateEducationAgencyId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStateEducationAgencyBySnapshotId](@Ids) F
        ON
        	P.[StateEducationAgencyId] = F.[StateEducationAgencyId]
        WHERE F.[StateEducationAgencyId] IS NULL 
        AND P.[StateEducationAgencyId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencyAccountabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgencyAccountability]' AS DataTableName
        ,P.[tid_LocalEducationAgencyAccountability]  AS Tid
        ,@Msg +  'GunFreeSchoolsActReportingStatusType = ' + COALESCE(CAST(P.GunFreeSchoolsActReportingStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyAccountabilityBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[GunFreeSchoolsActReportingStatusType] F
        ON
        	P.[GunFreeSchoolsActReportingStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[GunFreeSchoolsActReportingStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencyAccountabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgencyAccountability]' AS DataTableName
        ,P.[tid_LocalEducationAgencyAccountability]  AS Tid
        ,@Msg +  'SchoolChoiceImplementStatusType = ' + COALESCE(CAST(P.SchoolChoiceImplementStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyAccountabilityBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SchoolChoiceImplementStatusType] F
        ON
        	P.[SchoolChoiceImplementStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SchoolChoiceImplementStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocalEducationAgencyAccountabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[LocalEducationAgencyAccountability]' AS DataTableName
        ,P.[tid_LocalEducationAgencyAccountability]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocalEducationAgencyAccountabilityBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eLocation]******/

CREATE PROCEDURE [t_edfi].[usp_eLocation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[LocationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Location]' AS DataTableName
        ,P.[tid_Location]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetLocationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eObjectiveAssessment]******/

CREATE PROCEDURE [t_edfi].[usp_eObjectiveAssessment]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ObjectiveAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ObjectiveAssessment]' AS DataTableName
        ,P.[tid_ObjectiveAssessment]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessmentTitle = ' + COALESCE(CAST(P.AssessmentTitle AS NVARCHAR(128)), '') +  '|'+'Version = ' + COALESCE(CAST(P.Version AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetObjectiveAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[AssessedGradeLevelDescriptorCodeValue] = F.[AssessedGradeLevelDescriptorCodeValue]
        	AND P.[AssessedGradeLevelDescriptorNamespace] = F.[AssessedGradeLevelDescriptorNamespace]
        	AND P.[AssessmentTitle] = F.[AssessmentTitle]
        	AND P.[Version] = F.[Version]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[AssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorNamespace] IS NULL 
        AND P.[AssessedGradeLevelDescriptorNamespace] IS NOT NULL
        AND F.[AssessmentTitle] IS NULL 
        AND P.[AssessmentTitle] IS NOT NULL
        AND F.[Version] IS NULL 
        AND P.[Version] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ObjectiveAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ObjectiveAssessment]' AS DataTableName
        ,P.[tid_ObjectiveAssessment]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessmentTitle = ' + COALESCE(CAST(P.AssessmentTitle AS NVARCHAR(128)), '') +  '|'+'ParentIdentificationCode = ' + COALESCE(CAST(P.ParentIdentificationCode AS NVARCHAR(128)), '') +  '|'+'Version = ' + COALESCE(CAST(P.Version AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetObjectiveAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetObjectiveAssessmentBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[AssessedGradeLevelDescriptorCodeValue] = F.[AssessedGradeLevelDescriptorCodeValue]
        	AND P.[AssessedGradeLevelDescriptorNamespace] = F.[AssessedGradeLevelDescriptorNamespace]
        	AND P.[AssessmentTitle] = F.[AssessmentTitle]
        	AND P.[ParentIdentificationCode] = F.[IdentificationCode]
        	AND P.[Version] = F.[Version]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[AssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorNamespace] IS NULL 
        AND P.[AssessedGradeLevelDescriptorNamespace] IS NOT NULL
        AND F.[AssessmentTitle] IS NULL 
        AND P.[AssessmentTitle] IS NOT NULL
        AND F.[IdentificationCode] IS NULL 
        AND P.[ParentIdentificationCode] IS NOT NULL
        AND F.[Version] IS NULL 
        AND P.[Version] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ObjectiveAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ObjectiveAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_ObjectiveAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetObjectiveAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentReportingMethodType] F
        ON
        	P.[AssessmentReportingMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentReportingMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ObjectiveAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ObjectiveAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_ObjectiveAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetObjectiveAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetPerformanceLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PerformanceLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PerformanceLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PerformanceLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PerformanceLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ObjectiveAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ObjectiveAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_ObjectiveAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetObjectiveAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[ResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ObjectiveAssessmentAssessmentItemSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ObjectiveAssessmentAssessmentItem]' AS DataTableName
        ,P.[tid_ObjectiveAssessmentAssessmentItem]  AS Tid
        ,@Msg +  'AssessmentItemIdentificationCode = ' + COALESCE(CAST(P.AssessmentItemIdentificationCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetObjectiveAssessmentAssessmentItemBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentItemBySnapshotId](@Ids) F
        ON
        	P.[AssessmentItemIdentificationCode] = F.[IdentificationCode]
        WHERE F.[IdentificationCode] IS NULL 
        AND P.[AssessmentItemIdentificationCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ObjectiveAssessmentLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ObjectiveAssessmentLearningObjective]' AS DataTableName
        ,P.[tid_ObjectiveAssessmentLearningObjective]  AS Tid
        ,@Msg +  'Objective = ' + COALESCE(CAST(P.Objective AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetObjectiveAssessmentLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) F
        ON
        	P.[Objective] = F.[Objective]
        WHERE F.[Objective] IS NULL 
        AND P.[Objective] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ObjectiveAssessmentLearningStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ObjectiveAssessmentLearningStandard]' AS DataTableName
        ,P.[tid_ObjectiveAssessmentLearningStandard]  AS Tid
        ,@Msg +  'LearningStandardId = ' + COALESCE(CAST(P.LearningStandardId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetObjectiveAssessmentLearningStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) F
        ON
        	P.[LearningStandardId] = F.[LearningStandardId]
        WHERE F.[LearningStandardId] IS NULL 
        AND P.[LearningStandardId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eOpenStaffPosition]******/

CREATE PROCEDURE [t_edfi].[usp_eOpenStaffPosition]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[OpenStaffPositionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[OpenStaffPosition]' AS DataTableName
        ,P.[tid_OpenStaffPosition]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetOpenStaffPositionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[OpenStaffPositionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[OpenStaffPosition]' AS DataTableName
        ,P.[tid_OpenStaffPosition]  AS Tid
        ,@Msg +  'EmploymentStatusDescriptorCodeValue = ' + COALESCE(CAST(P.EmploymentStatusDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EmploymentStatusDescriptorNamespace = ' + COALESCE(CAST(P.EmploymentStatusDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetOpenStaffPositionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEmploymentStatusDescriptorBySnapshotId](@Ids) F
        ON
        	P.[EmploymentStatusDescriptorCodeValue] = F.[CodeValue]
        	AND P.[EmploymentStatusDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[EmploymentStatusDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[EmploymentStatusDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[OpenStaffPositionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[OpenStaffPosition]' AS DataTableName
        ,P.[tid_OpenStaffPosition]  AS Tid
        ,@Msg +  'PostingResultType = ' + COALESCE(CAST(P.PostingResultType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetOpenStaffPositionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PostingResultType] F
        ON
        	P.[PostingResultType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PostingResultType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[OpenStaffPositionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[OpenStaffPosition]' AS DataTableName
        ,P.[tid_OpenStaffPosition]  AS Tid
        ,@Msg +  'ProgramAssignmentDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramAssignmentDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramAssignmentDescriptorNamespace = ' + COALESCE(CAST(P.ProgramAssignmentDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetOpenStaffPositionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramAssignmentDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ProgramAssignmentDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ProgramAssignmentDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ProgramAssignmentDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ProgramAssignmentDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[OpenStaffPositionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[OpenStaffPosition]' AS DataTableName
        ,P.[tid_OpenStaffPosition]  AS Tid
        ,@Msg +  'StaffClassificationDescriptorCodeValue = ' + COALESCE(CAST(P.StaffClassificationDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StaffClassificationDescriptorNamespace = ' + COALESCE(CAST(P.StaffClassificationDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetOpenStaffPositionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffClassificationDescriptorBySnapshotId](@Ids) F
        ON
        	P.[StaffClassificationDescriptorCodeValue] = F.[CodeValue]
        	AND P.[StaffClassificationDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[StaffClassificationDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[StaffClassificationDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[OpenStaffPositionInstructionalGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[OpenStaffPositionInstructionalGradeLevel]' AS DataTableName
        ,P.[tid_OpenStaffPositionInstructionalGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetOpenStaffPositionInstructionalGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[OpenStaffPositionAcademicSubjectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[OpenStaffPositionAcademicSubject]' AS DataTableName
        ,P.[tid_OpenStaffPositionAcademicSubject]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetOpenStaffPositionAcademicSubjectBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eParent]******/

CREATE PROCEDURE [t_edfi].[usp_eParent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Parent]' AS DataTableName
        ,P.[tid_Parent]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SexType] F
        ON
        	P.[SexType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SexType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentOtherNameSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentOtherName]' AS DataTableName
        ,P.[tid_ParentOtherName]  AS Tid
        ,@Msg +  'OtherNameType = ' + COALESCE(CAST(P.OtherNameType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentOtherNameBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[OtherNameType] F
        ON
        	P.[OtherNameType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[OtherNameType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentAddress]' AS DataTableName
        ,P.[tid_ParentAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentAddress]' AS DataTableName
        ,P.[tid_ParentAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[StateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[StateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentInternationalAddress]' AS DataTableName
        ,P.[tid_ParentInternationalAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentInternationalAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentInternationalAddress]' AS DataTableName
        ,P.[tid_ParentInternationalAddress]  AS Tid
        ,@Msg +  'CountryDescriptorCodeValue = ' + COALESCE(CAST(P.CountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CountryDescriptorNamespace = ' + COALESCE(CAST(P.CountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentInternationalAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentTelephoneSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentTelephone]' AS DataTableName
        ,P.[tid_ParentTelephone]  AS Tid
        ,@Msg +  'TelephoneNumberType = ' + COALESCE(CAST(P.TelephoneNumberType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentTelephoneBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[TelephoneNumberType] F
        ON
        	P.[TelephoneNumberType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[TelephoneNumberType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentElectronicMailSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentElectronicMail]' AS DataTableName
        ,P.[tid_ParentElectronicMail]  AS Tid
        ,@Msg +  'ElectronicMailType = ' + COALESCE(CAST(P.ElectronicMailType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentElectronicMailBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ElectronicMailType] F
        ON
        	P.[ElectronicMailType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ElectronicMailType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentIdentificationDocument]' AS DataTableName
        ,P.[tid_ParentIdentificationDocument]  AS Tid
        ,@Msg +  'IssuerCountryDescriptorCodeValue = ' + COALESCE(CAST(P.IssuerCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'IssuerCountryDescriptorNamespace = ' + COALESCE(CAST(P.IssuerCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[IssuerCountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[IssuerCountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[IssuerCountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[IssuerCountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentIdentificationDocument]' AS DataTableName
        ,P.[tid_ParentIdentificationDocument]  AS Tid
        ,@Msg +  'IdentificationDocumentUseType = ' + COALESCE(CAST(P.IdentificationDocumentUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[IdentificationDocumentUseType] F
        ON
        	P.[IdentificationDocumentUseType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[IdentificationDocumentUseType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ParentIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ParentIdentificationDocument]' AS DataTableName
        ,P.[tid_ParentIdentificationDocument]  AS Tid
        ,@Msg +  'PersonalInformationVerificationType = ' + COALESCE(CAST(P.PersonalInformationVerificationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetParentIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PersonalInformationVerificationType] F
        ON
        	P.[PersonalInformationVerificationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PersonalInformationVerificationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_ePayroll]******/

CREATE PROCEDURE [t_edfi].[usp_ePayroll]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[PayrollSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Payroll]' AS DataTableName
        ,P.[tid_Payroll]  AS Tid
        ,@Msg +  'AccountNumber = ' + COALESCE(CAST(P.AccountNumber AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FiscalYear = ' + COALESCE(CAST(P.FiscalYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetPayrollBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAccountBySnapshotId](@Ids) F
        ON
        	P.[AccountNumber] = F.[AccountNumber]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FiscalYear] = F.[FiscalYear]
        WHERE F.[AccountNumber] IS NULL 
        AND P.[AccountNumber] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FiscalYear] IS NULL 
        AND P.[FiscalYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[PayrollSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Payroll]' AS DataTableName
        ,P.[tid_Payroll]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetPayrollBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_ePostSecondaryEvent]******/

CREATE PROCEDURE [t_edfi].[usp_ePostSecondaryEvent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[PostSecondaryEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[PostSecondaryEvent]' AS DataTableName
        ,P.[tid_PostSecondaryEvent]  AS Tid
        ,@Msg +  'PostSecondaryEventCategoryType = ' + COALESCE(CAST(P.PostSecondaryEventCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetPostSecondaryEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PostSecondaryEventCategoryType] F
        ON
        	P.[PostSecondaryEventCategoryType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PostSecondaryEventCategoryType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[PostSecondaryEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[PostSecondaryEvent]' AS DataTableName
        ,P.[tid_PostSecondaryEvent]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetPostSecondaryEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[PostSecondaryEventPostSecondaryInstitutionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[PostSecondaryEventPostSecondaryInstitution]' AS DataTableName
        ,P.[tid_PostSecondaryEventPostSecondaryInstitution]  AS Tid
        ,@Msg +  'AdministrativeFundingControlDescriptorCodeValue = ' + COALESCE(CAST(P.AdministrativeFundingControlDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AdministrativeFundingControlDescriptorNamespace = ' + COALESCE(CAST(P.AdministrativeFundingControlDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetPostSecondaryEventPostSecondaryInstitutionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAdministrativeFundingControlDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AdministrativeFundingControlDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AdministrativeFundingControlDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AdministrativeFundingControlDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AdministrativeFundingControlDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[PostSecondaryEventPostSecondaryInstitutionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[PostSecondaryEventPostSecondaryInstitution]' AS DataTableName
        ,P.[tid_PostSecondaryEventPostSecondaryInstitution]  AS Tid
        ,@Msg +  'PostSecondaryInstitutionLevelType = ' + COALESCE(CAST(P.PostSecondaryInstitutionLevelType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetPostSecondaryEventPostSecondaryInstitutionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PostSecondaryInstitutionLevelType] F
        ON
        	P.[PostSecondaryInstitutionLevelType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PostSecondaryInstitutionLevelType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[PostSecondaryEventPostSecondaryInstitutionIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[PostSecondaryEventPostSecondaryInstitutionIdentificationCode]' AS DataTableName
        ,P.[tid_PostSecondaryEventPostSecondaryInstitutionIdentificationCode]  AS Tid
        ,@Msg +  'EducationOrganizationIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.EducationOrganizationIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.EducationOrganizationIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetPostSecondaryEventPostSecondaryInstitutionIdentificationCodeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationIdentificationSystemDescriptorBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationIdentificationSystemDescriptorCodeValue] = F.[CodeValue]
        	AND P.[EducationOrganizationIdentificationSystemDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[EducationOrganizationIdentificationSystemDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[EducationOrganizationIdentificationSystemDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[PostSecondaryEventPostSecondaryInstitutionMediumOfInstructionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[PostSecondaryEventPostSecondaryInstitutionMediumOfInstruction]' AS DataTableName
        ,P.[tid_PostSecondaryEventPostSecondaryInstitutionMediumOfInstruction]  AS Tid
        ,@Msg +  'MediumOfInstructionType = ' + COALESCE(CAST(P.MediumOfInstructionType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetPostSecondaryEventPostSecondaryInstitutionMediumOfInstructionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[MediumOfInstructionType] F
        ON
        	P.[MediumOfInstructionType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[MediumOfInstructionType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eProgram]******/

CREATE PROCEDURE [t_edfi].[usp_eProgram]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Program]' AS DataTableName
        ,P.[tid_Program]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Program]' AS DataTableName
        ,P.[tid_Program]  AS Tid
        ,@Msg +  'ProgramSponsorType = ' + COALESCE(CAST(P.ProgramSponsorType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ProgramSponsorType] F
        ON
        	P.[ProgramSponsorType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ProgramSponsorType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Program]' AS DataTableName
        ,P.[tid_Program]  AS Tid
        ,@Msg +  'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ProgramType] F
        ON
        	P.[ProgramType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ProgramCharacteristicSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ProgramCharacteristic]' AS DataTableName
        ,P.[tid_ProgramCharacteristic]  AS Tid
        ,@Msg +  'ProgramCharacteristicDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramCharacteristicDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramCharacteristicDescriptorNamespace = ' + COALESCE(CAST(P.ProgramCharacteristicDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetProgramCharacteristicBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramCharacteristicDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ProgramCharacteristicDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ProgramCharacteristicDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ProgramCharacteristicDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ProgramCharacteristicDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ProgramServiceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ProgramService]' AS DataTableName
        ,P.[tid_ProgramService]  AS Tid
        ,@Msg +  'ServiceDescriptorCodeValue = ' + COALESCE(CAST(P.ServiceDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ServiceDescriptorNamespace = ' + COALESCE(CAST(P.ServiceDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetProgramServiceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetServiceDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ServiceDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ServiceDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ServiceDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ServiceDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ProgramLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ProgramLearningObjective]' AS DataTableName
        ,P.[tid_ProgramLearningObjective]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'Objective = ' + COALESCE(CAST(P.Objective AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetProgramLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[Objective] = F.[Objective]
        	AND P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[ObjectiveGradeLevelDescriptorCodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[ObjectiveGradeLevelDescriptorNamespace]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[Objective] IS NULL 
        AND P.[Objective] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorNamespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ProgramLearningStandardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ProgramLearningStandard]' AS DataTableName
        ,P.[tid_ProgramLearningStandard]  AS Tid
        ,@Msg +  'LearningStandardId = ' + COALESCE(CAST(P.LearningStandardId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetProgramLearningStandardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningStandardBySnapshotId](@Ids) F
        ON
        	P.[LearningStandardId] = F.[LearningStandardId]
        WHERE F.[LearningStandardId] IS NULL 
        AND P.[LearningStandardId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eReportCard]******/

CREATE PROCEDURE [t_edfi].[usp_eReportCard]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ReportCardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ReportCard]' AS DataTableName
        ,P.[tid_ReportCard]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetReportCardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ReportCardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ReportCard]' AS DataTableName
        ,P.[tid_ReportCard]  AS Tid
        ,@Msg +  'GradingPeriodBeginDate = ' + COALESCE(CAST(P.GradingPeriodBeginDate AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.GradingPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorNamespace = ' + COALESCE(CAST(P.GradingPeriodDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetReportCardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradingPeriodBySnapshotId](@Ids) F
        ON
        	P.[GradingPeriodBeginDate] = F.[BeginDate]
        	AND P.[GradingPeriodDescriptorCodeValue] = F.[GradingPeriodDescriptorCodeValue]
        	AND P.[GradingPeriodDescriptorNamespace] = F.[GradingPeriodDescriptorNamespace]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[GradingPeriodBeginDate] IS NOT NULL
        AND F.[GradingPeriodDescriptorCodeValue] IS NULL 
        AND P.[GradingPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[GradingPeriodDescriptorNamespace] IS NULL 
        AND P.[GradingPeriodDescriptorNamespace] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ReportCardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ReportCard]' AS DataTableName
        ,P.[tid_ReportCard]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetReportCardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ReportCardGradeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ReportCardGrade]' AS DataTableName
        ,P.[tid_ReportCardGrade]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'GradeType = ' + COALESCE(CAST(P.GradeType AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetReportCardGradeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[GradeType] = F.[GradeType]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[GradeType] IS NULL 
        AND P.[GradeType] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ReportCardStudentCompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ReportCardStudentCompetencyObjective]' AS DataTableName
        ,P.[tid_ReportCardStudentCompetencyObjective]  AS Tid
        ,@Msg +  'Objective = ' + COALESCE(CAST(P.Objective AS NVARCHAR(128)), '') +  '|'+'ObjectiveEducationOrganizationId = ' + COALESCE(CAST(P.ObjectiveEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetReportCardStudentCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentCompetencyObjectiveBySnapshotId](@Ids) F
        ON
        	P.[Objective] = F.[Objective]
        	AND P.[ObjectiveEducationOrganizationId] = F.[ObjectiveEducationOrganizationId]
        	AND P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[ObjectiveGradeLevelDescriptorCodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[ObjectiveGradeLevelDescriptorNamespace]
        WHERE F.[Objective] IS NULL 
        AND P.[Objective] IS NOT NULL
        AND F.[ObjectiveEducationOrganizationId] IS NULL 
        AND P.[ObjectiveEducationOrganizationId] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorNamespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[ReportCardStudentLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[ReportCardStudentLearningObjective]' AS DataTableName
        ,P.[tid_ReportCardStudentLearningObjective]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'Objective = ' + COALESCE(CAST(P.Objective AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetReportCardStudentLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentLearningObjectiveBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[Objective] = F.[Objective]
        	AND P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[ObjectiveGradeLevelDescriptorCodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[ObjectiveGradeLevelDescriptorNamespace]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[Objective] IS NULL 
        AND P.[Objective] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorNamespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eRestraintEvent]******/

CREATE PROCEDURE [t_edfi].[usp_eRestraintEvent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[RestraintEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[RestraintEvent]' AS DataTableName
        ,P.[tid_RestraintEvent]  AS Tid
        ,@Msg +  'EducationalEnvironmentType = ' + COALESCE(CAST(P.EducationalEnvironmentType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetRestraintEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EducationalEnvironmentType] F
        ON
        	P.[EducationalEnvironmentType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EducationalEnvironmentType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[RestraintEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[RestraintEvent]' AS DataTableName
        ,P.[tid_RestraintEvent]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetRestraintEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[RestraintEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[RestraintEvent]' AS DataTableName
        ,P.[tid_RestraintEvent]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetRestraintEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[RestraintEventReasonSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[RestraintEventReason]' AS DataTableName
        ,P.[tid_RestraintEventReason]  AS Tid
        ,@Msg +  'RestraintEventReasonType = ' + COALESCE(CAST(P.RestraintEventReasonType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetRestraintEventReasonBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[RestraintEventReasonType] F
        ON
        	P.[RestraintEventReasonType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RestraintEventReasonType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[RestraintEventProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[RestraintEventProgram]' AS DataTableName
        ,P.[tid_RestraintEventProgram]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetRestraintEventProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eSchool]******/

CREATE PROCEDURE [t_edfi].[usp_eSchool]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'AdministrativeFundingControlDescriptorCodeValue = ' + COALESCE(CAST(P.AdministrativeFundingControlDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AdministrativeFundingControlDescriptorNamespace = ' + COALESCE(CAST(P.AdministrativeFundingControlDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAdministrativeFundingControlDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AdministrativeFundingControlDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AdministrativeFundingControlDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AdministrativeFundingControlDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AdministrativeFundingControlDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'CharterApprovalAgencyType = ' + COALESCE(CAST(P.CharterApprovalAgencyType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CharterApprovalAgencyType] F
        ON
        	P.[CharterApprovalAgencyType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CharterApprovalAgencyType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'CharterStatusType = ' + COALESCE(CAST(P.CharterStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CharterStatusType] F
        ON
        	P.[CharterStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CharterStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'InternetAccessType = ' + COALESCE(CAST(P.InternetAccessType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[InternetAccessType] F
        ON
        	P.[InternetAccessType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[InternetAccessType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'LocalEducationAgencyId = ' + COALESCE(CAST(P.LocalEducationAgencyId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLocalEducationAgencyBySnapshotId](@Ids) F
        ON
        	P.[LocalEducationAgencyId] = F.[LocalEducationAgencyId]
        WHERE F.[LocalEducationAgencyId] IS NULL 
        AND P.[LocalEducationAgencyId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'MagnetSpecialProgramEmphasisSchoolType = ' + COALESCE(CAST(P.MagnetSpecialProgramEmphasisSchoolType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[MagnetSpecialProgramEmphasisSchoolType] F
        ON
        	P.[MagnetSpecialProgramEmphasisSchoolType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[MagnetSpecialProgramEmphasisSchoolType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'SchoolType = ' + COALESCE(CAST(P.SchoolType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SchoolType] F
        ON
        	P.[SchoolType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SchoolType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'CharterApprovalSchoolYear = ' + COALESCE(CAST(P.CharterApprovalSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[CharterApprovalSchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[CharterApprovalSchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[School]' AS DataTableName
        ,P.[tid_School]  AS Tid
        ,@Msg +  'TitleIPartASchoolDesignationType = ' + COALESCE(CAST(P.TitleIPartASchoolDesignationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[TitleIPartASchoolDesignationType] F
        ON
        	P.[TitleIPartASchoolDesignationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[TitleIPartASchoolDesignationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SchoolGradeLevel]' AS DataTableName
        ,P.[tid_SchoolGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SchoolCategorySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SchoolCategory]' AS DataTableName
        ,P.[tid_SchoolCategory]  AS Tid
        ,@Msg +  'SchoolCategoryType = ' + COALESCE(CAST(P.SchoolCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSchoolCategoryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SchoolCategoryType] F
        ON
        	P.[SchoolCategoryType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SchoolCategoryType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SchoolExtensionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SchoolExtension]' AS DataTableName
        ,P.[tid_SchoolExtension]  AS Tid
        ,@Msg +  'SchoolStatusDescriptorCodeValue = ' + COALESCE(CAST(P.SchoolStatusDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SchoolStatusDescriptorNamespace = ' + COALESCE(CAST(P.SchoolStatusDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSchoolExtensionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSchoolStatusDescriptorBySnapshotId](@Ids) F
        ON
        	P.[SchoolStatusDescriptorCodeValue] = F.[CodeValue]
        	AND P.[SchoolStatusDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[SchoolStatusDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[SchoolStatusDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eSection]******/

CREATE PROCEDURE [t_edfi].[usp_eSection]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetClassPeriodBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCourseOfferingBySnapshotId](@Ids) F
        ON
        	P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        WHERE F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'AvailableCreditType = ' + COALESCE(CAST(P.AvailableCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[AvailableCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AvailableCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'EducationalEnvironmentType = ' + COALESCE(CAST(P.EducationalEnvironmentType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EducationalEnvironmentType] F
        ON
        	P.[EducationalEnvironmentType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EducationalEnvironmentType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'InstructionLanguageDescriptorCodeValue = ' + COALESCE(CAST(P.InstructionLanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'InstructionLanguageDescriptorNamespace = ' + COALESCE(CAST(P.InstructionLanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLanguageDescriptorBySnapshotId](@Ids) F
        ON
        	P.[InstructionLanguageDescriptorCodeValue] = F.[CodeValue]
        	AND P.[InstructionLanguageDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[InstructionLanguageDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[InstructionLanguageDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLocationBySnapshotId](@Ids) F
        ON
        	P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'MediumOfInstructionType = ' + COALESCE(CAST(P.MediumOfInstructionType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[MediumOfInstructionType] F
        ON
        	P.[MediumOfInstructionType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[MediumOfInstructionType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'PopulationServedType = ' + COALESCE(CAST(P.PopulationServedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PopulationServedType] F
        ON
        	P.[PopulationServedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PopulationServedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Section]' AS DataTableName
        ,P.[tid_Section]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionCharacteristicSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SectionCharacteristic]' AS DataTableName
        ,P.[tid_SectionCharacteristic]  AS Tid
        ,@Msg +  'SectionCharacteristicDescriptorCodeValue = ' + COALESCE(CAST(P.SectionCharacteristicDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SectionCharacteristicDescriptorNamespace = ' + COALESCE(CAST(P.SectionCharacteristicDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionCharacteristicBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSectionCharacteristicDescriptorBySnapshotId](@Ids) F
        ON
        	P.[SectionCharacteristicDescriptorCodeValue] = F.[CodeValue]
        	AND P.[SectionCharacteristicDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[SectionCharacteristicDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[SectionCharacteristicDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SectionProgram]' AS DataTableName
        ,P.[tid_SectionProgram]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eSectionAttendanceTakenEvent]******/

CREATE PROCEDURE [t_edfi].[usp_eSectionAttendanceTakenEvent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionAttendanceTakenEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SectionAttendanceTakenEvent]' AS DataTableName
        ,P.[tid_SectionAttendanceTakenEvent]  AS Tid
        ,@Msg +  'Date = ' + COALESCE(CAST(P.Date AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionAttendanceTakenEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCalendarDateBySnapshotId](@Ids) F
        ON
        	P.[Date] = F.[Date]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[Date] IS NULL 
        AND P.[Date] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionAttendanceTakenEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SectionAttendanceTakenEvent]' AS DataTableName
        ,P.[tid_SectionAttendanceTakenEvent]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionAttendanceTakenEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SectionAttendanceTakenEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SectionAttendanceTakenEvent]' AS DataTableName
        ,P.[tid_SectionAttendanceTakenEvent]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSectionAttendanceTakenEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eSession]******/

CREATE PROCEDURE [t_edfi].[usp_eSession]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SessionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Session]' AS DataTableName
        ,P.[tid_Session]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSessionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SessionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Session]' AS DataTableName
        ,P.[tid_Session]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSessionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SessionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Session]' AS DataTableName
        ,P.[tid_Session]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSessionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetTermDescriptorBySnapshotId](@Ids) F
        ON
        	P.[TermDescriptorCodeValue] = F.[CodeValue]
        	AND P.[TermDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SessionGradingPeriodSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SessionGradingPeriod]' AS DataTableName
        ,P.[tid_SessionGradingPeriod]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.GradingPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorNamespace = ' + COALESCE(CAST(P.GradingPeriodDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSessionGradingPeriodBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradingPeriodBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[GradingPeriodDescriptorCodeValue] = F.[GradingPeriodDescriptorCodeValue]
        	AND P.[GradingPeriodDescriptorNamespace] = F.[GradingPeriodDescriptorNamespace]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[GradingPeriodDescriptorCodeValue] IS NULL 
        AND P.[GradingPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[GradingPeriodDescriptorNamespace] IS NULL 
        AND P.[GradingPeriodDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[SessionAcademicWeekSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[SessionAcademicWeek]' AS DataTableName
        ,P.[tid_SessionAcademicWeek]  AS Tid
        ,@Msg +  'WeekIdentifier = ' + COALESCE(CAST(P.WeekIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetSessionAcademicWeekBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicWeekBySnapshotId](@Ids) F
        ON
        	P.[WeekIdentifier] = F.[WeekIdentifier]
        WHERE F.[WeekIdentifier] IS NULL 
        AND P.[WeekIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStaff]******/

CREATE PROCEDURE [t_edfi].[usp_eStaff]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Staff]' AS DataTableName
        ,P.[tid_Staff]  AS Tid
        ,@Msg +  'CitizenshipStatusType = ' + COALESCE(CAST(P.CitizenshipStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CitizenshipStatusType] F
        ON
        	P.[CitizenshipStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CitizenshipStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Staff]' AS DataTableName
        ,P.[tid_Staff]  AS Tid
        ,@Msg +  'HighestCompletedLevelOfEducationDescriptorCodeValue = ' + COALESCE(CAST(P.HighestCompletedLevelOfEducationDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'HighestCompletedLevelOfEducationDescriptorNamespace = ' + COALESCE(CAST(P.HighestCompletedLevelOfEducationDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLevelOfEducationDescriptorBySnapshotId](@Ids) F
        ON
        	P.[HighestCompletedLevelOfEducationDescriptorCodeValue] = F.[CodeValue]
        	AND P.[HighestCompletedLevelOfEducationDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[HighestCompletedLevelOfEducationDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[HighestCompletedLevelOfEducationDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Staff]' AS DataTableName
        ,P.[tid_Staff]  AS Tid
        ,@Msg +  'OldEthnicityType = ' + COALESCE(CAST(P.OldEthnicityType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[OldEthnicityType] F
        ON
        	P.[OldEthnicityType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[OldEthnicityType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Staff]' AS DataTableName
        ,P.[tid_Staff]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SexType] F
        ON
        	P.[SexType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SexType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffIdentificationCode]' AS DataTableName
        ,P.[tid_StaffIdentificationCode]  AS Tid
        ,@Msg +  'StaffIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.StaffIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StaffIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.StaffIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffIdentificationCodeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffIdentificationSystemDescriptorBySnapshotId](@Ids) F
        ON
        	P.[StaffIdentificationSystemDescriptorCodeValue] = F.[CodeValue]
        	AND P.[StaffIdentificationSystemDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[StaffIdentificationSystemDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[StaffIdentificationSystemDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffOtherNameSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffOtherName]' AS DataTableName
        ,P.[tid_StaffOtherName]  AS Tid
        ,@Msg +  'OtherNameType = ' + COALESCE(CAST(P.OtherNameType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffOtherNameBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[OtherNameType] F
        ON
        	P.[OtherNameType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[OtherNameType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffAddress]' AS DataTableName
        ,P.[tid_StaffAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffAddress]' AS DataTableName
        ,P.[tid_StaffAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[StateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[StateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffInternationalAddress]' AS DataTableName
        ,P.[tid_StaffInternationalAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffInternationalAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffInternationalAddress]' AS DataTableName
        ,P.[tid_StaffInternationalAddress]  AS Tid
        ,@Msg +  'CountryDescriptorCodeValue = ' + COALESCE(CAST(P.CountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CountryDescriptorNamespace = ' + COALESCE(CAST(P.CountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffInternationalAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffTelephoneSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffTelephone]' AS DataTableName
        ,P.[tid_StaffTelephone]  AS Tid
        ,@Msg +  'TelephoneNumberType = ' + COALESCE(CAST(P.TelephoneNumberType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffTelephoneBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[TelephoneNumberType] F
        ON
        	P.[TelephoneNumberType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[TelephoneNumberType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffElectronicMailSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffElectronicMail]' AS DataTableName
        ,P.[tid_StaffElectronicMail]  AS Tid
        ,@Msg +  'ElectronicMailType = ' + COALESCE(CAST(P.ElectronicMailType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffElectronicMailBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ElectronicMailType] F
        ON
        	P.[ElectronicMailType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ElectronicMailType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffRace]' AS DataTableName
        ,P.[tid_StaffRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffRaceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[RaceType] F
        ON
        	P.[RaceType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RaceType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffVisaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffVisa]' AS DataTableName
        ,P.[tid_StaffVisa]  AS Tid
        ,@Msg +  'VisaType = ' + COALESCE(CAST(P.VisaType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffVisaBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[VisaType] F
        ON
        	P.[VisaType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[VisaType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffLanguage]' AS DataTableName
        ,P.[tid_StaffLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffLanguageBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLanguageDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LanguageDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LanguageDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LanguageDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LanguageDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffLanguageUseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffLanguageUse]' AS DataTableName
        ,P.[tid_StaffLanguageUse]  AS Tid
        ,@Msg +  'LanguageUseType = ' + COALESCE(CAST(P.LanguageUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffLanguageUseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[LanguageUseType] F
        ON
        	P.[LanguageUseType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[LanguageUseType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffRecognitionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffRecognition]' AS DataTableName
        ,P.[tid_StaffRecognition]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffRecognitionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAchievementCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AchievementCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AchievementCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AchievementCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AchievementCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffRecognitionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffRecognition]' AS DataTableName
        ,P.[tid_StaffRecognition]  AS Tid
        ,@Msg +  'RecognitionType = ' + COALESCE(CAST(P.RecognitionType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffRecognitionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[RecognitionType] F
        ON
        	P.[RecognitionType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RecognitionType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffCredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffCredential]' AS DataTableName
        ,P.[tid_StaffCredential]  AS Tid
        ,@Msg +  'CredentialIdentifier = ' + COALESCE(CAST(P.CredentialIdentifier AS NVARCHAR(128)), '') +  '|'+'StateOfIssueStateAbbreviationType = ' + COALESCE(CAST(P.StateOfIssueStateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffCredentialBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCredentialBySnapshotId](@Ids) F
        ON
        	P.[CredentialIdentifier] = F.[CredentialIdentifier]
        	AND P.[StateOfIssueStateAbbreviationType] = F.[StateOfIssueStateAbbreviationType]
        WHERE F.[CredentialIdentifier] IS NULL 
        AND P.[CredentialIdentifier] IS NOT NULL
        AND F.[StateOfIssueStateAbbreviationType] IS NULL 
        AND P.[StateOfIssueStateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffIdentificationDocument]' AS DataTableName
        ,P.[tid_StaffIdentificationDocument]  AS Tid
        ,@Msg +  'IssuerCountryDescriptorCodeValue = ' + COALESCE(CAST(P.IssuerCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'IssuerCountryDescriptorNamespace = ' + COALESCE(CAST(P.IssuerCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[IssuerCountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[IssuerCountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[IssuerCountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[IssuerCountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffIdentificationDocument]' AS DataTableName
        ,P.[tid_StaffIdentificationDocument]  AS Tid
        ,@Msg +  'IdentificationDocumentUseType = ' + COALESCE(CAST(P.IdentificationDocumentUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[IdentificationDocumentUseType] F
        ON
        	P.[IdentificationDocumentUseType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[IdentificationDocumentUseType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffIdentificationDocument]' AS DataTableName
        ,P.[tid_StaffIdentificationDocument]  AS Tid
        ,@Msg +  'PersonalInformationVerificationType = ' + COALESCE(CAST(P.PersonalInformationVerificationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PersonalInformationVerificationType] F
        ON
        	P.[PersonalInformationVerificationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PersonalInformationVerificationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffExtensionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffExtension]' AS DataTableName
        ,P.[tid_StaffExtension]  AS Tid
        ,@Msg +  'GenderType = ' + COALESCE(CAST(P.GenderType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffExtensionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[GenderType] F
        ON
        	P.[GenderType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[GenderType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffBackgroundCheckSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffBackgroundCheck]' AS DataTableName
        ,P.[tid_StaffBackgroundCheck]  AS Tid
        ,@Msg +  'BackgroundCheckStatusDescriptorCodeValue = ' + COALESCE(CAST(P.BackgroundCheckStatusDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BackgroundCheckStatusDescriptorNamespace = ' + COALESCE(CAST(P.BackgroundCheckStatusDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffBackgroundCheckBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetBackgroundCheckStatusDescriptorBySnapshotId](@Ids) F
        ON
        	P.[BackgroundCheckStatusDescriptorCodeValue] = F.[CodeValue]
        	AND P.[BackgroundCheckStatusDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[BackgroundCheckStatusDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[BackgroundCheckStatusDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffBackgroundCheckSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffBackgroundCheck]' AS DataTableName
        ,P.[tid_StaffBackgroundCheck]  AS Tid
        ,@Msg +  'BackgroundCheckTypeDescriptorCodeValue = ' + COALESCE(CAST(P.BackgroundCheckTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BackgroundCheckTypeDescriptorNamespace = ' + COALESCE(CAST(P.BackgroundCheckTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffBackgroundCheckBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetBackgroundCheckTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[BackgroundCheckTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[BackgroundCheckTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[BackgroundCheckTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[BackgroundCheckTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffHighlyQualifiedAcademicSubjectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffHighlyQualifiedAcademicSubject]' AS DataTableName
        ,P.[tid_StaffHighlyQualifiedAcademicSubject]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffHighlyQualifiedAcademicSubjectBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffSenioritySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffSeniority]' AS DataTableName
        ,P.[tid_StaffSeniority]  AS Tid
        ,@Msg +  'CredentialFieldDescriptorCodeValue = ' + COALESCE(CAST(P.CredentialFieldDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CredentialFieldDescriptorNamespace = ' + COALESCE(CAST(P.CredentialFieldDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffSeniorityBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCredentialFieldDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CredentialFieldDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CredentialFieldDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CredentialFieldDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CredentialFieldDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffTeacherPreparationProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProgram]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProgram]  AS Tid
        ,@Msg +  'LevelOfDegreeAwardedDescriptorCodeValue = ' + COALESCE(CAST(P.LevelOfDegreeAwardedDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LevelOfDegreeAwardedDescriptorNamespace = ' + COALESCE(CAST(P.LevelOfDegreeAwardedDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetLevelOfDegreeAwardedDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LevelOfDegreeAwardedDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LevelOfDegreeAwardedDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LevelOfDegreeAwardedDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LevelOfDegreeAwardedDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffTeacherPreparationProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProgram]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProgram]  AS Tid
        ,@Msg +  'TeacherPreparationProgramTypeDescriptorCodeValue = ' + COALESCE(CAST(P.TeacherPreparationProgramTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TeacherPreparationProgramTypeDescriptorNamespace = ' + COALESCE(CAST(P.TeacherPreparationProgramTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherPreparationProgramTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[TeacherPreparationProgramTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[TeacherPreparationProgramTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[TeacherPreparationProgramTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[TeacherPreparationProgramTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffTeacherPreparationProgramAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProgramAddress]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProgramAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProgramAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffTeacherPreparationProgramAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProgramAddress]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProgramAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProgramAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[StateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[StateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffApplicantSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffApplicant]' AS DataTableName
        ,P.[tid_StaffApplicant]  AS Tid
        ,@Msg +  'ApplicantIdentifier = ' + COALESCE(CAST(P.ApplicantIdentifier AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffApplicantBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetApplicantBySnapshotId](@Ids) F
        ON
        	P.[ApplicantIdentifier] = F.[ApplicantIdentifier]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[ApplicantIdentifier] IS NULL 
        AND P.[ApplicantIdentifier] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffSalarySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffSalary]' AS DataTableName
        ,P.[tid_StaffSalary]  AS Tid
        ,@Msg +  'SalaryTypeDescriptorCodeValue = ' + COALESCE(CAST(P.SalaryTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SalaryTypeDescriptorNamespace = ' + COALESCE(CAST(P.SalaryTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffSalaryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSalaryTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[SalaryTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[SalaryTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[SalaryTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[SalaryTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStaffCohortAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStaffCohortAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffCohortAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffCohortAssociation]' AS DataTableName
        ,P.[tid_StaffCohortAssociation]  AS Tid
        ,@Msg +  'CohortIdentifier = ' + COALESCE(CAST(P.CohortIdentifier AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffCohortAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCohortBySnapshotId](@Ids) F
        ON
        	P.[CohortIdentifier] = F.[CohortIdentifier]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[CohortIdentifier] IS NULL 
        AND P.[CohortIdentifier] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffCohortAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffCohortAssociation]' AS DataTableName
        ,P.[tid_StaffCohortAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffCohortAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStaffEducationOrganizationAssignmentAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStaffEducationOrganizationAssignmentAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationAssignmentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationAssignmentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationAssignmentAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationAssignmentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationAssignmentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationAssignmentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationAssignmentAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationAssignmentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationAssignmentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationAssignmentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationAssignmentAssociation]  AS Tid
        ,@Msg +  'StaffClassificationDescriptorCodeValue = ' + COALESCE(CAST(P.StaffClassificationDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StaffClassificationDescriptorNamespace = ' + COALESCE(CAST(P.StaffClassificationDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationAssignmentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffClassificationDescriptorBySnapshotId](@Ids) F
        ON
        	P.[StaffClassificationDescriptorCodeValue] = F.[CodeValue]
        	AND P.[StaffClassificationDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[StaffClassificationDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[StaffClassificationDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationAssignmentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationAssignmentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationAssignmentAssociation]  AS Tid
        ,@Msg +  'EmploymentEducationOrganizationId = ' + COALESCE(CAST(P.EmploymentEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'EmploymentStatusDescriptorCodeValue = ' + COALESCE(CAST(P.EmploymentStatusDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EmploymentStatusDescriptorNamespace = ' + COALESCE(CAST(P.EmploymentStatusDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'EmploymentHireDate = ' + COALESCE(CAST(P.EmploymentHireDate AS NVARCHAR(128)), '') +  '|'+'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationAssignmentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffEducationOrganizationEmploymentAssociationBySnapshotId](@Ids) F
        ON
        	P.[EmploymentEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[EmploymentStatusDescriptorCodeValue] = F.[EmploymentStatusDescriptorCodeValue]
        	AND P.[EmploymentStatusDescriptorNamespace] = F.[EmploymentStatusDescriptorNamespace]
        	AND P.[EmploymentHireDate] = F.[HireDate]
        	AND P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EmploymentEducationOrganizationId] IS NOT NULL
        AND F.[EmploymentStatusDescriptorCodeValue] IS NULL 
        AND P.[EmploymentStatusDescriptorCodeValue] IS NOT NULL
        AND F.[EmploymentStatusDescriptorNamespace] IS NULL 
        AND P.[EmploymentStatusDescriptorNamespace] IS NOT NULL
        AND F.[HireDate] IS NULL 
        AND P.[EmploymentHireDate] IS NOT NULL
        AND F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStaffEducationOrganizationEmploymentAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStaffEducationOrganizationEmploymentAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationEmploymentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationEmploymentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationEmploymentAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationEmploymentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationEmploymentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationEmploymentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationEmploymentAssociation]  AS Tid
        ,@Msg +  'EmploymentStatusDescriptorCodeValue = ' + COALESCE(CAST(P.EmploymentStatusDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EmploymentStatusDescriptorNamespace = ' + COALESCE(CAST(P.EmploymentStatusDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationEmploymentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEmploymentStatusDescriptorBySnapshotId](@Ids) F
        ON
        	P.[EmploymentStatusDescriptorCodeValue] = F.[CodeValue]
        	AND P.[EmploymentStatusDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[EmploymentStatusDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[EmploymentStatusDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationEmploymentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationEmploymentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationEmploymentAssociation]  AS Tid
        ,@Msg +  'SeparationReasonDescriptorCodeValue = ' + COALESCE(CAST(P.SeparationReasonDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SeparationReasonDescriptorNamespace = ' + COALESCE(CAST(P.SeparationReasonDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationEmploymentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSeparationReasonDescriptorBySnapshotId](@Ids) F
        ON
        	P.[SeparationReasonDescriptorCodeValue] = F.[CodeValue]
        	AND P.[SeparationReasonDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[SeparationReasonDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[SeparationReasonDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationEmploymentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationEmploymentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationEmploymentAssociation]  AS Tid
        ,@Msg +  'SeparationType = ' + COALESCE(CAST(P.SeparationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationEmploymentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SeparationType] F
        ON
        	P.[SeparationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SeparationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffEducationOrganizationEmploymentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffEducationOrganizationEmploymentAssociation]' AS DataTableName
        ,P.[tid_StaffEducationOrganizationEmploymentAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffEducationOrganizationEmploymentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStaffProgramAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStaffProgramAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffProgramAssociation]' AS DataTableName
        ,P.[tid_StaffProgramAssociation]  AS Tid
        ,@Msg +  'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) F
        ON
        	P.[ProgramEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffProgramAssociation]' AS DataTableName
        ,P.[tid_StaffProgramAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStaffSchoolAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStaffSchoolAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSchoolAssociation]' AS DataTableName
        ,P.[tid_StaffSchoolAssociation]  AS Tid
        ,@Msg +  'ProgramAssignmentDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramAssignmentDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramAssignmentDescriptorNamespace = ' + COALESCE(CAST(P.ProgramAssignmentDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramAssignmentDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ProgramAssignmentDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ProgramAssignmentDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ProgramAssignmentDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ProgramAssignmentDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSchoolAssociation]' AS DataTableName
        ,P.[tid_StaffSchoolAssociation]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSchoolAssociation]' AS DataTableName
        ,P.[tid_StaffSchoolAssociation]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSchoolAssociation]' AS DataTableName
        ,P.[tid_StaffSchoolAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSchoolAssociationGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSchoolAssociationGradeLevel]' AS DataTableName
        ,P.[tid_StaffSchoolAssociationGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSchoolAssociationGradeLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSchoolAssociationAcademicSubjectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSchoolAssociationAcademicSubject]' AS DataTableName
        ,P.[tid_StaffSchoolAssociationAcademicSubject]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSchoolAssociationAcademicSubjectBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStaffSectionAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStaffSectionAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSectionAssociation]' AS DataTableName
        ,P.[tid_StaffSectionAssociation]  AS Tid
        ,@Msg +  'ClassroomPositionDescriptorCodeValue = ' + COALESCE(CAST(P.ClassroomPositionDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ClassroomPositionDescriptorNamespace = ' + COALESCE(CAST(P.ClassroomPositionDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetClassroomPositionDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ClassroomPositionDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ClassroomPositionDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ClassroomPositionDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ClassroomPositionDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSectionAssociation]' AS DataTableName
        ,P.[tid_StaffSectionAssociation]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StaffSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StaffSectionAssociation]' AS DataTableName
        ,P.[tid_StaffSectionAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStaffSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStateEducationAgency]******/

CREATE PROCEDURE [t_edfi].[usp_eStateEducationAgency]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StateEducationAgencySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StateEducationAgency]' AS DataTableName
        ,P.[tid_StateEducationAgency]  AS Tid
        ,@Msg +  'StateEducationAgencyId = ' + COALESCE(CAST(P.StateEducationAgencyId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStateEducationAgencyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[StateEducationAgencyId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[StateEducationAgencyId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StateEducationAgencyAccountabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StateEducationAgencyAccountability]' AS DataTableName
        ,P.[tid_StateEducationAgencyAccountability]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStateEducationAgencyAccountabilityBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudent]******/

CREATE PROCEDURE [t_edfi].[usp_eStudent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Student]' AS DataTableName
        ,P.[tid_Student]  AS Tid
        ,@Msg +  'CitizenshipStatusType = ' + COALESCE(CAST(P.CitizenshipStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CitizenshipStatusType] F
        ON
        	P.[CitizenshipStatusType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CitizenshipStatusType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Student]' AS DataTableName
        ,P.[tid_Student]  AS Tid
        ,@Msg +  'BirthCountryDescriptorCodeValue = ' + COALESCE(CAST(P.BirthCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BirthCountryDescriptorNamespace = ' + COALESCE(CAST(P.BirthCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[BirthCountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[BirthCountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[BirthCountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[BirthCountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Student]' AS DataTableName
        ,P.[tid_Student]  AS Tid
        ,@Msg +  'LimitedEnglishProficiencyDescriptorCodeValue = ' + COALESCE(CAST(P.LimitedEnglishProficiencyDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LimitedEnglishProficiencyDescriptorNamespace = ' + COALESCE(CAST(P.LimitedEnglishProficiencyDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLimitedEnglishProficiencyDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LimitedEnglishProficiencyDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LimitedEnglishProficiencyDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LimitedEnglishProficiencyDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LimitedEnglishProficiencyDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Student]' AS DataTableName
        ,P.[tid_Student]  AS Tid
        ,@Msg +  'OldEthnicityType = ' + COALESCE(CAST(P.OldEthnicityType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[OldEthnicityType] F
        ON
        	P.[OldEthnicityType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[OldEthnicityType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Student]' AS DataTableName
        ,P.[tid_Student]  AS Tid
        ,@Msg +  'SchoolFoodServicesEligibilityDescriptorCodeValue = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SchoolFoodServicesEligibilityDescriptorNamespace = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolFoodServicesEligibilityDescriptorBySnapshotId](@Ids) F
        ON
        	P.[SchoolFoodServicesEligibilityDescriptorCodeValue] = F.[CodeValue]
        	AND P.[SchoolFoodServicesEligibilityDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[SchoolFoodServicesEligibilityDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[SchoolFoodServicesEligibilityDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Student]' AS DataTableName
        ,P.[tid_Student]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[SexType] F
        ON
        	P.[SexType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SexType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[Student]' AS DataTableName
        ,P.[tid_Student]  AS Tid
        ,@Msg +  'BirthStateAbbreviationType = ' + COALESCE(CAST(P.BirthStateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[BirthStateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[BirthStateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentIdentificationCode]' AS DataTableName
        ,P.[tid_StudentIdentificationCode]  AS Tid
        ,@Msg +  'StudentIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.StudentIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StudentIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.StudentIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentIdentificationCodeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentIdentificationSystemDescriptorBySnapshotId](@Ids) F
        ON
        	P.[StudentIdentificationSystemDescriptorCodeValue] = F.[CodeValue]
        	AND P.[StudentIdentificationSystemDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[StudentIdentificationSystemDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[StudentIdentificationSystemDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentOtherNameSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentOtherName]' AS DataTableName
        ,P.[tid_StudentOtherName]  AS Tid
        ,@Msg +  'OtherNameType = ' + COALESCE(CAST(P.OtherNameType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentOtherNameBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[OtherNameType] F
        ON
        	P.[OtherNameType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[OtherNameType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAddress]' AS DataTableName
        ,P.[tid_StudentAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAddress]' AS DataTableName
        ,P.[tid_StudentAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StateAbbreviationType] F
        ON
        	P.[StateAbbreviationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[StateAbbreviationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInternationalAddress]' AS DataTableName
        ,P.[tid_StudentInternationalAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInternationalAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AddressType] F
        ON
        	P.[AddressType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AddressType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInternationalAddress]' AS DataTableName
        ,P.[tid_StudentInternationalAddress]  AS Tid
        ,@Msg +  'CountryDescriptorCodeValue = ' + COALESCE(CAST(P.CountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CountryDescriptorNamespace = ' + COALESCE(CAST(P.CountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInternationalAddressBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentTelephoneSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentTelephone]' AS DataTableName
        ,P.[tid_StudentTelephone]  AS Tid
        ,@Msg +  'TelephoneNumberType = ' + COALESCE(CAST(P.TelephoneNumberType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentTelephoneBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[TelephoneNumberType] F
        ON
        	P.[TelephoneNumberType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[TelephoneNumberType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentElectronicMailSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentElectronicMail]' AS DataTableName
        ,P.[tid_StudentElectronicMail]  AS Tid
        ,@Msg +  'ElectronicMailType = ' + COALESCE(CAST(P.ElectronicMailType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentElectronicMailBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ElectronicMailType] F
        ON
        	P.[ElectronicMailType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ElectronicMailType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentRace]' AS DataTableName
        ,P.[tid_StudentRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentRaceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[RaceType] F
        ON
        	P.[RaceType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RaceType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentVisaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentVisa]' AS DataTableName
        ,P.[tid_StudentVisa]  AS Tid
        ,@Msg +  'VisaType = ' + COALESCE(CAST(P.VisaType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentVisaBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[VisaType] F
        ON
        	P.[VisaType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[VisaType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCharacteristicSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCharacteristic]' AS DataTableName
        ,P.[tid_StudentCharacteristic]  AS Tid
        ,@Msg +  'StudentCharacteristicDescriptorCodeValue = ' + COALESCE(CAST(P.StudentCharacteristicDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StudentCharacteristicDescriptorNamespace = ' + COALESCE(CAST(P.StudentCharacteristicDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCharacteristicBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentCharacteristicDescriptorBySnapshotId](@Ids) F
        ON
        	P.[StudentCharacteristicDescriptorCodeValue] = F.[CodeValue]
        	AND P.[StudentCharacteristicDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[StudentCharacteristicDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[StudentCharacteristicDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentLanguage]' AS DataTableName
        ,P.[tid_StudentLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentLanguageBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLanguageDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LanguageDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LanguageDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LanguageDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LanguageDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentLanguageUseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentLanguageUse]' AS DataTableName
        ,P.[tid_StudentLanguageUse]  AS Tid
        ,@Msg +  'LanguageUseType = ' + COALESCE(CAST(P.LanguageUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentLanguageUseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[LanguageUseType] F
        ON
        	P.[LanguageUseType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[LanguageUseType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentDisability]' AS DataTableName
        ,P.[tid_StudentDisability]  AS Tid
        ,@Msg +  'DisabilityDescriptorCodeValue = ' + COALESCE(CAST(P.DisabilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisabilityDescriptorNamespace = ' + COALESCE(CAST(P.DisabilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentDisabilityBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetDisabilityDescriptorBySnapshotId](@Ids) F
        ON
        	P.[DisabilityDescriptorCodeValue] = F.[CodeValue]
        	AND P.[DisabilityDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[DisabilityDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[DisabilityDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentDisability]' AS DataTableName
        ,P.[tid_StudentDisability]  AS Tid
        ,@Msg +  'DisabilityDeterminationSourceType = ' + COALESCE(CAST(P.DisabilityDeterminationSourceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentDisabilityBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[DisabilityDeterminationSourceType] F
        ON
        	P.[DisabilityDeterminationSourceType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[DisabilityDeterminationSourceType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramParticipationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramParticipation]' AS DataTableName
        ,P.[tid_StudentProgramParticipation]  AS Tid
        ,@Msg +  'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramParticipationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ProgramType] F
        ON
        	P.[ProgramType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramParticipationProgramCharacteristicSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramParticipationProgramCharacteristic]' AS DataTableName
        ,P.[tid_StudentProgramParticipationProgramCharacteristic]  AS Tid
        ,@Msg +  'ProgramCharacteristicDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramCharacteristicDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramCharacteristicDescriptorNamespace = ' + COALESCE(CAST(P.ProgramCharacteristicDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramParticipationProgramCharacteristicBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramCharacteristicDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ProgramCharacteristicDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ProgramCharacteristicDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ProgramCharacteristicDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ProgramCharacteristicDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCohortYearSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCohortYear]' AS DataTableName
        ,P.[tid_StudentCohortYear]  AS Tid
        ,@Msg +  'CohortYearType = ' + COALESCE(CAST(P.CohortYearType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCohortYearBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CohortYearType] F
        ON
        	P.[CohortYearType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CohortYearType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCohortYearSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCohortYear]' AS DataTableName
        ,P.[tid_StudentCohortYear]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCohortYearBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentIdentificationDocument]' AS DataTableName
        ,P.[tid_StudentIdentificationDocument]  AS Tid
        ,@Msg +  'IssuerCountryDescriptorCodeValue = ' + COALESCE(CAST(P.IssuerCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'IssuerCountryDescriptorNamespace = ' + COALESCE(CAST(P.IssuerCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCountryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[IssuerCountryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[IssuerCountryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[IssuerCountryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[IssuerCountryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentIdentificationDocument]' AS DataTableName
        ,P.[tid_StudentIdentificationDocument]  AS Tid
        ,@Msg +  'IdentificationDocumentUseType = ' + COALESCE(CAST(P.IdentificationDocumentUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[IdentificationDocumentUseType] F
        ON
        	P.[IdentificationDocumentUseType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[IdentificationDocumentUseType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentIdentificationDocument]' AS DataTableName
        ,P.[tid_StudentIdentificationDocument]  AS Tid
        ,@Msg +  'PersonalInformationVerificationType = ' + COALESCE(CAST(P.PersonalInformationVerificationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentIdentificationDocumentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PersonalInformationVerificationType] F
        ON
        	P.[PersonalInformationVerificationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PersonalInformationVerificationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentAcademicRecord]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentAcademicRecord]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecord]' AS DataTableName
        ,P.[tid_StudentAcademicRecord]  AS Tid
        ,@Msg +  'CumulativeEarnedCreditType = ' + COALESCE(CAST(P.CumulativeEarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[CumulativeEarnedCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CumulativeEarnedCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecord]' AS DataTableName
        ,P.[tid_StudentAcademicRecord]  AS Tid
        ,@Msg +  'CumulativeAttemptedCreditType = ' + COALESCE(CAST(P.CumulativeAttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[CumulativeAttemptedCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CumulativeAttemptedCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecord]' AS DataTableName
        ,P.[tid_StudentAcademicRecord]  AS Tid
        ,@Msg +  'SessionEarnedCreditType = ' + COALESCE(CAST(P.SessionEarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[SessionEarnedCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SessionEarnedCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecord]' AS DataTableName
        ,P.[tid_StudentAcademicRecord]  AS Tid
        ,@Msg +  'SessionAttemptedCreditType = ' + COALESCE(CAST(P.SessionAttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CreditType] F
        ON
        	P.[SessionAttemptedCreditType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[SessionAttemptedCreditType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecord]' AS DataTableName
        ,P.[tid_StudentAcademicRecord]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecord]' AS DataTableName
        ,P.[tid_StudentAcademicRecord]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecord]' AS DataTableName
        ,P.[tid_StudentAcademicRecord]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecord]' AS DataTableName
        ,P.[tid_StudentAcademicRecord]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetTermDescriptorBySnapshotId](@Ids) F
        ON
        	P.[TermDescriptorCodeValue] = F.[CodeValue]
        	AND P.[TermDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordAcademicHonorSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecordAcademicHonor]' AS DataTableName
        ,P.[tid_StudentAcademicRecordAcademicHonor]  AS Tid
        ,@Msg +  'AcademicHonorCategoryType = ' + COALESCE(CAST(P.AcademicHonorCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordAcademicHonorBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AcademicHonorCategoryType] F
        ON
        	P.[AcademicHonorCategoryType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AcademicHonorCategoryType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordAcademicHonorSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecordAcademicHonor]' AS DataTableName
        ,P.[tid_StudentAcademicRecordAcademicHonor]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordAcademicHonorBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAchievementCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AchievementCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AchievementCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AchievementCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AchievementCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordRecognitionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecordRecognition]' AS DataTableName
        ,P.[tid_StudentAcademicRecordRecognition]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordRecognitionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAchievementCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AchievementCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AchievementCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AchievementCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AchievementCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordRecognitionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecordRecognition]' AS DataTableName
        ,P.[tid_StudentAcademicRecordRecognition]  AS Tid
        ,@Msg +  'RecognitionType = ' + COALESCE(CAST(P.RecognitionType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordRecognitionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[RecognitionType] F
        ON
        	P.[RecognitionType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RecognitionType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_StudentAcademicRecordDiploma]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordDiplomaBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAchievementCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AchievementCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AchievementCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AchievementCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AchievementCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_StudentAcademicRecordDiploma]  AS Tid
        ,@Msg +  'DiplomaLevelType = ' + COALESCE(CAST(P.DiplomaLevelType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordDiplomaBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[DiplomaLevelType] F
        ON
        	P.[DiplomaLevelType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[DiplomaLevelType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_StudentAcademicRecordDiploma]  AS Tid
        ,@Msg +  'DiplomaType = ' + COALESCE(CAST(P.DiplomaType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordDiplomaBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[DiplomaType] F
        ON
        	P.[DiplomaType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[DiplomaType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAcademicRecordReportCardSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAcademicRecordReportCard]' AS DataTableName
        ,P.[tid_StudentAcademicRecordReportCard]  AS Tid
        ,@Msg +  'GradingPeriodBeginDate = ' + COALESCE(CAST(P.GradingPeriodBeginDate AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.GradingPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorNamespace = ' + COALESCE(CAST(P.GradingPeriodDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAcademicRecordReportCardBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetReportCardBySnapshotId](@Ids) F
        ON
        	P.[GradingPeriodBeginDate] = F.[GradingPeriodBeginDate]
        	AND P.[GradingPeriodDescriptorCodeValue] = F.[GradingPeriodDescriptorCodeValue]
        	AND P.[GradingPeriodDescriptorNamespace] = F.[GradingPeriodDescriptorNamespace]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[GradingPeriodBeginDate] IS NULL 
        AND P.[GradingPeriodBeginDate] IS NOT NULL
        AND F.[GradingPeriodDescriptorCodeValue] IS NULL 
        AND P.[GradingPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[GradingPeriodDescriptorNamespace] IS NULL 
        AND P.[GradingPeriodDescriptorNamespace] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentAssessment]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentAssessment]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessment]' AS DataTableName
        ,P.[tid_StudentAssessment]  AS Tid
        ,@Msg +  'AdministrationEnvironmentType = ' + COALESCE(CAST(P.AdministrationEnvironmentType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AdministrationEnvironmentType] F
        ON
        	P.[AdministrationEnvironmentType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AdministrationEnvironmentType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessment]' AS DataTableName
        ,P.[tid_StudentAssessment]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.AssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'AssessmentTitle = ' + COALESCE(CAST(P.AssessmentTitle AS NVARCHAR(128)), '') +  '|'+'Version = ' + COALESCE(CAST(P.Version AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[AssessedGradeLevelDescriptorCodeValue] = F.[AssessedGradeLevelDescriptorCodeValue]
        	AND P.[AssessedGradeLevelDescriptorNamespace] = F.[AssessedGradeLevelDescriptorNamespace]
        	AND P.[AssessmentTitle] = F.[AssessmentTitle]
        	AND P.[Version] = F.[Version]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[AssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[AssessedGradeLevelDescriptorNamespace] IS NULL 
        AND P.[AssessedGradeLevelDescriptorNamespace] IS NOT NULL
        AND F.[AssessmentTitle] IS NULL 
        AND P.[AssessmentTitle] IS NOT NULL
        AND F.[Version] IS NULL 
        AND P.[Version] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessment]' AS DataTableName
        ,P.[tid_StudentAssessment]  AS Tid
        ,@Msg +  'EventCircumstanceType = ' + COALESCE(CAST(P.EventCircumstanceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EventCircumstanceType] F
        ON
        	P.[EventCircumstanceType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EventCircumstanceType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessment]' AS DataTableName
        ,P.[tid_StudentAssessment]  AS Tid
        ,@Msg +  'WhenAssessedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.WhenAssessedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'WhenAssessedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.WhenAssessedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[WhenAssessedGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[WhenAssessedGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[WhenAssessedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[WhenAssessedGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessment]' AS DataTableName
        ,P.[tid_StudentAssessment]  AS Tid
        ,@Msg +  'AdministrationLanguageDescriptorCodeValue = ' + COALESCE(CAST(P.AdministrationLanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AdministrationLanguageDescriptorNamespace = ' + COALESCE(CAST(P.AdministrationLanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLanguageDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AdministrationLanguageDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AdministrationLanguageDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AdministrationLanguageDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AdministrationLanguageDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessment]' AS DataTableName
        ,P.[tid_StudentAssessment]  AS Tid
        ,@Msg +  'ReasonNotTestedType = ' + COALESCE(CAST(P.ReasonNotTestedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ReasonNotTestedType] F
        ON
        	P.[ReasonNotTestedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ReasonNotTestedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessment]' AS DataTableName
        ,P.[tid_StudentAssessment]  AS Tid
        ,@Msg +  'RetestIndicatorType = ' + COALESCE(CAST(P.RetestIndicatorType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[RetestIndicatorType] F
        ON
        	P.[RetestIndicatorType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RetestIndicatorType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessment]' AS DataTableName
        ,P.[tid_StudentAssessment]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentAccommodationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentAccommodation]' AS DataTableName
        ,P.[tid_StudentAssessmentAccommodation]  AS Tid
        ,@Msg +  'AccommodationDescriptorCodeValue = ' + COALESCE(CAST(P.AccommodationDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AccommodationDescriptorNamespace = ' + COALESCE(CAST(P.AccommodationDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentAccommodationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAccommodationDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AccommodationDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AccommodationDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AccommodationDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AccommodationDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentScoreResult]' AS DataTableName
        ,P.[tid_StudentAssessmentScoreResult]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentScoreResultBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentReportingMethodType] F
        ON
        	P.[AssessmentReportingMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentReportingMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentScoreResult]' AS DataTableName
        ,P.[tid_StudentAssessmentScoreResult]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentScoreResultBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[ResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_StudentAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetPerformanceLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PerformanceLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PerformanceLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PerformanceLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PerformanceLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentItemSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentItem]' AS DataTableName
        ,P.[tid_StudentAssessmentItem]  AS Tid
        ,@Msg +  'IdentificationCode = ' + COALESCE(CAST(P.IdentificationCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentItemBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAssessmentItemBySnapshotId](@Ids) F
        ON
        	P.[IdentificationCode] = F.[IdentificationCode]
        WHERE F.[IdentificationCode] IS NULL 
        AND P.[IdentificationCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentItemSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentItem]' AS DataTableName
        ,P.[tid_StudentAssessmentItem]  AS Tid
        ,@Msg +  'AssessmentItemResultType = ' + COALESCE(CAST(P.AssessmentItemResultType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentItemBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentItemResultType] F
        ON
        	P.[AssessmentItemResultType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentItemResultType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentItemSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentItem]' AS DataTableName
        ,P.[tid_StudentAssessmentItem]  AS Tid
        ,@Msg +  'ResponseIndicatorType = ' + COALESCE(CAST(P.ResponseIndicatorType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentItemBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResponseIndicatorType] F
        ON
        	P.[ResponseIndicatorType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ResponseIndicatorType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentStudentObjectiveAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentStudentObjectiveAssessment]' AS DataTableName
        ,P.[tid_StudentAssessmentStudentObjectiveAssessment]  AS Tid
        ,@Msg +  'IdentificationCode = ' + COALESCE(CAST(P.IdentificationCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentStudentObjectiveAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetObjectiveAssessmentBySnapshotId](@Ids) F
        ON
        	P.[IdentificationCode] = F.[IdentificationCode]
        WHERE F.[IdentificationCode] IS NULL 
        AND P.[IdentificationCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentStudentObjectiveAssessmentScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentStudentObjectiveAssessmentScoreResult]' AS DataTableName
        ,P.[tid_StudentAssessmentStudentObjectiveAssessmentScoreResult]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentStudentObjectiveAssessmentScoreResultBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[AssessmentReportingMethodType] F
        ON
        	P.[AssessmentReportingMethodType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AssessmentReportingMethodType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentStudentObjectiveAssessmentScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentStudentObjectiveAssessmentScoreResult]' AS DataTableName
        ,P.[tid_StudentAssessmentStudentObjectiveAssessmentScoreResult]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentStudentObjectiveAssessmentScoreResultBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[ResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[ResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentAssessmentStudentObjectiveAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentAssessmentStudentObjectiveAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_StudentAssessmentStudentObjectiveAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentAssessmentStudentObjectiveAssessmentPerformanceLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetPerformanceLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PerformanceLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PerformanceLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PerformanceLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PerformanceLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentCohortAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentCohortAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCohortAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCohortAssociation]' AS DataTableName
        ,P.[tid_StudentCohortAssociation]  AS Tid
        ,@Msg +  'CohortIdentifier = ' + COALESCE(CAST(P.CohortIdentifier AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCohortAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCohortBySnapshotId](@Ids) F
        ON
        	P.[CohortIdentifier] = F.[CohortIdentifier]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[CohortIdentifier] IS NULL 
        AND P.[CohortIdentifier] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCohortAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCohortAssociation]' AS DataTableName
        ,P.[tid_StudentCohortAssociation]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCohortAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCohortAssociationSectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCohortAssociationSection]' AS DataTableName
        ,P.[tid_StudentCohortAssociationSection]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCohortAssociationSectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentCompetencyObjective]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentCompetencyObjective]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCompetencyObjective]' AS DataTableName
        ,P.[tid_StudentCompetencyObjective]  AS Tid
        ,@Msg +  'CompetencyLevelDescriptorCodeValue = ' + COALESCE(CAST(P.CompetencyLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CompetencyLevelDescriptorNamespace = ' + COALESCE(CAST(P.CompetencyLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCompetencyLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CompetencyLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CompetencyLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CompetencyLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CompetencyLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCompetencyObjective]' AS DataTableName
        ,P.[tid_StudentCompetencyObjective]  AS Tid
        ,@Msg +  'ObjectiveEducationOrganizationId = ' + COALESCE(CAST(P.ObjectiveEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'Objective = ' + COALESCE(CAST(P.Objective AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCompetencyObjectiveBySnapshotId](@Ids) F
        ON
        	P.[ObjectiveEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[Objective] = F.[Objective]
        	AND P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[ObjectiveGradeLevelDescriptorCodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[ObjectiveGradeLevelDescriptorNamespace]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[ObjectiveEducationOrganizationId] IS NOT NULL
        AND F.[Objective] IS NULL 
        AND P.[Objective] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorNamespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCompetencyObjective]' AS DataTableName
        ,P.[tid_StudentCompetencyObjective]  AS Tid
        ,@Msg +  'GradingPeriodBeginDate = ' + COALESCE(CAST(P.GradingPeriodBeginDate AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.GradingPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorNamespace = ' + COALESCE(CAST(P.GradingPeriodDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradingPeriodBySnapshotId](@Ids) F
        ON
        	P.[GradingPeriodBeginDate] = F.[BeginDate]
        	AND P.[GradingPeriodDescriptorCodeValue] = F.[GradingPeriodDescriptorCodeValue]
        	AND P.[GradingPeriodDescriptorNamespace] = F.[GradingPeriodDescriptorNamespace]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[GradingPeriodBeginDate] IS NOT NULL
        AND F.[GradingPeriodDescriptorCodeValue] IS NULL 
        AND P.[GradingPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[GradingPeriodDescriptorNamespace] IS NULL 
        AND P.[GradingPeriodDescriptorNamespace] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCompetencyObjective]' AS DataTableName
        ,P.[tid_StudentCompetencyObjective]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCompetencyObjective]' AS DataTableName
        ,P.[tid_StudentCompetencyObjective]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramEducationOrganizationId] = F.[ProgramEducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramEducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCompetencyObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCompetencyObjective]' AS DataTableName
        ,P.[tid_StudentCompetencyObjective]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCompetencyObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentSectionAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentCTEProgramAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentCTEProgramAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCTEProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCTEProgramAssociation]' AS DataTableName
        ,P.[tid_StudentCTEProgramAssociation]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCTEProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramEducationOrganizationId] = F.[ProgramEducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramEducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentCTEProgramAssociationCTEProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentCTEProgramAssociationCTEProgram]' AS DataTableName
        ,P.[tid_StudentCTEProgramAssociationCTEProgram]  AS Tid
        ,@Msg +  'CareerPathwayType = ' + COALESCE(CAST(P.CareerPathwayType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentCTEProgramAssociationCTEProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[CareerPathwayType] F
        ON
        	P.[CareerPathwayType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[CareerPathwayType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentDisciplineIncidentAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentDisciplineIncidentAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentDisciplineIncidentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentDisciplineIncidentAssociation]' AS DataTableName
        ,P.[tid_StudentDisciplineIncidentAssociation]  AS Tid
        ,@Msg +  'IncidentIdentifier = ' + COALESCE(CAST(P.IncidentIdentifier AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentDisciplineIncidentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetDisciplineIncidentBySnapshotId](@Ids) F
        ON
        	P.[IncidentIdentifier] = F.[IncidentIdentifier]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[IncidentIdentifier] IS NULL 
        AND P.[IncidentIdentifier] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentDisciplineIncidentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentDisciplineIncidentAssociation]' AS DataTableName
        ,P.[tid_StudentDisciplineIncidentAssociation]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentDisciplineIncidentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentDisciplineIncidentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentDisciplineIncidentAssociation]' AS DataTableName
        ,P.[tid_StudentDisciplineIncidentAssociation]  AS Tid
        ,@Msg +  'StudentParticipationCodeType = ' + COALESCE(CAST(P.StudentParticipationCodeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentDisciplineIncidentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[StudentParticipationCodeType] F
        ON
        	P.[StudentParticipationCodeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[StudentParticipationCodeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentDisciplineIncidentAssociationBehaviorSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentDisciplineIncidentAssociationBehavior]' AS DataTableName
        ,P.[tid_StudentDisciplineIncidentAssociationBehavior]  AS Tid
        ,@Msg +  'BehaviorDescriptorCodeValue = ' + COALESCE(CAST(P.BehaviorDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BehaviorDescriptorNamespace = ' + COALESCE(CAST(P.BehaviorDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentDisciplineIncidentAssociationBehaviorBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetBehaviorDescriptorBySnapshotId](@Ids) F
        ON
        	P.[BehaviorDescriptorCodeValue] = F.[CodeValue]
        	AND P.[BehaviorDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[BehaviorDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[BehaviorDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentEducationOrganizationAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentEducationOrganizationAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_StudentEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentEducationOrganizationAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_StudentEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'ResponsibilityDescriptorCodeValue = ' + COALESCE(CAST(P.ResponsibilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ResponsibilityDescriptorNamespace = ' + COALESCE(CAST(P.ResponsibilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentEducationOrganizationAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetResponsibilityDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ResponsibilityDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ResponsibilityDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ResponsibilityDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ResponsibilityDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_StudentEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentEducationOrganizationAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentGradebookEntry]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentGradebookEntry]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentGradebookEntrySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentGradebookEntry]' AS DataTableName
        ,P.[tid_StudentGradebookEntry]  AS Tid
        ,@Msg +  'CompetencyLevelDescriptorCodeValue = ' + COALESCE(CAST(P.CompetencyLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CompetencyLevelDescriptorNamespace = ' + COALESCE(CAST(P.CompetencyLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentGradebookEntryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCompetencyLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CompetencyLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CompetencyLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CompetencyLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CompetencyLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentGradebookEntrySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentGradebookEntry]' AS DataTableName
        ,P.[tid_StudentGradebookEntry]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'DateAssigned = ' + COALESCE(CAST(P.DateAssigned AS NVARCHAR(128)), '') +  '|'+'GradebookEntryTitle = ' + COALESCE(CAST(P.GradebookEntryTitle AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentGradebookEntryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradebookEntryBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[DateAssigned] = F.[DateAssigned]
        	AND P.[GradebookEntryTitle] = F.[GradebookEntryTitle]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[DateAssigned] IS NULL 
        AND P.[DateAssigned] IS NOT NULL
        AND F.[GradebookEntryTitle] IS NULL 
        AND P.[GradebookEntryTitle] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentGradebookEntrySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentGradebookEntry]' AS DataTableName
        ,P.[tid_StudentGradebookEntry]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentGradebookEntryBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentSectionAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentInterventionAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentInterventionAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAssociation]' AS DataTableName
        ,P.[tid_StudentInterventionAssociation]  AS Tid
        ,@Msg +  'CohortIdentifier = ' + COALESCE(CAST(P.CohortIdentifier AS NVARCHAR(128)), '') +  '|'+'CohortEducationOrganizationId = ' + COALESCE(CAST(P.CohortEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCohortBySnapshotId](@Ids) F
        ON
        	P.[CohortIdentifier] = F.[CohortIdentifier]
        	AND P.[CohortEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[CohortIdentifier] IS NULL 
        AND P.[CohortIdentifier] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[CohortEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAssociation]' AS DataTableName
        ,P.[tid_StudentInterventionAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'InterventionIdentificationCode = ' + COALESCE(CAST(P.InterventionIdentificationCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetInterventionBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[InterventionIdentificationCode] = F.[InterventionIdentificationCode]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[InterventionIdentificationCode] IS NULL 
        AND P.[InterventionIdentificationCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAssociation]' AS DataTableName
        ,P.[tid_StudentInterventionAssociation]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAssociationInterventionEffectivenessSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAssociationInterventionEffectiveness]' AS DataTableName
        ,P.[tid_StudentInterventionAssociationInterventionEffectiveness]  AS Tid
        ,@Msg +  'DiagnosisDescriptorCodeValue = ' + COALESCE(CAST(P.DiagnosisDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DiagnosisDescriptorNamespace = ' + COALESCE(CAST(P.DiagnosisDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAssociationInterventionEffectivenessBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetDiagnosisDescriptorBySnapshotId](@Ids) F
        ON
        	P.[DiagnosisDescriptorCodeValue] = F.[CodeValue]
        	AND P.[DiagnosisDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[DiagnosisDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[DiagnosisDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAssociationInterventionEffectivenessSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAssociationInterventionEffectiveness]' AS DataTableName
        ,P.[tid_StudentInterventionAssociationInterventionEffectiveness]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAssociationInterventionEffectivenessBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[GradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[GradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[GradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[GradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAssociationInterventionEffectivenessSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAssociationInterventionEffectiveness]' AS DataTableName
        ,P.[tid_StudentInterventionAssociationInterventionEffectiveness]  AS Tid
        ,@Msg +  'InterventionEffectivenessRatingType = ' + COALESCE(CAST(P.InterventionEffectivenessRatingType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAssociationInterventionEffectivenessBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[InterventionEffectivenessRatingType] F
        ON
        	P.[InterventionEffectivenessRatingType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[InterventionEffectivenessRatingType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAssociationInterventionEffectivenessSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAssociationInterventionEffectiveness]' AS DataTableName
        ,P.[tid_StudentInterventionAssociationInterventionEffectiveness]  AS Tid
        ,@Msg +  'PopulationServedType = ' + COALESCE(CAST(P.PopulationServedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAssociationInterventionEffectivenessBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[PopulationServedType] F
        ON
        	P.[PopulationServedType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[PopulationServedType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentInterventionAttendanceEvent]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentInterventionAttendanceEvent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentInterventionAttendanceEvent]  AS Tid
        ,@Msg +  'AttendanceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AttendanceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAttendanceEventCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AttendanceEventCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AttendanceEventCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AttendanceEventCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AttendanceEventCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentInterventionAttendanceEvent]  AS Tid
        ,@Msg +  'EducationalEnvironmentType = ' + COALESCE(CAST(P.EducationalEnvironmentType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EducationalEnvironmentType] F
        ON
        	P.[EducationalEnvironmentType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EducationalEnvironmentType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentInterventionAttendanceEvent]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'InterventionIdentificationCode = ' + COALESCE(CAST(P.InterventionIdentificationCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetInterventionBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[InterventionIdentificationCode] = F.[InterventionIdentificationCode]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[InterventionIdentificationCode] IS NULL 
        AND P.[InterventionIdentificationCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentInterventionAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentInterventionAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentInterventionAttendanceEvent]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentInterventionAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentLearningObjective]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentLearningObjective]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentLearningObjective]' AS DataTableName
        ,P.[tid_StudentLearningObjective]  AS Tid
        ,@Msg +  'CompetencyLevelDescriptorCodeValue = ' + COALESCE(CAST(P.CompetencyLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CompetencyLevelDescriptorNamespace = ' + COALESCE(CAST(P.CompetencyLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetCompetencyLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[CompetencyLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[CompetencyLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[CompetencyLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[CompetencyLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentLearningObjective]' AS DataTableName
        ,P.[tid_StudentLearningObjective]  AS Tid
        ,@Msg +  'GradingPeriodBeginDate = ' + COALESCE(CAST(P.GradingPeriodBeginDate AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorCodeValue = ' + COALESCE(CAST(P.GradingPeriodDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradingPeriodDescriptorNamespace = ' + COALESCE(CAST(P.GradingPeriodDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradingPeriodBySnapshotId](@Ids) F
        ON
        	P.[GradingPeriodBeginDate] = F.[BeginDate]
        	AND P.[GradingPeriodDescriptorCodeValue] = F.[GradingPeriodDescriptorCodeValue]
        	AND P.[GradingPeriodDescriptorNamespace] = F.[GradingPeriodDescriptorNamespace]
        	AND P.[SchoolId] = F.[SchoolId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[GradingPeriodBeginDate] IS NOT NULL
        AND F.[GradingPeriodDescriptorCodeValue] IS NULL 
        AND P.[GradingPeriodDescriptorCodeValue] IS NOT NULL
        AND F.[GradingPeriodDescriptorNamespace] IS NULL 
        AND P.[GradingPeriodDescriptorNamespace] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentLearningObjective]' AS DataTableName
        ,P.[tid_StudentLearningObjective]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'Objective = ' + COALESCE(CAST(P.Objective AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ObjectiveGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.ObjectiveGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetLearningObjectiveBySnapshotId](@Ids) F
        ON
        	P.[AcademicSubjectDescriptorCodeValue] = F.[AcademicSubjectDescriptorCodeValue]
        	AND P.[AcademicSubjectDescriptorNamespace] = F.[AcademicSubjectDescriptorNamespace]
        	AND P.[Objective] = F.[Objective]
        	AND P.[ObjectiveGradeLevelDescriptorCodeValue] = F.[ObjectiveGradeLevelDescriptorCodeValue]
        	AND P.[ObjectiveGradeLevelDescriptorNamespace] = F.[ObjectiveGradeLevelDescriptorNamespace]
        WHERE F.[AcademicSubjectDescriptorCodeValue] IS NULL 
        AND P.[AcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[AcademicSubjectDescriptorNamespace] IS NULL 
        AND P.[AcademicSubjectDescriptorNamespace] IS NOT NULL
        AND F.[Objective] IS NULL 
        AND P.[Objective] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorCodeValue] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[ObjectiveGradeLevelDescriptorNamespace] IS NULL 
        AND P.[ObjectiveGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentLearningObjective]' AS DataTableName
        ,P.[tid_StudentLearningObjective]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentLearningObjective]' AS DataTableName
        ,P.[tid_StudentLearningObjective]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramEducationOrganizationId] = F.[ProgramEducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramEducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentLearningObjectiveSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentLearningObjective]' AS DataTableName
        ,P.[tid_StudentLearningObjective]  AS Tid
        ,@Msg +  'StudentSectionAssociationBeginDate = ' + COALESCE(CAST(P.StudentSectionAssociationBeginDate AS NVARCHAR(128)), '') +  '|'+'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentLearningObjectiveBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentSectionAssociationBySnapshotId](@Ids) F
        ON
        	P.[StudentSectionAssociationBeginDate] = F.[BeginDate]
        	AND P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[BeginDate] IS NULL 
        AND P.[StudentSectionAssociationBeginDate] IS NOT NULL
        AND F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentMigrantEducationProgramAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentMigrantEducationProgramAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentMigrantEducationProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentMigrantEducationProgramAssociation]' AS DataTableName
        ,P.[tid_StudentMigrantEducationProgramAssociation]  AS Tid
        ,@Msg +  'ContinuationOfServicesReasonDescriptorCodeValue = ' + COALESCE(CAST(P.ContinuationOfServicesReasonDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ContinuationOfServicesReasonDescriptorNamespace = ' + COALESCE(CAST(P.ContinuationOfServicesReasonDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentMigrantEducationProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetContinuationOfServicesReasonDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ContinuationOfServicesReasonDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ContinuationOfServicesReasonDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ContinuationOfServicesReasonDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ContinuationOfServicesReasonDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentMigrantEducationProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentMigrantEducationProgramAssociation]' AS DataTableName
        ,P.[tid_StudentMigrantEducationProgramAssociation]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentMigrantEducationProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramEducationOrganizationId] = F.[ProgramEducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramEducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentParentAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentParentAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentParentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentParentAssociation]' AS DataTableName
        ,P.[tid_StudentParentAssociation]  AS Tid
        ,@Msg +  'ParentUniqueId = ' + COALESCE(CAST(P.ParentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentParentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetParentBySnapshotId](@Ids) F
        ON
        	P.[ParentUniqueId] = F.[ParentUniqueId]
        WHERE F.[ParentUniqueId] IS NULL 
        AND P.[ParentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentParentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentParentAssociation]' AS DataTableName
        ,P.[tid_StudentParentAssociation]  AS Tid
        ,@Msg +  'RelationType = ' + COALESCE(CAST(P.RelationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentParentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[RelationType] F
        ON
        	P.[RelationType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RelationType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentParentAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentParentAssociation]' AS DataTableName
        ,P.[tid_StudentParentAssociation]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentParentAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentProgramAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentProgramAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAssociation]' AS DataTableName
        ,P.[tid_StudentProgramAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAssociation]' AS DataTableName
        ,P.[tid_StudentProgramAssociation]  AS Tid
        ,@Msg +  'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) F
        ON
        	P.[ProgramEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAssociation]' AS DataTableName
        ,P.[tid_StudentProgramAssociation]  AS Tid
        ,@Msg +  'ReasonExitedDescriptorCodeValue = ' + COALESCE(CAST(P.ReasonExitedDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ReasonExitedDescriptorNamespace = ' + COALESCE(CAST(P.ReasonExitedDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetReasonExitedDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ReasonExitedDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ReasonExitedDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ReasonExitedDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ReasonExitedDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAssociation]' AS DataTableName
        ,P.[tid_StudentProgramAssociation]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAssociationServiceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAssociationService]' AS DataTableName
        ,P.[tid_StudentProgramAssociationService]  AS Tid
        ,@Msg +  'ServiceDescriptorCodeValue = ' + COALESCE(CAST(P.ServiceDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ServiceDescriptorNamespace = ' + COALESCE(CAST(P.ServiceDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAssociationServiceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetServiceDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ServiceDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ServiceDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ServiceDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ServiceDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentProgramAttendanceEvent]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentProgramAttendanceEvent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentProgramAttendanceEvent]  AS Tid
        ,@Msg +  'AttendanceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AttendanceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAttendanceEventCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AttendanceEventCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AttendanceEventCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AttendanceEventCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AttendanceEventCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentProgramAttendanceEvent]  AS Tid
        ,@Msg +  'EducationalEnvironmentType = ' + COALESCE(CAST(P.EducationalEnvironmentType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EducationalEnvironmentType] F
        ON
        	P.[EducationalEnvironmentType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EducationalEnvironmentType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentProgramAttendanceEvent]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentProgramAttendanceEvent]  AS Tid
        ,@Msg +  'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetProgramBySnapshotId](@Ids) F
        ON
        	P.[ProgramEducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentProgramAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentProgramAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentProgramAttendanceEvent]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentProgramAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentSchoolAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentSchoolAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'EntryGradeLevelReasonType = ' + COALESCE(CAST(P.EntryGradeLevelReasonType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EntryGradeLevelReasonType] F
        ON
        	P.[EntryGradeLevelReasonType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EntryGradeLevelReasonType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'EntryTypeDescriptorCodeValue = ' + COALESCE(CAST(P.EntryTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EntryTypeDescriptorNamespace = ' + COALESCE(CAST(P.EntryTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEntryTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[EntryTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[EntryTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[EntryTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[EntryTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'ExitWithdrawTypeDescriptorCodeValue = ' + COALESCE(CAST(P.ExitWithdrawTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ExitWithdrawTypeDescriptorNamespace = ' + COALESCE(CAST(P.ExitWithdrawTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetExitWithdrawTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ExitWithdrawTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ExitWithdrawTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ExitWithdrawTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ExitWithdrawTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'EntryGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.EntryGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EntryGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.EntryGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[EntryGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[EntryGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[EntryGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[EntryGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'GraduationPlanTypeDescriptorCodeValue = ' + COALESCE(CAST(P.GraduationPlanTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GraduationPlanTypeDescriptorNamespace = ' + COALESCE(CAST(P.GraduationPlanTypeDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'GraduationSchoolYear = ' + COALESCE(CAST(P.GraduationSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGraduationPlanBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[GraduationPlanTypeDescriptorCodeValue] = F.[GraduationPlanTypeDescriptorCodeValue]
        	AND P.[GraduationPlanTypeDescriptorNamespace] = F.[GraduationPlanTypeDescriptorNamespace]
        	AND P.[GraduationSchoolYear] = F.[GraduationSchoolYear]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[GraduationPlanTypeDescriptorCodeValue] IS NULL 
        AND P.[GraduationPlanTypeDescriptorCodeValue] IS NOT NULL
        AND F.[GraduationPlanTypeDescriptorNamespace] IS NULL 
        AND P.[GraduationPlanTypeDescriptorNamespace] IS NOT NULL
        AND F.[GraduationSchoolYear] IS NULL 
        AND P.[GraduationSchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'ResidencyStatusDescriptorCodeValue = ' + COALESCE(CAST(P.ResidencyStatusDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ResidencyStatusDescriptorNamespace = ' + COALESCE(CAST(P.ResidencyStatusDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetResidencyStatusDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ResidencyStatusDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ResidencyStatusDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ResidencyStatusDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ResidencyStatusDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'ClassOfSchoolYear = ' + COALESCE(CAST(P.ClassOfSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[ClassOfSchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[ClassOfSchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociation]' AS DataTableName
        ,P.[tid_StudentSchoolAssociation]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAssociationEducationPlanSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAssociationEducationPlan]' AS DataTableName
        ,P.[tid_StudentSchoolAssociationEducationPlan]  AS Tid
        ,@Msg +  'EducationPlanType = ' + COALESCE(CAST(P.EducationPlanType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAssociationEducationPlanBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EducationPlanType] F
        ON
        	P.[EducationPlanType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EducationPlanType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentSchoolAttendanceEvent]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentSchoolAttendanceEvent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSchoolAttendanceEvent]  AS Tid
        ,@Msg +  'AttendanceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AttendanceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAttendanceEventCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AttendanceEventCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AttendanceEventCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AttendanceEventCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AttendanceEventCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSchoolAttendanceEvent]  AS Tid
        ,@Msg +  'EducationalEnvironmentType = ' + COALESCE(CAST(P.EducationalEnvironmentType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EducationalEnvironmentType] F
        ON
        	P.[EducationalEnvironmentType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EducationalEnvironmentType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSchoolAttendanceEvent]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSchoolAttendanceEvent]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSessionBySnapshotId](@Ids) F
        ON
        	P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        WHERE F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSchoolAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSchoolAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSchoolAttendanceEvent]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSchoolAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentSectionAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentSectionAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSectionAssociation]' AS DataTableName
        ,P.[tid_StudentSectionAssociation]  AS Tid
        ,@Msg +  'RepeatIdentifierType = ' + COALESCE(CAST(P.RepeatIdentifierType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[RepeatIdentifierType] F
        ON
        	P.[RepeatIdentifierType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[RepeatIdentifierType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSectionAssociation]' AS DataTableName
        ,P.[tid_StudentSectionAssociation]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSectionAssociation]' AS DataTableName
        ,P.[tid_StudentSectionAssociation]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentSectionAttendanceEvent]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentSectionAttendanceEvent]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSectionAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSectionAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSectionAttendanceEvent]  AS Tid
        ,@Msg +  'AttendanceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AttendanceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSectionAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAttendanceEventCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AttendanceEventCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AttendanceEventCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AttendanceEventCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AttendanceEventCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSectionAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSectionAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSectionAttendanceEvent]  AS Tid
        ,@Msg +  'EducationalEnvironmentType = ' + COALESCE(CAST(P.EducationalEnvironmentType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSectionAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[EducationalEnvironmentType] F
        ON
        	P.[EducationalEnvironmentType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[EducationalEnvironmentType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSectionAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSectionAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSectionAttendanceEvent]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSectionAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSectionBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[TermDescriptorCodeValue] IS NULL 
        AND P.[TermDescriptorCodeValue] IS NOT NULL
        AND F.[TermDescriptorNamespace] IS NULL 
        AND P.[TermDescriptorNamespace] IS NOT NULL
        AND F.[UniqueSectionCode] IS NULL 
        AND P.[UniqueSectionCode] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSectionAttendanceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSectionAttendanceEvent]' AS DataTableName
        ,P.[tid_StudentSectionAttendanceEvent]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSectionAttendanceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentBySnapshotId](@Ids) F
        ON
        	P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentSpecialEducationProgramAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentSpecialEducationProgramAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSpecialEducationProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSpecialEducationProgramAssociation]' AS DataTableName
        ,P.[tid_StudentSpecialEducationProgramAssociation]  AS Tid
        ,@Msg +  'SpecialEducationSettingDescriptorCodeValue = ' + COALESCE(CAST(P.SpecialEducationSettingDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SpecialEducationSettingDescriptorNamespace = ' + COALESCE(CAST(P.SpecialEducationSettingDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSpecialEducationProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSpecialEducationSettingDescriptorBySnapshotId](@Ids) F
        ON
        	P.[SpecialEducationSettingDescriptorCodeValue] = F.[CodeValue]
        	AND P.[SpecialEducationSettingDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[SpecialEducationSettingDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[SpecialEducationSettingDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSpecialEducationProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSpecialEducationProgramAssociation]' AS DataTableName
        ,P.[tid_StudentSpecialEducationProgramAssociation]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSpecialEducationProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramEducationOrganizationId] = F.[ProgramEducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramEducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentSpecialEducationProgramAssociationServiceProviderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentSpecialEducationProgramAssociationServiceProvider]' AS DataTableName
        ,P.[tid_StudentSpecialEducationProgramAssociationServiceProvider]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentSpecialEducationProgramAssociationServiceProviderBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[StaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_edfi].[usp_eStudentTitleIPartAProgramAssociation]******/

CREATE PROCEDURE [t_edfi].[usp_eStudentTitleIPartAProgramAssociation]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Ids  [tods].[udt_IntList];
    DECLARE @ErrorSeverity NVARCHAR(10) = 'WARN';
    DECLARE @ErrorNumber NVARCHAR(10) = '80001';
    DECLARE @Msg NVARCHAR(1024) = 'FAILED external referential integrity check: ';
    DECLARE @SprocName NVARCHAR(255) = OBJECT_NAME(@@PROCID);

    INSERT INTO @Ids
    SELECT @pSnapshotId;

    BEGIN TRY

        
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentTitleIPartAProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentTitleIPartAProgramAssociation]' AS DataTableName
        ,P.[tid_StudentTitleIPartAProgramAssociation]  AS Tid
        ,@Msg +  'BeginDate = ' + COALESCE(CAST(P.BeginDate AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramEducationOrganizationId = ' + COALESCE(CAST(P.ProgramEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentTitleIPartAProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStudentProgramAssociationBySnapshotId](@Ids) F
        ON
        	P.[BeginDate] = F.[BeginDate]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[ProgramEducationOrganizationId] = F.[ProgramEducationOrganizationId]
        	AND P.[ProgramName] = F.[ProgramName]
        	AND P.[ProgramType] = F.[ProgramType]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[BeginDate] IS NULL 
        AND P.[BeginDate] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[ProgramEducationOrganizationId] IS NULL 
        AND P.[ProgramEducationOrganizationId] IS NOT NULL
        AND F.[ProgramName] IS NULL 
        AND P.[ProgramName] IS NOT NULL
        AND F.[ProgramType] IS NULL 
        AND P.[ProgramType] IS NOT NULL
        AND F.[StudentUniqueId] IS NULL 
        AND P.[StudentUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_edfi].[StudentTitleIPartAProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_edfi].[StudentTitleIPartAProgramAssociation]' AS DataTableName
        ,P.[tid_StudentTitleIPartAProgramAssociation]  AS Tid
        ,@Msg +  'TitleIPartAParticipantType = ' + COALESCE(CAST(P.TitleIPartAParticipantType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_edfi].[ifn_GetStudentTitleIPartAProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[TitleIPartAParticipantType] F
        ON
        	P.[TitleIPartAParticipantType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[TitleIPartAParticipantType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [tods].[usp_eMaster]******/

IF OBJECT_ID('[tods].[usp_eMaster]', 'P') IS NOT NULL
    DROP PROCEDURE [tods].[usp_eMaster];
GO

CREATE PROCEDURE [tods].[usp_eMaster]
    @pSnapshotId INT
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
		EXEC [t_edfi].[usp_eAcademicWeek] @pSnapshotId;

		EXEC [t_edfi].[usp_eAccount] @pSnapshotId;

		EXEC [t_edfi].[usp_eAccountabilityRating] @pSnapshotId;

		EXEC [t_edfi].[usp_eActual] @pSnapshotId;

		EXEC [t_edfi].[usp_eAssessment] @pSnapshotId;

		EXEC [t_edfi].[usp_eAssessmentFamily] @pSnapshotId;

		EXEC [t_edfi].[usp_eAssessmentItem] @pSnapshotId;

		EXEC [t_edfi].[usp_eBellSchedule] @pSnapshotId;

		EXEC [t_edfi].[usp_eBudget] @pSnapshotId;

		EXEC [t_edfi].[usp_eCalendarDate] @pSnapshotId;

		EXEC [t_edfi].[usp_eClassPeriod] @pSnapshotId;

		EXEC [t_edfi].[usp_eCohort] @pSnapshotId;

		EXEC [t_edfi].[usp_eCompetencyObjective] @pSnapshotId;

		EXEC [t_edfi].[usp_eContractedStaff] @pSnapshotId;

		EXEC [t_edfi].[usp_eCourse] @pSnapshotId;

		EXEC [t_edfi].[usp_eCourseOffering] @pSnapshotId;

		EXEC [t_edfi].[usp_eCourseTranscript] @pSnapshotId;

		EXEC [t_edfi].[usp_eCredential] @pSnapshotId;

		EXEC [t_edfi].[usp_eDisciplineAction] @pSnapshotId;

		EXEC [t_edfi].[usp_eDisciplineIncident] @pSnapshotId;

		EXEC [t_edfi].[usp_eEducationContent] @pSnapshotId;

		EXEC [t_edfi].[usp_eEducationOrganization] @pSnapshotId;

		EXEC [t_edfi].[usp_eEducationOrganizationInterventionPrescriptionAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eEducationOrganizationNetwork] @pSnapshotId;

		EXEC [t_edfi].[usp_eEducationOrganizationNetworkAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eEducationOrganizationPeerAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eEducationServiceCenter] @pSnapshotId;

		EXEC [t_edfi].[usp_eFeederSchoolAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eGrade] @pSnapshotId;

		EXEC [t_edfi].[usp_eGradebookEntry] @pSnapshotId;

		EXEC [t_edfi].[usp_eGradingPeriod] @pSnapshotId;

		EXEC [t_edfi].[usp_eGraduationPlan] @pSnapshotId;

		EXEC [t_edfi].[usp_eIntervention] @pSnapshotId;

		EXEC [t_edfi].[usp_eInterventionPrescription] @pSnapshotId;

		EXEC [t_edfi].[usp_eInterventionStudy] @pSnapshotId;

		EXEC [t_edfi].[usp_eLearningObjective] @pSnapshotId;

		EXEC [t_edfi].[usp_eLearningStandard] @pSnapshotId;

		EXEC [t_edfi].[usp_eLeaveEvent] @pSnapshotId;

		EXEC [t_edfi].[usp_eLocalEducationAgency] @pSnapshotId;

		EXEC [t_edfi].[usp_eLocation] @pSnapshotId;

		EXEC [t_edfi].[usp_eObjectiveAssessment] @pSnapshotId;

		EXEC [t_edfi].[usp_eOpenStaffPosition] @pSnapshotId;

		EXEC [t_edfi].[usp_eParent] @pSnapshotId;

		EXEC [t_edfi].[usp_ePayroll] @pSnapshotId;

		EXEC [t_edfi].[usp_ePostSecondaryEvent] @pSnapshotId;

		EXEC [t_edfi].[usp_eProgram] @pSnapshotId;

		EXEC [t_edfi].[usp_eReportCard] @pSnapshotId;

		EXEC [t_edfi].[usp_eRestraintEvent] @pSnapshotId;

		EXEC [t_edfi].[usp_eSchool] @pSnapshotId;

		EXEC [t_edfi].[usp_eSection] @pSnapshotId;

		EXEC [t_edfi].[usp_eSectionAttendanceTakenEvent] @pSnapshotId;

		EXEC [t_edfi].[usp_eSession] @pSnapshotId;

		EXEC [t_edfi].[usp_eStaff] @pSnapshotId;

		EXEC [t_edfi].[usp_eStaffCohortAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStaffEducationOrganizationAssignmentAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStaffEducationOrganizationEmploymentAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStaffProgramAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStaffSchoolAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStaffSectionAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStateEducationAgency] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudent] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentAcademicRecord] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentAssessment] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentCohortAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentCompetencyObjective] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentCTEProgramAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentDisciplineIncidentAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentEducationOrganizationAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentGradebookEntry] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentInterventionAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentInterventionAttendanceEvent] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentLearningObjective] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentMigrantEducationProgramAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentParentAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentProgramAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentProgramAttendanceEvent] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentSchoolAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentSchoolAttendanceEvent] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentSectionAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentSectionAttendanceEvent] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentSpecialEducationProgramAssociation] @pSnapshotId;

		EXEC [t_edfi].[usp_eStudentTitleIPartAProgramAssociation] @pSnapshotId;

	END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO