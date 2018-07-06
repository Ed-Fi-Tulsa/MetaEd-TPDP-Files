/******Procedure [t_extension].[usp_eAnonymizedStudent]******/

CREATE PROCEDURE [t_extension].[usp_eAnonymizedStudent]
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
        (SELECT '[t_extension].[AnonymizedStudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudent]' AS DataTableName
        ,P.[tid_AnonymizedStudent]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudent]' AS DataTableName
        ,P.[tid_AnonymizedStudent]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudent]' AS DataTableName
        ,P.[tid_AnonymizedStudent]  AS Tid
        ,@Msg +  'GenderType = ' + COALESCE(CAST(P.GenderType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudent]' AS DataTableName
        ,P.[tid_AnonymizedStudent]  AS Tid
        ,@Msg +  'SchoolFoodServicesEligibilityDescriptorCodeValue = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SchoolFoodServicesEligibilityDescriptorNamespace = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudent]' AS DataTableName
        ,P.[tid_AnonymizedStudent]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudent]' AS DataTableName
        ,P.[tid_AnonymizedStudent]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudent]' AS DataTableName
        ,P.[tid_AnonymizedStudent]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudent]' AS DataTableName
        ,P.[tid_AnonymizedStudent]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentRace]' AS DataTableName
        ,P.[tid_AnonymizedStudentRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentLanguage]' AS DataTableName
        ,P.[tid_AnonymizedStudentLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentLanguageUseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentLanguageUse]' AS DataTableName
        ,P.[tid_AnonymizedStudentLanguageUse]  AS Tid
        ,@Msg +  'LanguageUseType = ' + COALESCE(CAST(P.LanguageUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentLanguageUseBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentDisability]' AS DataTableName
        ,P.[tid_AnonymizedStudentDisability]  AS Tid
        ,@Msg +  'DisabilityDescriptorCodeValue = ' + COALESCE(CAST(P.DisabilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisabilityDescriptorNamespace = ' + COALESCE(CAST(P.DisabilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentDisability]' AS DataTableName
        ,P.[tid_AnonymizedStudentDisability]  AS Tid
        ,@Msg +  'DisabilityDeterminationSourceType = ' + COALESCE(CAST(P.DisabilityDeterminationSourceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentDisabilityBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eAnonymizedStudentAcademicRecord]******/

CREATE PROCEDURE [t_extension].[usp_eAnonymizedStudentAcademicRecord]
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
        (SELECT '[t_extension].[AnonymizedStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAcademicRecord]' AS DataTableName
        ,P.[tid_AnonymizedStudentAcademicRecord]  AS Tid
        ,@Msg +  'AnonymizedStudentIdentifier = ' + COALESCE(CAST(P.AnonymizedStudentIdentifier AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) F
        ON
        	P.[AnonymizedStudentIdentifier] = F.[AnonymizedStudentIdentifier]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[AnonymizedStudentIdentifier] IS NULL 
        AND P.[AnonymizedStudentIdentifier] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
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
        (SELECT '[t_extension].[AnonymizedStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAcademicRecord]' AS DataTableName
        ,P.[tid_AnonymizedStudentAcademicRecord]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAcademicRecord]' AS DataTableName
        ,P.[tid_AnonymizedStudentAcademicRecord]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAcademicRecordBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eAnonymizedStudentAssessment]******/

CREATE PROCEDURE [t_extension].[usp_eAnonymizedStudentAssessment]
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessment]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessment]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessment]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessment]  AS Tid
        ,@Msg +  'AnonymizedStudentIdentifier = ' + COALESCE(CAST(P.AnonymizedStudentIdentifier AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) F
        ON
        	P.[AnonymizedStudentIdentifier] = F.[AnonymizedStudentIdentifier]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[AnonymizedStudentIdentifier] IS NULL 
        AND P.[AnonymizedStudentIdentifier] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessment]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessment]  AS Tid
        ,@Msg +  'AssessmentCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessment]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessment]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessment]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessment]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessment]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessment]  AS Tid
        ,@Msg +  'TakenSchoolYear = ' + COALESCE(CAST(P.TakenSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[TakenSchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[TakenSchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[AnonymizedStudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessment]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessment]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessment]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessment]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessmentScoreResult]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessmentScoreResult]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentScoreResultBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessmentScoreResult]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessmentScoreResult]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentScoreResultBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentAssessmentPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentAssessmentPerformanceLevel]' AS DataTableName
        ,P.[tid_AnonymizedStudentAssessmentPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentAssessmentPerformanceLevelBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eAnonymizedStudentCourseAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eAnonymizedStudentCourseAssociation]
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
        (SELECT '[t_extension].[AnonymizedStudentCourseAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentCourseAssociation]' AS DataTableName
        ,P.[tid_AnonymizedStudentCourseAssociation]  AS Tid
        ,@Msg +  'AnonymizedStudentIdentifier = ' + COALESCE(CAST(P.AnonymizedStudentIdentifier AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentCourseAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) F
        ON
        	P.[AnonymizedStudentIdentifier] = F.[AnonymizedStudentIdentifier]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[AnonymizedStudentIdentifier] IS NULL 
        AND P.[AnonymizedStudentIdentifier] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
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
        (SELECT '[t_extension].[AnonymizedStudentCourseAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentCourseAssociation]' AS DataTableName
        ,P.[tid_AnonymizedStudentCourseAssociation]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentCourseAssociationBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eAnonymizedStudentCourseTranscript]******/

CREATE PROCEDURE [t_extension].[usp_eAnonymizedStudentCourseTranscript]
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
        (SELECT '[t_extension].[AnonymizedStudentCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentCourseTranscript]' AS DataTableName
        ,P.[tid_AnonymizedStudentCourseTranscript]  AS Tid
        ,@Msg +  'AnonymizedStudentIdentifier = ' + COALESCE(CAST(P.AnonymizedStudentIdentifier AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAnonymizedStudentAcademicRecordBySnapshotId](@Ids) F
        ON
        	P.[AnonymizedStudentIdentifier] = F.[AnonymizedStudentIdentifier]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        WHERE F.[AnonymizedStudentIdentifier] IS NULL 
        AND P.[AnonymizedStudentIdentifier] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
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
        (SELECT '[t_extension].[AnonymizedStudentCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentCourseTranscript]' AS DataTableName
        ,P.[tid_AnonymizedStudentCourseTranscript]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[AnonymizedStudentCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentCourseTranscript]' AS DataTableName
        ,P.[tid_AnonymizedStudentCourseTranscript]  AS Tid
        ,@Msg +  'CourseRepeatCodeType = ' + COALESCE(CAST(P.CourseRepeatCodeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentCourseTranscriptBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eAnonymizedStudentEducationOrganizationAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eAnonymizedStudentEducationOrganizationAssociation]
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
        (SELECT '[t_extension].[AnonymizedStudentEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_AnonymizedStudentEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'AnonymizedStudentIdentifier = ' + COALESCE(CAST(P.AnonymizedStudentIdentifier AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentEducationOrganizationAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) F
        ON
        	P.[AnonymizedStudentIdentifier] = F.[AnonymizedStudentIdentifier]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[AnonymizedStudentIdentifier] IS NULL 
        AND P.[AnonymizedStudentIdentifier] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
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
        (SELECT '[t_extension].[AnonymizedStudentEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_AnonymizedStudentEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentEducationOrganizationAssociationBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eAnonymizedStudentSectionAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eAnonymizedStudentSectionAssociation]
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
        (SELECT '[t_extension].[AnonymizedStudentSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentSectionAssociation]' AS DataTableName
        ,P.[tid_AnonymizedStudentSectionAssociation]  AS Tid
        ,@Msg +  'AnonymizedStudentIdentifier = ' + COALESCE(CAST(P.AnonymizedStudentIdentifier AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAnonymizedStudentBySnapshotId](@Ids) F
        ON
        	P.[AnonymizedStudentIdentifier] = F.[AnonymizedStudentIdentifier]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[AnonymizedStudentIdentifier] IS NULL 
        AND P.[AnonymizedStudentIdentifier] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
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
        (SELECT '[t_extension].[AnonymizedStudentSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[AnonymizedStudentSectionAssociation]' AS DataTableName
        ,P.[tid_AnonymizedStudentSectionAssociation]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetAnonymizedStudentSectionAssociationBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eApplicant]******/

CREATE PROCEDURE [t_extension].[usp_eApplicant]
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
        (SELECT '[t_extension].[ApplicantSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Applicant]' AS DataTableName
        ,P.[tid_Applicant]  AS Tid
        ,@Msg +  'HighlyQualifiedAcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.HighlyQualifiedAcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'HighlyQualifiedAcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.HighlyQualifiedAcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetAcademicSubjectDescriptorBySnapshotId](@Ids) F
        ON
        	P.[HighlyQualifiedAcademicSubjectDescriptorCodeValue] = F.[CodeValue]
        	AND P.[HighlyQualifiedAcademicSubjectDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[HighlyQualifiedAcademicSubjectDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[HighlyQualifiedAcademicSubjectDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ApplicantSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Applicant]' AS DataTableName
        ,P.[tid_Applicant]  AS Tid
        ,@Msg +  'CitizenshipStatusType = ' + COALESCE(CAST(P.CitizenshipStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Applicant]' AS DataTableName
        ,P.[tid_Applicant]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Applicant]' AS DataTableName
        ,P.[tid_Applicant]  AS Tid
        ,@Msg +  'GenderType = ' + COALESCE(CAST(P.GenderType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Applicant]' AS DataTableName
        ,P.[tid_Applicant]  AS Tid
        ,@Msg +  'HighestCompletedLevelOfEducationDescriptorCodeValue = ' + COALESCE(CAST(P.HighestCompletedLevelOfEducationDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'HighestCompletedLevelOfEducationDescriptorNamespace = ' + COALESCE(CAST(P.HighestCompletedLevelOfEducationDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Applicant]' AS DataTableName
        ,P.[tid_Applicant]  AS Tid
        ,@Msg +  'ProspectEducationOrganizationId = ' + COALESCE(CAST(P.ProspectEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProspectIdentifier = ' + COALESCE(CAST(P.ProspectIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProspectBySnapshotId](@Ids) F
        ON
        	P.[ProspectEducationOrganizationId] = F.[ProspectEducationOrganizationId]
        	AND P.[ProspectIdentifier] = F.[ProspectIdentifier]
        WHERE F.[ProspectEducationOrganizationId] IS NULL 
        AND P.[ProspectEducationOrganizationId] IS NOT NULL
        AND F.[ProspectIdentifier] IS NULL 
        AND P.[ProspectIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ApplicantSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Applicant]' AS DataTableName
        ,P.[tid_Applicant]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantStaffIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantStaffIdentificationCode]' AS DataTableName
        ,P.[tid_ApplicantStaffIdentificationCode]  AS Tid
        ,@Msg +  'StaffIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.StaffIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StaffIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.StaffIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantStaffIdentificationCodeBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantAddress]' AS DataTableName
        ,P.[tid_ApplicantAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantAddress]' AS DataTableName
        ,P.[tid_ApplicantAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantInternationalAddress]' AS DataTableName
        ,P.[tid_ApplicantInternationalAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantInternationalAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantInternationalAddress]' AS DataTableName
        ,P.[tid_ApplicantInternationalAddress]  AS Tid
        ,@Msg +  'CountryDescriptorCodeValue = ' + COALESCE(CAST(P.CountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CountryDescriptorNamespace = ' + COALESCE(CAST(P.CountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantInternationalAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantTelephoneSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantTelephone]' AS DataTableName
        ,P.[tid_ApplicantTelephone]  AS Tid
        ,@Msg +  'TelephoneNumberType = ' + COALESCE(CAST(P.TelephoneNumberType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantTelephoneBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantElectronicMailSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantElectronicMail]' AS DataTableName
        ,P.[tid_ApplicantElectronicMail]  AS Tid
        ,@Msg +  'ElectronicMailType = ' + COALESCE(CAST(P.ElectronicMailType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantElectronicMailBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantRace]' AS DataTableName
        ,P.[tid_ApplicantRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantVisaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantVisa]' AS DataTableName
        ,P.[tid_ApplicantVisa]  AS Tid
        ,@Msg +  'VisaType = ' + COALESCE(CAST(P.VisaType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantVisaBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantLanguage]' AS DataTableName
        ,P.[tid_ApplicantLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantLanguageUseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantLanguageUse]' AS DataTableName
        ,P.[tid_ApplicantLanguageUse]  AS Tid
        ,@Msg +  'LanguageUseType = ' + COALESCE(CAST(P.LanguageUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantLanguageUseBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantCredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantCredential]' AS DataTableName
        ,P.[tid_ApplicantCredential]  AS Tid
        ,@Msg +  'CredentialIdentifier = ' + COALESCE(CAST(P.CredentialIdentifier AS NVARCHAR(128)), '') +  '|'+'StateOfIssueStateAbbreviationType = ' + COALESCE(CAST(P.StateOfIssueStateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantCredentialBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantBackgroundCheckSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantBackgroundCheck]' AS DataTableName
        ,P.[tid_ApplicantBackgroundCheck]  AS Tid
        ,@Msg +  'BackgroundCheckStatusDescriptorCodeValue = ' + COALESCE(CAST(P.BackgroundCheckStatusDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BackgroundCheckStatusDescriptorNamespace = ' + COALESCE(CAST(P.BackgroundCheckStatusDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBackgroundCheckBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantBackgroundCheckSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantBackgroundCheck]' AS DataTableName
        ,P.[tid_ApplicantBackgroundCheck]  AS Tid
        ,@Msg +  'BackgroundCheckTypeDescriptorCodeValue = ' + COALESCE(CAST(P.BackgroundCheckTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BackgroundCheckTypeDescriptorNamespace = ' + COALESCE(CAST(P.BackgroundCheckTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantBackgroundCheckBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantTeacherPreparationProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantTeacherPreparationProgram]' AS DataTableName
        ,P.[tid_ApplicantTeacherPreparationProgram]  AS Tid
        ,@Msg +  'LevelOfDegreeAwardedDescriptorCodeValue = ' + COALESCE(CAST(P.LevelOfDegreeAwardedDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LevelOfDegreeAwardedDescriptorNamespace = ' + COALESCE(CAST(P.LevelOfDegreeAwardedDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantTeacherPreparationProgramBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantTeacherPreparationProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantTeacherPreparationProgram]' AS DataTableName
        ,P.[tid_ApplicantTeacherPreparationProgram]  AS Tid
        ,@Msg +  'TeacherPreparationProgramTypeDescriptorCodeValue = ' + COALESCE(CAST(P.TeacherPreparationProgramTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TeacherPreparationProgramTypeDescriptorNamespace = ' + COALESCE(CAST(P.TeacherPreparationProgramTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantTeacherPreparationProgramBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantTeacherPreparationProgramAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantTeacherPreparationProgramAddress]' AS DataTableName
        ,P.[tid_ApplicantTeacherPreparationProgramAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantTeacherPreparationProgramAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantTeacherPreparationProgramAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantTeacherPreparationProgramAddress]' AS DataTableName
        ,P.[tid_ApplicantTeacherPreparationProgramAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantTeacherPreparationProgramAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantDisability]' AS DataTableName
        ,P.[tid_ApplicantDisability]  AS Tid
        ,@Msg +  'DisabilityDescriptorCodeValue = ' + COALESCE(CAST(P.DisabilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisabilityDescriptorNamespace = ' + COALESCE(CAST(P.DisabilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantDisability]' AS DataTableName
        ,P.[tid_ApplicantDisability]  AS Tid
        ,@Msg +  'DisabilityDeterminationSourceType = ' + COALESCE(CAST(P.DisabilityDeterminationSourceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantAidSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantAid]' AS DataTableName
        ,P.[tid_ApplicantAid]  AS Tid
        ,@Msg +  'AidTypeDescriptorCodeValue = ' + COALESCE(CAST(P.AidTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AidTypeDescriptorNamespace = ' + COALESCE(CAST(P.AidTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantAidBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAidTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AidTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AidTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AidTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AidTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ApplicantScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantScoreResult]' AS DataTableName
        ,P.[tid_ApplicantScoreResult]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantScoreResultBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantScoreResult]' AS DataTableName
        ,P.[tid_ApplicantScoreResult]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantScoreResultBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantIdentificationDocument]' AS DataTableName
        ,P.[tid_ApplicantIdentificationDocument]  AS Tid
        ,@Msg +  'IssuerCountryDescriptorCodeValue = ' + COALESCE(CAST(P.IssuerCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'IssuerCountryDescriptorNamespace = ' + COALESCE(CAST(P.IssuerCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantIdentificationDocumentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantIdentificationDocument]' AS DataTableName
        ,P.[tid_ApplicantIdentificationDocument]  AS Tid
        ,@Msg +  'IdentificationDocumentUseType = ' + COALESCE(CAST(P.IdentificationDocumentUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantIdentificationDocumentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ApplicantIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ApplicantIdentificationDocument]' AS DataTableName
        ,P.[tid_ApplicantIdentificationDocument]  AS Tid
        ,@Msg +  'PersonalInformationVerificationType = ' + COALESCE(CAST(P.PersonalInformationVerificationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetApplicantIdentificationDocumentBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eCourseCourseTranscriptFacts]******/

CREATE PROCEDURE [t_extension].[usp_eCourseCourseTranscriptFacts]
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
        (SELECT '[t_extension].[CourseCourseTranscriptFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseCourseTranscriptFacts]' AS DataTableName
        ,P.[tid_CourseCourseTranscriptFacts]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseCourseTranscriptFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetCourseStudentAcademicRecordFactsBySnapshotId](@Ids) F
        ON
        	P.[CourseCode] = F.[CourseCode]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        WHERE F.[CourseCode] IS NULL 
        AND P.[CourseCode] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
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
        (SELECT '[t_extension].[CourseCourseTranscriptFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseCourseTranscriptFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_CourseCourseTranscriptFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseCourseTranscriptFactsStudentsEnrolledBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eCourseStudentAcademicRecordFacts]******/

CREATE PROCEDURE [t_extension].[usp_eCourseStudentAcademicRecordFacts]
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
        (SELECT '[t_extension].[CourseStudentAcademicRecordFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAcademicRecordFacts]' AS DataTableName
        ,P.[tid_CourseStudentAcademicRecordFacts]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAcademicRecordFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAcademicRecordFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAcademicRecordFacts]' AS DataTableName
        ,P.[tid_CourseStudentAcademicRecordFacts]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAcademicRecordFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAcademicRecordFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAcademicRecordFacts]' AS DataTableName
        ,P.[tid_CourseStudentAcademicRecordFacts]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAcademicRecordFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAcademicRecordFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAcademicRecordFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_CourseStudentAcademicRecordFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAcademicRecordFactsStudentsEnrolledBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eCourseStudentAssessmentFacts]******/

CREATE PROCEDURE [t_extension].[usp_eCourseStudentAssessmentFacts]
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
        (SELECT '[t_extension].[CourseStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFacts]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFacts]  AS Tid
        ,@Msg +  'AssessmentCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFacts]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFacts]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFacts]  AS Tid
        ,@Msg +  'TakenSchoolYear = ' + COALESCE(CAST(P.TakenSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[TakenSchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[TakenSchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CourseStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFacts]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAssessmentFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsStudentsEnrolledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAssessmentFactsAggregatedScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFactsAggregatedScoreResult]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFactsAggregatedScoreResult]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsAggregatedScoreResultBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentAssessmentFactsAggregatedScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFactsAggregatedScoreResult]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFactsAggregatedScoreResult]  AS Tid
        ,@Msg +  'AverageScoreResultDatatypeType = ' + COALESCE(CAST(P.AverageScoreResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsAggregatedScoreResultBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[AverageScoreResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AverageScoreResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CourseStudentAssessmentFactsAggregatedPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentAssessmentFactsAggregatedPerformanceLevel]' AS DataTableName
        ,P.[tid_CourseStudentAssessmentFactsAggregatedPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentAssessmentFactsAggregatedPerformanceLevelBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eCourseStudentFacts]******/

CREATE PROCEDURE [t_extension].[usp_eCourseStudentFacts]
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
        (SELECT '[t_extension].[CourseStudentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFacts]' AS DataTableName
        ,P.[tid_CourseStudentFacts]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_CourseStudentFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsStudentsEnrolledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedSex]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedSex]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedSexBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedSex]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedSex]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedSexBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedGenderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedGender]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedGender]  AS Tid
        ,@Msg +  'GenderType = ' + COALESCE(CAST(P.GenderType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedGenderBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedGenderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedGender]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedGender]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedGenderBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedHispanicLatinoEthnicitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedHispanicLatinoEthnicity]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedHispanicLatinoEthnicity]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedHispanicLatinoEthnicityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedRace]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedRace]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedRace]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedSchoolFoodServicesEligibilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedSchoolFoodServicesEligibility]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedSchoolFoodServicesEligibility]  AS Tid
        ,@Msg +  'SchoolFoodServicesEligibilityDescriptorCodeValue = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SchoolFoodServicesEligibilityDescriptorNamespace = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedSchoolFoodServicesEligibilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedSchoolFoodServicesEligibilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedSchoolFoodServicesEligibility]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedSchoolFoodServicesEligibility]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedSchoolFoodServicesEligibilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedLanguage]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedLanguage]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedLanguage]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedByDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedByDisability]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedByDisability]  AS Tid
        ,@Msg +  'DisabilityDescriptorCodeValue = ' + COALESCE(CAST(P.DisabilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisabilityDescriptorNamespace = ' + COALESCE(CAST(P.DisabilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedByDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedByDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedByDisability]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedByDisability]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedByDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedDisabilityTotalStudentsDisabledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedDisabilityTotalStudentsDisabled]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedDisabilityTotalStudentsDisabled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedDisabilityTotalStudentsDisabledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedELLEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedELLEnrollment]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedELLEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedELLEnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedESLEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedESLEnrollment]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedESLEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedESLEnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedSection504EnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedSection504Enrollment]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedSection504Enrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedSection504EnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedSPEDSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedSPED]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedSPED]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedSPEDBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseStudentFactsAggregatedTitleIEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseStudentFactsAggregatedTitleIEnrollment]' AS DataTableName
        ,P.[tid_CourseStudentFactsAggregatedTitleIEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseStudentFactsAggregatedTitleIEnrollmentBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eCourseSurveyQuestionResponseFacts]******/

CREATE PROCEDURE [t_extension].[usp_eCourseSurveyQuestionResponseFacts]
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
        (SELECT '[t_extension].[CourseSurveyQuestionResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseSurveyQuestionResponseFacts]' AS DataTableName
        ,P.[tid_CourseSurveyQuestionResponseFacts]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseSurveyQuestionResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetCourseSurveyResponseFactsBySnapshotId](@Ids) F
        ON
        	P.[CourseCode] = F.[CourseCode]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        WHERE F.[CourseCode] IS NULL 
        AND P.[CourseCode] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CourseSurveyQuestionResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseSurveyQuestionResponseFacts]' AS DataTableName
        ,P.[tid_CourseSurveyQuestionResponseFacts]  AS Tid
        ,@Msg +  'QuestionCode = ' + COALESCE(CAST(P.QuestionCode AS NVARCHAR(128)), '') +  '|'+'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseSurveyQuestionResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyQuestionBySnapshotId](@Ids) F
        ON
        	P.[QuestionCode] = F.[QuestionCode]
        	AND P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[QuestionCode] IS NULL 
        AND P.[QuestionCode] IS NOT NULL
        AND F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eCourseSurveyResponseFacts]******/

CREATE PROCEDURE [t_extension].[usp_eCourseSurveyResponseFacts]
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
        (SELECT '[t_extension].[CourseSurveyResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseSurveyResponseFacts]' AS DataTableName
        ,P.[tid_CourseSurveyResponseFacts]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseSurveyResponseFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseSurveyResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseSurveyResponseFacts]' AS DataTableName
        ,P.[tid_CourseSurveyResponseFacts]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseSurveyResponseFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[CourseSurveyResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseSurveyResponseFacts]' AS DataTableName
        ,P.[tid_CourseSurveyResponseFacts]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseSurveyResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eCourseSurveySectionResponseRatingFacts]******/

CREATE PROCEDURE [t_extension].[usp_eCourseSurveySectionResponseRatingFacts]
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
        (SELECT '[t_extension].[CourseSurveySectionResponseRatingFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseSurveySectionResponseRatingFacts]' AS DataTableName
        ,P.[tid_CourseSurveySectionResponseRatingFacts]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseSurveySectionResponseRatingFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetCourseSurveyResponseFactsBySnapshotId](@Ids) F
        ON
        	P.[CourseCode] = F.[CourseCode]
        	AND P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        WHERE F.[CourseCode] IS NULL 
        AND P.[CourseCode] IS NOT NULL
        AND F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[CourseSurveySectionResponseRatingFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[CourseSurveySectionResponseRatingFacts]' AS DataTableName
        ,P.[tid_CourseSurveySectionResponseRatingFacts]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'SurveySectionTitle = ' + COALESCE(CAST(P.SurveySectionTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetCourseSurveySectionResponseRatingFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveySectionBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[SurveySectionTitle] = F.[SurveySectionTitle]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        AND F.[SurveySectionTitle] IS NULL 
        AND P.[SurveySectionTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eEducationOrganizationCourseTranscriptFacts]******/

CREATE PROCEDURE [t_extension].[usp_eEducationOrganizationCourseTranscriptFacts]
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
        (SELECT '[t_extension].[EducationOrganizationCourseTranscriptFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationCourseTranscriptFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationCourseTranscriptFacts]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationCourseTranscriptFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetEducationOrganizationStudentAcademicRecordFactsBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
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
        (SELECT '[t_extension].[EducationOrganizationCourseTranscriptFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationCourseTranscriptFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_EducationOrganizationCourseTranscriptFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationCourseTranscriptFactsStudentsEnrolledBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eEducationOrganizationFacts]******/

CREATE PROCEDURE [t_extension].[usp_eEducationOrganizationFacts]
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
        (SELECT '[t_extension].[EducationOrganizationFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationFacts]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationFacts]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationFactsVacanciesSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationFactsVacancies]' AS DataTableName
        ,P.[tid_EducationOrganizationFactsVacancies]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationFactsVacanciesBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationFactsVacanciesSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationFactsVacancies]' AS DataTableName
        ,P.[tid_EducationOrganizationFactsVacancies]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationFactsVacanciesBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eEducationOrganizationStudentAcademicRecordFacts]******/

CREATE PROCEDURE [t_extension].[usp_eEducationOrganizationStudentAcademicRecordFacts]
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
        (SELECT '[t_extension].[EducationOrganizationStudentAcademicRecordFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAcademicRecordFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAcademicRecordFacts]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAcademicRecordFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAcademicRecordFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAcademicRecordFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAcademicRecordFacts]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAcademicRecordFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAcademicRecordFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAcademicRecordFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAcademicRecordFacts]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAcademicRecordFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAcademicRecordFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAcademicRecordFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAcademicRecordFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAcademicRecordFactsStudentsEnrolledBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eEducationOrganizationStudentAssessmentFacts]******/

CREATE PROCEDURE [t_extension].[usp_eEducationOrganizationStudentAssessmentFacts]
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
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFacts]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFacts]  AS Tid
        ,@Msg +  'AssessmentCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFacts]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFacts]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFacts]  AS Tid
        ,@Msg +  'TakenSchoolYear = ' + COALESCE(CAST(P.TakenSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[TakenSchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[TakenSchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFacts]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsStudentsEnrolledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsAggregatedScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFactsAggregatedScoreResult]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFactsAggregatedScoreResult]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsAggregatedScoreResultBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsAggregatedScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFactsAggregatedScoreResult]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFactsAggregatedScoreResult]  AS Tid
        ,@Msg +  'AverageScoreResultDatatypeType = ' + COALESCE(CAST(P.AverageScoreResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsAggregatedScoreResultBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[AverageScoreResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AverageScoreResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[EducationOrganizationStudentAssessmentFactsAggregatedPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentAssessmentFactsAggregatedPerformanceLevel]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentAssessmentFactsAggregatedPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentAssessmentFactsAggregatedPerformanceLevelBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eEducationOrganizationStudentFacts]******/

CREATE PROCEDURE [t_extension].[usp_eEducationOrganizationStudentFacts]
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFacts]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsStudentsEnrolledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedSex]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedSex]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedSexBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedSex]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedSex]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedSexBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedGenderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedGender]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedGender]  AS Tid
        ,@Msg +  'GenderType = ' + COALESCE(CAST(P.GenderType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedGenderBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedGenderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedGender]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedGender]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedGenderBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedHispanicLatinoEthnicitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedHispanicLatinoEthnicity]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedHispanicLatinoEthnicity]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedHispanicLatinoEthnicityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedRace]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedRace]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedRace]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedSchoolFoodServicesEligibilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedSchoolFoodServicesEligibility]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedSchoolFoodServicesEligibility]  AS Tid
        ,@Msg +  'SchoolFoodServicesEligibilityDescriptorCodeValue = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SchoolFoodServicesEligibilityDescriptorNamespace = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedSchoolFoodServicesEligibilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedSchoolFoodServicesEligibilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedSchoolFoodServicesEligibility]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedSchoolFoodServicesEligibility]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedSchoolFoodServicesEligibilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedLanguage]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedLanguage]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedLanguage]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedByDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedByDisability]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedByDisability]  AS Tid
        ,@Msg +  'DisabilityDescriptorCodeValue = ' + COALESCE(CAST(P.DisabilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisabilityDescriptorNamespace = ' + COALESCE(CAST(P.DisabilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedByDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedByDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedByDisability]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedByDisability]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedByDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedDisabilityTotalStudentsDisabledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedDisabilityTotalStudentsDisabled]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedDisabilityTotalStudentsDisabled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedDisabilityTotalStudentsDisabledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedELLEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedELLEnrollment]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedELLEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedELLEnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedESLEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedESLEnrollment]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedESLEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedESLEnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedSection504EnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedSection504Enrollment]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedSection504Enrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedSection504EnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedSPEDSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedSPED]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedSPED]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedSPEDBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationStudentFactsAggregatedTitleIEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationStudentFactsAggregatedTitleIEnrollment]' AS DataTableName
        ,P.[tid_EducationOrganizationStudentFactsAggregatedTitleIEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationStudentFactsAggregatedTitleIEnrollmentBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eEducationOrganizationSurveyQuestionResponseFacts]******/

CREATE PROCEDURE [t_extension].[usp_eEducationOrganizationSurveyQuestionResponseFacts]
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
        (SELECT '[t_extension].[EducationOrganizationSurveyQuestionResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationSurveyQuestionResponseFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationSurveyQuestionResponseFacts]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationSurveyQuestionResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetEducationOrganizationSurveyResponseFactsBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[EducationOrganizationSurveyQuestionResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationSurveyQuestionResponseFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationSurveyQuestionResponseFacts]  AS Tid
        ,@Msg +  'QuestionCode = ' + COALESCE(CAST(P.QuestionCode AS NVARCHAR(128)), '') +  '|'+'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationSurveyQuestionResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyQuestionBySnapshotId](@Ids) F
        ON
        	P.[QuestionCode] = F.[QuestionCode]
        	AND P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[QuestionCode] IS NULL 
        AND P.[QuestionCode] IS NOT NULL
        AND F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eEducationOrganizationSurveyResponseFacts]******/

CREATE PROCEDURE [t_extension].[usp_eEducationOrganizationSurveyResponseFacts]
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
        (SELECT '[t_extension].[EducationOrganizationSurveyResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationSurveyResponseFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationSurveyResponseFacts]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationSurveyResponseFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationSurveyResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationSurveyResponseFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationSurveyResponseFacts]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationSurveyResponseFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[EducationOrganizationSurveyResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationSurveyResponseFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationSurveyResponseFacts]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationSurveyResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eEducationOrganizationSurveySectionResponseRatingFacts]******/

CREATE PROCEDURE [t_extension].[usp_eEducationOrganizationSurveySectionResponseRatingFacts]
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
        (SELECT '[t_extension].[EducationOrganizationSurveySectionResponseRatingFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationSurveySectionResponseRatingFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationSurveySectionResponseRatingFacts]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationSurveySectionResponseRatingFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetEducationOrganizationSurveyResponseFactsBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[EducationOrganizationSurveySectionResponseRatingFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[EducationOrganizationSurveySectionResponseRatingFacts]' AS DataTableName
        ,P.[tid_EducationOrganizationSurveySectionResponseRatingFacts]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'SurveySectionTitle = ' + COALESCE(CAST(P.SurveySectionTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetEducationOrganizationSurveySectionResponseRatingFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveySectionBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[SurveySectionTitle] = F.[SurveySectionTitle]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        AND F.[SurveySectionTitle] IS NULL 
        AND P.[SurveySectionTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eExternalCourseTranscript]******/

CREATE PROCEDURE [t_extension].[usp_eExternalCourseTranscript]
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'CareerPathwayType = ' + COALESCE(CAST(P.CareerPathwayType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'CourseEducationOrganizationId = ' + COALESCE(CAST(P.CourseEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'CourseAttemptResultType = ' + COALESCE(CAST(P.CourseAttemptResultType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'CourseRepeatCodeType = ' + COALESCE(CAST(P.CourseRepeatCodeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'AttemptedCreditType = ' + COALESCE(CAST(P.AttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'EarnedCreditType = ' + COALESCE(CAST(P.EarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'ExternalTerm = ' + COALESCE(CAST(P.ExternalTerm AS NVARCHAR(128)), '') +  '|'+'NameOfInstitution = ' + COALESCE(CAST(P.NameOfInstitution AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetExternalStudentAcademicRecordBySnapshotId](@Ids) F
        ON
        	P.[ExternalTerm] = F.[ExternalTerm]
        	AND P.[NameOfInstitution] = F.[NameOfInstitution]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[StudentUniqueId] = F.[StudentUniqueId]
        WHERE F.[ExternalTerm] IS NULL 
        AND P.[ExternalTerm] IS NOT NULL
        AND F.[NameOfInstitution] IS NULL 
        AND P.[NameOfInstitution] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'WhenTakenGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.WhenTakenGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'WhenTakenGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.WhenTakenGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'OfferedGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.OfferedGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'OfferedGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.OfferedGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetGradeLevelDescriptorBySnapshotId](@Ids) F
        ON
        	P.[OfferedGradeLevelDescriptorCodeValue] = F.[CodeValue]
        	AND P.[OfferedGradeLevelDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[OfferedGradeLevelDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[OfferedGradeLevelDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ExternalCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscript]' AS DataTableName
        ,P.[tid_ExternalCourseTranscript]  AS Tid
        ,@Msg +  'MethodCreditEarnedType = ' + COALESCE(CAST(P.MethodCreditEarnedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptEarnedAdditionalCreditsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscriptEarnedAdditionalCredits]' AS DataTableName
        ,P.[tid_ExternalCourseTranscriptEarnedAdditionalCredits]  AS Tid
        ,@Msg +  'AdditionalCreditType = ' + COALESCE(CAST(P.AdditionalCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptEarnedAdditionalCreditsBySnapshotId](@Ids) P
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
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ExternalCourseTranscriptCourseIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscriptCourseIdentificationCode]' AS DataTableName
        ,P.[tid_ExternalCourseTranscriptCourseIdentificationCode]  AS Tid
        ,@Msg +  'CourseIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.CourseIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CourseIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.CourseIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptCourseIdentificationCodeBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalCourseTranscriptCourseLevelCharacteristicSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalCourseTranscriptCourseLevelCharacteristic]' AS DataTableName
        ,P.[tid_ExternalCourseTranscriptCourseLevelCharacteristic]  AS Tid
        ,@Msg +  'CourseLevelCharacteristicType = ' + COALESCE(CAST(P.CourseLevelCharacteristicType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalCourseTranscriptCourseLevelCharacteristicBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eExternalEducationOrganization]******/

CREATE PROCEDURE [t_extension].[usp_eExternalEducationOrganization]
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
        (SELECT '[t_extension].[ExternalEducationOrganizationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalEducationOrganization]' AS DataTableName
        ,P.[tid_ExternalEducationOrganization]  AS Tid
        ,@Msg +  'OperationalStatusType = ' + COALESCE(CAST(P.OperationalStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalEducationOrganizationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalEducationOrganizationIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalEducationOrganizationIdentificationCode]' AS DataTableName
        ,P.[tid_ExternalEducationOrganizationIdentificationCode]  AS Tid
        ,@Msg +  'EducationOrganizationIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.EducationOrganizationIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.EducationOrganizationIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalEducationOrganizationIdentificationCodeBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalEducationOrganizationCategorySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalEducationOrganizationCategory]' AS DataTableName
        ,P.[tid_ExternalEducationOrganizationCategory]  AS Tid
        ,@Msg +  'EducationOrganizationCategoryType = ' + COALESCE(CAST(P.EducationOrganizationCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalEducationOrganizationCategoryBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalEducationOrganizationAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalEducationOrganizationAddress]' AS DataTableName
        ,P.[tid_ExternalEducationOrganizationAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalEducationOrganizationAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalEducationOrganizationAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalEducationOrganizationAddress]' AS DataTableName
        ,P.[tid_ExternalEducationOrganizationAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalEducationOrganizationAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalEducationOrganizationInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalEducationOrganizationInternationalAddress]' AS DataTableName
        ,P.[tid_ExternalEducationOrganizationInternationalAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalEducationOrganizationInternationalAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalEducationOrganizationInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalEducationOrganizationInternationalAddress]' AS DataTableName
        ,P.[tid_ExternalEducationOrganizationInternationalAddress]  AS Tid
        ,@Msg +  'CountryDescriptorCodeValue = ' + COALESCE(CAST(P.CountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CountryDescriptorNamespace = ' + COALESCE(CAST(P.CountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalEducationOrganizationInternationalAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalEducationOrganizationInstitutionTelephoneSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalEducationOrganizationInstitutionTelephone]' AS DataTableName
        ,P.[tid_ExternalEducationOrganizationInstitutionTelephone]  AS Tid
        ,@Msg +  'InstitutionTelephoneNumberType = ' + COALESCE(CAST(P.InstitutionTelephoneNumberType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalEducationOrganizationInstitutionTelephoneBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eExternalStudentAcademicRecord]******/

CREATE PROCEDURE [t_extension].[usp_eExternalStudentAcademicRecord]
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecord]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecord]  AS Tid
        ,@Msg +  'CumulativeEarnedCreditType = ' + COALESCE(CAST(P.CumulativeEarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecord]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecord]  AS Tid
        ,@Msg +  'CumulativeAttemptedCreditType = ' + COALESCE(CAST(P.CumulativeAttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecord]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecord]  AS Tid
        ,@Msg +  'SessionEarnedCreditType = ' + COALESCE(CAST(P.SessionEarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecord]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecord]  AS Tid
        ,@Msg +  'SessionAttemptedCreditType = ' + COALESCE(CAST(P.SessionAttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecord]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecord]  AS Tid
        ,@Msg +  'NameOfInstitution = ' + COALESCE(CAST(P.NameOfInstitution AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetExternalEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[NameOfInstitution] = F.[NameOfInstitution]
        WHERE F.[NameOfInstitution] IS NULL 
        AND P.[NameOfInstitution] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ExternalStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecord]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecord]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecord]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecord]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordAcademicHonorSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecordAcademicHonor]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecordAcademicHonor]  AS Tid
        ,@Msg +  'AcademicHonorCategoryType = ' + COALESCE(CAST(P.AcademicHonorCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordAcademicHonorBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordAcademicHonorSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecordAcademicHonor]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecordAcademicHonor]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordAcademicHonorBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordRecognitionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecordRecognition]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecordRecognition]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordRecognitionBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordRecognitionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecordRecognition]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecordRecognition]  AS Tid
        ,@Msg +  'RecognitionType = ' + COALESCE(CAST(P.RecognitionType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordRecognitionBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecordDiploma]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordDiplomaBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecordDiploma]  AS Tid
        ,@Msg +  'DiplomaLevelType = ' + COALESCE(CAST(P.DiplomaLevelType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordDiplomaBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ExternalStudentAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ExternalStudentAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_ExternalStudentAcademicRecordDiploma]  AS Tid
        ,@Msg +  'DiplomaType = ' + COALESCE(CAST(P.DiplomaType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetExternalStudentAcademicRecordDiplomaBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_ePerformanceMeasure]******/

CREATE PROCEDURE [t_extension].[usp_ePerformanceMeasure]
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
        (SELECT '[t_extension].[PerformanceMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasure]' AS DataTableName
        ,P.[tid_PerformanceMeasure]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[PerformanceMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasure]' AS DataTableName
        ,P.[tid_PerformanceMeasure]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[PerformanceMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasure]' AS DataTableName
        ,P.[tid_PerformanceMeasure]  AS Tid
        ,@Msg +  'PerformanceMeasureTypeDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceMeasureTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceMeasureTypeDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceMeasureTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetPerformanceMeasureTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PerformanceMeasureTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PerformanceMeasureTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PerformanceMeasureTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PerformanceMeasureTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[PerformanceMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasure]' AS DataTableName
        ,P.[tid_PerformanceMeasure]  AS Tid
        ,@Msg +  'RubricEducationOrganizationId = ' + COALESCE(CAST(P.RubricEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'RubricTitle = ' + COALESCE(CAST(P.RubricTitle AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorCodeValue = ' + COALESCE(CAST(P.RubricTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorNamespace = ' + COALESCE(CAST(P.RubricTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetRubricBySnapshotId](@Ids) F
        ON
        	P.[RubricEducationOrganizationId] = F.[RubricEducationOrganizationId]
        	AND P.[RubricTitle] = F.[RubricTitle]
        	AND P.[RubricTypeDescriptorCodeValue] = F.[RubricTypeDescriptorCodeValue]
        	AND P.[RubricTypeDescriptorNamespace] = F.[RubricTypeDescriptorNamespace]
        WHERE F.[RubricEducationOrganizationId] IS NULL 
        AND P.[RubricEducationOrganizationId] IS NOT NULL
        AND F.[RubricTitle] IS NULL 
        AND P.[RubricTitle] IS NOT NULL
        AND F.[RubricTypeDescriptorCodeValue] IS NULL 
        AND P.[RubricTypeDescriptorCodeValue] IS NOT NULL
        AND F.[RubricTypeDescriptorNamespace] IS NULL 
        AND P.[RubricTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[PerformanceMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasure]' AS DataTableName
        ,P.[tid_PerformanceMeasure]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[PerformanceMeasureProgramGatewaySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasureProgramGateway]' AS DataTableName
        ,P.[tid_PerformanceMeasureProgramGateway]  AS Tid
        ,@Msg +  'ProgramGatewayDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramGatewayDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramGatewayDescriptorNamespace = ' + COALESCE(CAST(P.ProgramGatewayDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureProgramGatewayBySnapshotId](@Ids) P
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
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[PerformanceMeasureGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasureGradeLevel]' AS DataTableName
        ,P.[tid_PerformanceMeasureGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureGradeLevelBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[PerformanceMeasurePersonBeingReviewedSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasurePersonBeingReviewed]' AS DataTableName
        ,P.[tid_PerformanceMeasurePersonBeingReviewed]  AS Tid
        ,@Msg +  'ProspectEducationOrganizationId = ' + COALESCE(CAST(P.ProspectEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProspectIdentifier = ' + COALESCE(CAST(P.ProspectIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasurePersonBeingReviewedBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProspectBySnapshotId](@Ids) F
        ON
        	P.[ProspectEducationOrganizationId] = F.[ProspectEducationOrganizationId]
        	AND P.[ProspectIdentifier] = F.[ProspectIdentifier]
        WHERE F.[ProspectEducationOrganizationId] IS NULL 
        AND P.[ProspectEducationOrganizationId] IS NOT NULL
        AND F.[ProspectIdentifier] IS NULL 
        AND P.[ProspectIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[PerformanceMeasurePersonBeingReviewedSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasurePersonBeingReviewed]' AS DataTableName
        ,P.[tid_PerformanceMeasurePersonBeingReviewed]  AS Tid
        ,@Msg +  'PersonBeingReviewedStaffUniqueId = ' + COALESCE(CAST(P.PersonBeingReviewedStaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasurePersonBeingReviewedBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[PersonBeingReviewedStaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[PersonBeingReviewedStaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[PerformanceMeasurePersonBeingReviewedSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasurePersonBeingReviewed]' AS DataTableName
        ,P.[tid_PerformanceMeasurePersonBeingReviewed]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasurePersonBeingReviewedBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[PerformanceMeasureReviewerSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasureReviewer]' AS DataTableName
        ,P.[tid_PerformanceMeasureReviewer]  AS Tid
        ,@Msg +  'ReviewerStaffUniqueId = ' + COALESCE(CAST(P.ReviewerStaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureReviewerBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetStaffBySnapshotId](@Ids) F
        ON
        	P.[ReviewerStaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[StaffUniqueId] IS NULL 
        AND P.[ReviewerStaffUniqueId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_ePerformanceMeasureFacts]******/

CREATE PROCEDURE [t_extension].[usp_ePerformanceMeasureFacts]
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
        (SELECT '[t_extension].[PerformanceMeasureFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasureFacts]' AS DataTableName
        ,P.[tid_PerformanceMeasureFacts]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[PerformanceMeasureFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasureFacts]' AS DataTableName
        ,P.[tid_PerformanceMeasureFacts]  AS Tid
        ,@Msg +  'PerformanceMeasureTypeDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceMeasureTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceMeasureTypeDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceMeasureTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetPerformanceMeasureTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PerformanceMeasureTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PerformanceMeasureTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PerformanceMeasureTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PerformanceMeasureTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[PerformanceMeasureFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasureFacts]' AS DataTableName
        ,P.[tid_PerformanceMeasureFacts]  AS Tid
        ,@Msg +  'RubricEducationOrganizationId = ' + COALESCE(CAST(P.RubricEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'RubricTitle = ' + COALESCE(CAST(P.RubricTitle AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorCodeValue = ' + COALESCE(CAST(P.RubricTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorNamespace = ' + COALESCE(CAST(P.RubricTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetRubricBySnapshotId](@Ids) F
        ON
        	P.[RubricEducationOrganizationId] = F.[RubricEducationOrganizationId]
        	AND P.[RubricTitle] = F.[RubricTitle]
        	AND P.[RubricTypeDescriptorCodeValue] = F.[RubricTypeDescriptorCodeValue]
        	AND P.[RubricTypeDescriptorNamespace] = F.[RubricTypeDescriptorNamespace]
        WHERE F.[RubricEducationOrganizationId] IS NULL 
        AND P.[RubricEducationOrganizationId] IS NOT NULL
        AND F.[RubricTitle] IS NULL 
        AND P.[RubricTitle] IS NOT NULL
        AND F.[RubricTypeDescriptorCodeValue] IS NULL 
        AND P.[RubricTypeDescriptorCodeValue] IS NOT NULL
        AND F.[RubricTypeDescriptorNamespace] IS NULL 
        AND P.[RubricTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[PerformanceMeasureFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasureFacts]' AS DataTableName
        ,P.[tid_PerformanceMeasureFacts]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[PerformanceMeasureFactsGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[PerformanceMeasureFactsGradeLevel]' AS DataTableName
        ,P.[tid_PerformanceMeasureFactsGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetPerformanceMeasureFactsGradeLevelBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eProfessionalDevelopmentEvent]******/

CREATE PROCEDURE [t_extension].[usp_eProfessionalDevelopmentEvent]
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
        (SELECT '[t_extension].[ProfessionalDevelopmentEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProfessionalDevelopmentEvent]' AS DataTableName
        ,P.[tid_ProfessionalDevelopmentEvent]  AS Tid
        ,@Msg +  'ProfessionalDevelopmentOfferedByDescriptorCodeValue = ' + COALESCE(CAST(P.ProfessionalDevelopmentOfferedByDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProfessionalDevelopmentOfferedByDescriptorNamespace = ' + COALESCE(CAST(P.ProfessionalDevelopmentOfferedByDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProfessionalDevelopmentEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProfessionalDevelopmentOfferedByDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ProfessionalDevelopmentOfferedByDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ProfessionalDevelopmentOfferedByDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ProfessionalDevelopmentOfferedByDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ProfessionalDevelopmentOfferedByDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eProspect]******/

CREATE PROCEDURE [t_extension].[usp_eProspect]
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
        (SELECT '[t_extension].[ProspectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Prospect]' AS DataTableName
        ,P.[tid_Prospect]  AS Tid
        ,@Msg +  'ProspectEducationOrganizationId = ' + COALESCE(CAST(P.ProspectEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[ProspectEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[ProspectEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ProspectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Prospect]' AS DataTableName
        ,P.[tid_Prospect]  AS Tid
        ,@Msg +  'GenderType = ' + COALESCE(CAST(P.GenderType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Prospect]' AS DataTableName
        ,P.[tid_Prospect]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectCurrentPositionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectCurrentPosition]' AS DataTableName
        ,P.[tid_ProspectCurrentPosition]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectCurrentPositionBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectCurrentPositionGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectCurrentPositionGradeLevel]' AS DataTableName
        ,P.[tid_ProspectCurrentPositionGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectCurrentPositionGradeLevelBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectRace]' AS DataTableName
        ,P.[tid_ProspectRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectRecruitmentEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectRecruitmentEvent]' AS DataTableName
        ,P.[tid_ProspectRecruitmentEvent]  AS Tid
        ,@Msg +  'EventDate = ' + COALESCE(CAST(P.EventDate AS NVARCHAR(128)), '') +  '|'+'EventTitle = ' + COALESCE(CAST(P.EventTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectRecruitmentEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetRecruitmentEventBySnapshotId](@Ids) F
        ON
        	P.[EventDate] = F.[EventDate]
        	AND P.[EventTitle] = F.[EventTitle]
        WHERE F.[EventDate] IS NULL 
        AND P.[EventDate] IS NOT NULL
        AND F.[EventTitle] IS NULL 
        AND P.[EventTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ProspectTelephoneSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectTelephone]' AS DataTableName
        ,P.[tid_ProspectTelephone]  AS Tid
        ,@Msg +  'TelephoneNumberType = ' + COALESCE(CAST(P.TelephoneNumberType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectTelephoneBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectCredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectCredential]' AS DataTableName
        ,P.[tid_ProspectCredential]  AS Tid
        ,@Msg +  'CredentialIdentifier = ' + COALESCE(CAST(P.CredentialIdentifier AS NVARCHAR(128)), '') +  '|'+'StateOfIssueStateAbbreviationType = ' + COALESCE(CAST(P.StateOfIssueStateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectCredentialBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectDisability]' AS DataTableName
        ,P.[tid_ProspectDisability]  AS Tid
        ,@Msg +  'DisabilityDescriptorCodeValue = ' + COALESCE(CAST(P.DisabilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisabilityDescriptorNamespace = ' + COALESCE(CAST(P.DisabilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectDisability]' AS DataTableName
        ,P.[tid_ProspectDisability]  AS Tid
        ,@Msg +  'DisabilityDeterminationSourceType = ' + COALESCE(CAST(P.DisabilityDeterminationSourceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectIdentificationDocument]' AS DataTableName
        ,P.[tid_ProspectIdentificationDocument]  AS Tid
        ,@Msg +  'IssuerCountryDescriptorCodeValue = ' + COALESCE(CAST(P.IssuerCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'IssuerCountryDescriptorNamespace = ' + COALESCE(CAST(P.IssuerCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectIdentificationDocumentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectIdentificationDocument]' AS DataTableName
        ,P.[tid_ProspectIdentificationDocument]  AS Tid
        ,@Msg +  'IdentificationDocumentUseType = ' + COALESCE(CAST(P.IdentificationDocumentUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectIdentificationDocumentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectIdentificationDocument]' AS DataTableName
        ,P.[tid_ProspectIdentificationDocument]  AS Tid
        ,@Msg +  'PersonalInformationVerificationType = ' + COALESCE(CAST(P.PersonalInformationVerificationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectIdentificationDocumentBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eProspectProfessionalDevelopmentEventAttendance]******/

CREATE PROCEDURE [t_extension].[usp_eProspectProfessionalDevelopmentEventAttendance]
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
        (SELECT '[t_extension].[ProspectProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_ProspectProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'AttendanceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AttendanceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[ProspectProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_ProspectProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'ProfessionalDevelopmentTitle = ' + COALESCE(CAST(P.ProfessionalDevelopmentTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProfessionalDevelopmentEventBySnapshotId](@Ids) F
        ON
        	P.[ProfessionalDevelopmentTitle] = F.[ProfessionalDevelopmentTitle]
        WHERE F.[ProfessionalDevelopmentTitle] IS NULL 
        AND P.[ProfessionalDevelopmentTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[ProspectProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[ProspectProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_ProspectProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'ProspectEducationOrganizationId = ' + COALESCE(CAST(P.ProspectEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProspectIdentifier = ' + COALESCE(CAST(P.ProspectIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetProspectProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProspectBySnapshotId](@Ids) F
        ON
        	P.[ProspectEducationOrganizationId] = F.[ProspectEducationOrganizationId]
        	AND P.[ProspectIdentifier] = F.[ProspectIdentifier]
        WHERE F.[ProspectEducationOrganizationId] IS NULL 
        AND P.[ProspectEducationOrganizationId] IS NOT NULL
        AND F.[ProspectIdentifier] IS NULL 
        AND P.[ProspectIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eRubric]******/

CREATE PROCEDURE [t_extension].[usp_eRubric]
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
        (SELECT '[t_extension].[RubricSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Rubric]' AS DataTableName
        ,P.[tid_Rubric]  AS Tid
        ,@Msg +  'RubricEducationOrganizationId = ' + COALESCE(CAST(P.RubricEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[RubricEducationOrganizationId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[RubricEducationOrganizationId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[RubricSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Rubric]' AS DataTableName
        ,P.[tid_Rubric]  AS Tid
        ,@Msg +  'RubricTypeDescriptorCodeValue = ' + COALESCE(CAST(P.RubricTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorNamespace = ' + COALESCE(CAST(P.RubricTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetRubricTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[RubricTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[RubricTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[RubricTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[RubricTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eRubricLevel]******/

CREATE PROCEDURE [t_extension].[usp_eRubricLevel]
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
        (SELECT '[t_extension].[RubricLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[RubricLevel]' AS DataTableName
        ,P.[tid_RubricLevel]  AS Tid
        ,@Msg +  'RubricEducationOrganizationId = ' + COALESCE(CAST(P.RubricEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'RubricTitle = ' + COALESCE(CAST(P.RubricTitle AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorCodeValue = ' + COALESCE(CAST(P.RubricTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorNamespace = ' + COALESCE(CAST(P.RubricTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricLevelBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetRubricBySnapshotId](@Ids) F
        ON
        	P.[RubricEducationOrganizationId] = F.[RubricEducationOrganizationId]
        	AND P.[RubricTitle] = F.[RubricTitle]
        	AND P.[RubricTypeDescriptorCodeValue] = F.[RubricTypeDescriptorCodeValue]
        	AND P.[RubricTypeDescriptorNamespace] = F.[RubricTypeDescriptorNamespace]
        WHERE F.[RubricEducationOrganizationId] IS NULL 
        AND P.[RubricEducationOrganizationId] IS NOT NULL
        AND F.[RubricTitle] IS NULL 
        AND P.[RubricTitle] IS NOT NULL
        AND F.[RubricTypeDescriptorCodeValue] IS NULL 
        AND P.[RubricTypeDescriptorCodeValue] IS NOT NULL
        AND F.[RubricTypeDescriptorNamespace] IS NULL 
        AND P.[RubricTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[RubricLevelInformationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[RubricLevelInformation]' AS DataTableName
        ,P.[tid_RubricLevelInformation]  AS Tid
        ,@Msg +  'LevelTypeDescriptorCodeValue = ' + COALESCE(CAST(P.LevelTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LevelTypeDescriptorNamespace = ' + COALESCE(CAST(P.LevelTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricLevelInformationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetLevelTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[LevelTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[LevelTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[LevelTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[LevelTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[RubricLevelThemeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[RubricLevelTheme]' AS DataTableName
        ,P.[tid_RubricLevelTheme]  AS Tid
        ,@Msg +  'ThemeDescriptorCodeValue = ' + COALESCE(CAST(P.ThemeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ThemeDescriptorNamespace = ' + COALESCE(CAST(P.ThemeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricLevelThemeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetThemeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[ThemeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[ThemeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[ThemeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[ThemeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eRubricLevelResponse]******/

CREATE PROCEDURE [t_extension].[usp_eRubricLevelResponse]
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
        (SELECT '[t_extension].[RubricLevelResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[RubricLevelResponse]' AS DataTableName
        ,P.[tid_RubricLevelResponse]  AS Tid
        ,@Msg +  'PerformanceMeasureIdentifier = ' + COALESCE(CAST(P.PerformanceMeasureIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricLevelResponseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetPerformanceMeasureBySnapshotId](@Ids) F
        ON
        	P.[PerformanceMeasureIdentifier] = F.[PerformanceMeasureIdentifier]
        WHERE F.[PerformanceMeasureIdentifier] IS NULL 
        AND P.[PerformanceMeasureIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[RubricLevelResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[RubricLevelResponse]' AS DataTableName
        ,P.[tid_RubricLevelResponse]  AS Tid
        ,@Msg +  'RubricEducationOrganizationId = ' + COALESCE(CAST(P.RubricEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'RubricLevelCode = ' + COALESCE(CAST(P.RubricLevelCode AS NVARCHAR(128)), '') +  '|'+'RubricTitle = ' + COALESCE(CAST(P.RubricTitle AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorCodeValue = ' + COALESCE(CAST(P.RubricTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorNamespace = ' + COALESCE(CAST(P.RubricTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricLevelResponseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetRubricLevelBySnapshotId](@Ids) F
        ON
        	P.[RubricEducationOrganizationId] = F.[RubricEducationOrganizationId]
        	AND P.[RubricLevelCode] = F.[RubricLevelCode]
        	AND P.[RubricTitle] = F.[RubricTitle]
        	AND P.[RubricTypeDescriptorCodeValue] = F.[RubricTypeDescriptorCodeValue]
        	AND P.[RubricTypeDescriptorNamespace] = F.[RubricTypeDescriptorNamespace]
        WHERE F.[RubricEducationOrganizationId] IS NULL 
        AND P.[RubricEducationOrganizationId] IS NOT NULL
        AND F.[RubricLevelCode] IS NULL 
        AND P.[RubricLevelCode] IS NOT NULL
        AND F.[RubricTitle] IS NULL 
        AND P.[RubricTitle] IS NOT NULL
        AND F.[RubricTypeDescriptorCodeValue] IS NULL 
        AND P.[RubricTypeDescriptorCodeValue] IS NOT NULL
        AND F.[RubricTypeDescriptorNamespace] IS NULL 
        AND P.[RubricTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eRubricLevelResponseFacts]******/

CREATE PROCEDURE [t_extension].[usp_eRubricLevelResponseFacts]
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
        (SELECT '[t_extension].[RubricLevelResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[RubricLevelResponseFacts]' AS DataTableName
        ,P.[tid_RubricLevelResponseFacts]  AS Tid
        ,@Msg +  'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'RubricEducationOrganizationId = ' + COALESCE(CAST(P.RubricEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'RubricTitle = ' + COALESCE(CAST(P.RubricTitle AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorCodeValue = ' + COALESCE(CAST(P.RubricTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorNamespace = ' + COALESCE(CAST(P.RubricTypeDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricLevelResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetPerformanceMeasureFactsBySnapshotId](@Ids) F
        ON
        	P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[RubricEducationOrganizationId] = F.[RubricEducationOrganizationId]
        	AND P.[RubricTitle] = F.[RubricTitle]
        	AND P.[RubricTypeDescriptorCodeValue] = F.[RubricTypeDescriptorCodeValue]
        	AND P.[RubricTypeDescriptorNamespace] = F.[RubricTypeDescriptorNamespace]
        	AND P.[SchoolYear] = F.[SchoolYear]
        WHERE F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[RubricEducationOrganizationId] IS NULL 
        AND P.[RubricEducationOrganizationId] IS NOT NULL
        AND F.[RubricTitle] IS NULL 
        AND P.[RubricTitle] IS NOT NULL
        AND F.[RubricTypeDescriptorCodeValue] IS NULL 
        AND P.[RubricTypeDescriptorCodeValue] IS NOT NULL
        AND F.[RubricTypeDescriptorNamespace] IS NULL 
        AND P.[RubricTypeDescriptorNamespace] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
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
        (SELECT '[t_extension].[RubricLevelResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[RubricLevelResponseFacts]' AS DataTableName
        ,P.[tid_RubricLevelResponseFacts]  AS Tid
        ,@Msg +  'RubricEducationOrganizationId = ' + COALESCE(CAST(P.RubricEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'RubricLevelCode = ' + COALESCE(CAST(P.RubricLevelCode AS NVARCHAR(128)), '') +  '|'+'RubricTitle = ' + COALESCE(CAST(P.RubricTitle AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorCodeValue = ' + COALESCE(CAST(P.RubricTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'RubricTypeDescriptorNamespace = ' + COALESCE(CAST(P.RubricTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetRubricLevelResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetRubricLevelBySnapshotId](@Ids) F
        ON
        	P.[RubricEducationOrganizationId] = F.[RubricEducationOrganizationId]
        	AND P.[RubricLevelCode] = F.[RubricLevelCode]
        	AND P.[RubricTitle] = F.[RubricTitle]
        	AND P.[RubricTypeDescriptorCodeValue] = F.[RubricTypeDescriptorCodeValue]
        	AND P.[RubricTypeDescriptorNamespace] = F.[RubricTypeDescriptorNamespace]
        WHERE F.[RubricEducationOrganizationId] IS NULL 
        AND P.[RubricEducationOrganizationId] IS NOT NULL
        AND F.[RubricLevelCode] IS NULL 
        AND P.[RubricLevelCode] IS NOT NULL
        AND F.[RubricTitle] IS NULL 
        AND P.[RubricTitle] IS NOT NULL
        AND F.[RubricTypeDescriptorCodeValue] IS NULL 
        AND P.[RubricTypeDescriptorCodeValue] IS NOT NULL
        AND F.[RubricTypeDescriptorNamespace] IS NULL 
        AND P.[RubricTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eSchoolStudentGrowthMeasure]******/

CREATE PROCEDURE [t_extension].[usp_eSchoolStudentGrowthMeasure]
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
        (SELECT '[t_extension].[SchoolStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SchoolStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_SchoolStudentGrowthMeasure]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSchoolStudentGrowthMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SchoolStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SchoolStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_SchoolStudentGrowthMeasure]  AS Tid
        ,@Msg +  'PopulationCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.PopulationCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PopulationCategoryDescriptorNamespace = ' + COALESCE(CAST(P.PopulationCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSchoolStudentGrowthMeasureBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetPopulationCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PopulationCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PopulationCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PopulationCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PopulationCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SchoolStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SchoolStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_SchoolStudentGrowthMeasure]  AS Tid
        ,@Msg +  'PopulationSubCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.PopulationSubCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PopulationSubCategoryDescriptorNamespace = ' + COALESCE(CAST(P.PopulationSubCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSchoolStudentGrowthMeasureBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetPopulationSubCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PopulationSubCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PopulationSubCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PopulationSubCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PopulationSubCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SchoolStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SchoolStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_SchoolStudentGrowthMeasure]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSchoolStudentGrowthMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SchoolStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SchoolStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_SchoolStudentGrowthMeasure]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSchoolStudentGrowthMeasureBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eSectionCourseTranscriptFacts]******/

CREATE PROCEDURE [t_extension].[usp_eSectionCourseTranscriptFacts]
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
        (SELECT '[t_extension].[SectionCourseTranscriptFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionCourseTranscriptFacts]' AS DataTableName
        ,P.[tid_SectionCourseTranscriptFacts]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionCourseTranscriptFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSectionStudentAcademicRecordFactsBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[FactAsOfDate] = F.[FactAsOfDate]
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
        AND F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
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
        (SELECT '[t_extension].[SectionCourseTranscriptFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionCourseTranscriptFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_SectionCourseTranscriptFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionCourseTranscriptFactsStudentsEnrolledBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eSectionStudentAcademicRecordFacts]******/

CREATE PROCEDURE [t_extension].[usp_eSectionStudentAcademicRecordFacts]
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
        (SELECT '[t_extension].[SectionStudentAcademicRecordFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAcademicRecordFacts]' AS DataTableName
        ,P.[tid_SectionStudentAcademicRecordFacts]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAcademicRecordFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentAcademicRecordFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAcademicRecordFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_SectionStudentAcademicRecordFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAcademicRecordFactsStudentsEnrolledBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eSectionStudentAssessmentFacts]******/

CREATE PROCEDURE [t_extension].[usp_eSectionStudentAssessmentFacts]
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
        (SELECT '[t_extension].[SectionStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFacts]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFacts]  AS Tid
        ,@Msg +  'AssessmentCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AssessmentCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AssessmentCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFacts]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFacts]  AS Tid
        ,@Msg +  'TakenSchoolYear = ' + COALESCE(CAST(P.TakenSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolYearTypeBySnapshotId](@Ids) F
        ON
        	P.[TakenSchoolYear] = F.[SchoolYear]
        WHERE F.[SchoolYear] IS NULL 
        AND P.[TakenSchoolYear] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SectionStudentAssessmentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFacts]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFacts]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentAssessmentFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsStudentsEnrolledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentAssessmentFactsAggregatedScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFactsAggregatedScoreResult]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFactsAggregatedScoreResult]  AS Tid
        ,@Msg +  'AssessmentReportingMethodType = ' + COALESCE(CAST(P.AssessmentReportingMethodType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsAggregatedScoreResultBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentAssessmentFactsAggregatedScoreResultSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFactsAggregatedScoreResult]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFactsAggregatedScoreResult]  AS Tid
        ,@Msg +  'AverageScoreResultDatatypeType = ' + COALESCE(CAST(P.AverageScoreResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsAggregatedScoreResultBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ResultDatatypeType] F
        ON
        	P.[AverageScoreResultDatatypeType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[AverageScoreResultDatatypeType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SectionStudentAssessmentFactsAggregatedPerformanceLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentAssessmentFactsAggregatedPerformanceLevel]' AS DataTableName
        ,P.[tid_SectionStudentAssessmentFactsAggregatedPerformanceLevel]  AS Tid
        ,@Msg +  'PerformanceLevelDescriptorCodeValue = ' + COALESCE(CAST(P.PerformanceLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PerformanceLevelDescriptorNamespace = ' + COALESCE(CAST(P.PerformanceLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentAssessmentFactsAggregatedPerformanceLevelBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eSectionStudentFacts]******/

CREATE PROCEDURE [t_extension].[usp_eSectionStudentFacts]
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
        (SELECT '[t_extension].[SectionStudentFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFacts]' AS DataTableName
        ,P.[tid_SectionStudentFacts]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsStudentsEnrolledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsStudentsEnrolled]' AS DataTableName
        ,P.[tid_SectionStudentFactsStudentsEnrolled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsStudentsEnrolledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedSex]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedSex]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedSexBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedSexSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedSex]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedSex]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedSexBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedGenderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedGender]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedGender]  AS Tid
        ,@Msg +  'GenderType = ' + COALESCE(CAST(P.GenderType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedGenderBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedGenderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedGender]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedGender]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedGenderBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedHispanicLatinoEthnicitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedHispanicLatinoEthnicity]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedHispanicLatinoEthnicity]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedHispanicLatinoEthnicityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedRace]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedRace]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedRace]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedSchoolFoodServicesEligibilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedSchoolFoodServicesEligibility]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedSchoolFoodServicesEligibility]  AS Tid
        ,@Msg +  'SchoolFoodServicesEligibilityDescriptorCodeValue = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SchoolFoodServicesEligibilityDescriptorNamespace = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedSchoolFoodServicesEligibilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedSchoolFoodServicesEligibilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedSchoolFoodServicesEligibility]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedSchoolFoodServicesEligibility]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedSchoolFoodServicesEligibilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedLanguage]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedLanguage]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedLanguage]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedByDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedByDisability]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedByDisability]  AS Tid
        ,@Msg +  'DisabilityDescriptorCodeValue = ' + COALESCE(CAST(P.DisabilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisabilityDescriptorNamespace = ' + COALESCE(CAST(P.DisabilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedByDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedByDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedByDisability]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedByDisability]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedByDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedDisabilityTotalStudentsDisabledSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedDisabilityTotalStudentsDisabled]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedDisabilityTotalStudentsDisabled]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedDisabilityTotalStudentsDisabledBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedELLEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedELLEnrollment]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedELLEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedELLEnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedESLEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedESLEnrollment]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedESLEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedESLEnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedSection504EnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedSection504Enrollment]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedSection504Enrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedSection504EnrollmentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedSPEDSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedSPED]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedSPED]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedSPEDBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionStudentFactsAggregatedTitleIEnrollmentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionStudentFactsAggregatedTitleIEnrollment]' AS DataTableName
        ,P.[tid_SectionStudentFactsAggregatedTitleIEnrollment]  AS Tid
        ,@Msg +  'ValueType = ' + COALESCE(CAST(P.ValueType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionStudentFactsAggregatedTitleIEnrollmentBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eSectionSurveyQuestionResponseFacts]******/

CREATE PROCEDURE [t_extension].[usp_eSectionSurveyQuestionResponseFacts]
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
        (SELECT '[t_extension].[SectionSurveyQuestionResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionSurveyQuestionResponseFacts]' AS DataTableName
        ,P.[tid_SectionSurveyQuestionResponseFacts]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionSurveyQuestionResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSectionSurveyResponseFactsBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
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
        (SELECT '[t_extension].[SectionSurveyQuestionResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionSurveyQuestionResponseFacts]' AS DataTableName
        ,P.[tid_SectionSurveyQuestionResponseFacts]  AS Tid
        ,@Msg +  'QuestionCode = ' + COALESCE(CAST(P.QuestionCode AS NVARCHAR(128)), '') +  '|'+'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionSurveyQuestionResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyQuestionBySnapshotId](@Ids) F
        ON
        	P.[QuestionCode] = F.[QuestionCode]
        	AND P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[QuestionCode] IS NULL 
        AND P.[QuestionCode] IS NOT NULL
        AND F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eSectionSurveyResponseFacts]******/

CREATE PROCEDURE [t_extension].[usp_eSectionSurveyResponseFacts]
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
        (SELECT '[t_extension].[SectionSurveyResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionSurveyResponseFacts]' AS DataTableName
        ,P.[tid_SectionSurveyResponseFacts]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionSurveyResponseFactsBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SectionSurveyResponseFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionSurveyResponseFacts]' AS DataTableName
        ,P.[tid_SectionSurveyResponseFacts]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionSurveyResponseFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eSectionSurveySectionResponseRatingFacts]******/

CREATE PROCEDURE [t_extension].[usp_eSectionSurveySectionResponseRatingFacts]
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
        (SELECT '[t_extension].[SectionSurveySectionResponseRatingFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionSurveySectionResponseRatingFacts]' AS DataTableName
        ,P.[tid_SectionSurveySectionResponseRatingFacts]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'FactsAsOfDate = ' + COALESCE(CAST(P.FactsAsOfDate AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionSurveySectionResponseRatingFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSectionSurveyResponseFactsBySnapshotId](@Ids) F
        ON
        	P.[ClassPeriodName] = F.[ClassPeriodName]
        	AND P.[ClassroomIdentificationCode] = F.[ClassroomIdentificationCode]
        	AND P.[FactsAsOfDate] = F.[FactsAsOfDate]
        	AND P.[LocalCourseCode] = F.[LocalCourseCode]
        	AND P.[SchoolId] = F.[SchoolId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[SequenceOfCourse] = F.[SequenceOfCourse]
        	AND P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        	AND P.[UniqueSectionCode] = F.[UniqueSectionCode]
        WHERE F.[ClassPeriodName] IS NULL 
        AND P.[ClassPeriodName] IS NOT NULL
        AND F.[ClassroomIdentificationCode] IS NULL 
        AND P.[ClassroomIdentificationCode] IS NOT NULL
        AND F.[FactsAsOfDate] IS NULL 
        AND P.[FactsAsOfDate] IS NOT NULL
        AND F.[LocalCourseCode] IS NULL 
        AND P.[LocalCourseCode] IS NOT NULL
        AND F.[SchoolId] IS NULL 
        AND P.[SchoolId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[SequenceOfCourse] IS NULL 
        AND P.[SequenceOfCourse] IS NOT NULL
        AND F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
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
        (SELECT '[t_extension].[SectionSurveySectionResponseRatingFactsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SectionSurveySectionResponseRatingFacts]' AS DataTableName
        ,P.[tid_SectionSurveySectionResponseRatingFacts]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'SurveySectionTitle = ' + COALESCE(CAST(P.SurveySectionTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSectionSurveySectionResponseRatingFactsBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveySectionBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[SurveySectionTitle] = F.[SurveySectionTitle]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        AND F.[SurveySectionTitle] IS NULL 
        AND P.[SurveySectionTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eStaffFieldworkAbsenceEvent]******/

CREATE PROCEDURE [t_extension].[usp_eStaffFieldworkAbsenceEvent]
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
        (SELECT '[t_extension].[StaffFieldworkAbsenceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffFieldworkAbsenceEvent]' AS DataTableName
        ,P.[tid_StaffFieldworkAbsenceEvent]  AS Tid
        ,@Msg +  'AbsenceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AbsenceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AbsenceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AbsenceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffFieldworkAbsenceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAbsenceEventCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AbsenceEventCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AbsenceEventCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AbsenceEventCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AbsenceEventCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffFieldworkAbsenceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffFieldworkAbsenceEvent]' AS DataTableName
        ,P.[tid_StaffFieldworkAbsenceEvent]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffFieldworkAbsenceEventBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eStaffFieldworkExperience]******/

CREATE PROCEDURE [t_extension].[usp_eStaffFieldworkExperience]
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
        (SELECT '[t_extension].[StaffFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffFieldworkExperience]' AS DataTableName
        ,P.[tid_StaffFieldworkExperience]  AS Tid
        ,@Msg +  'FieldworkTypeDescriptorCodeValue = ' + COALESCE(CAST(P.FieldworkTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'FieldworkTypeDescriptorNamespace = ' + COALESCE(CAST(P.FieldworkTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffFieldworkExperienceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetFieldworkTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[FieldworkTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[FieldworkTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[FieldworkTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[FieldworkTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffFieldworkExperience]' AS DataTableName
        ,P.[tid_StaffFieldworkExperience]  AS Tid
        ,@Msg +  'ProgramGatewayDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramGatewayDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramGatewayDescriptorNamespace = ' + COALESCE(CAST(P.ProgramGatewayDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffFieldworkExperienceBySnapshotId](@Ids) P
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
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffFieldworkExperience]' AS DataTableName
        ,P.[tid_StaffFieldworkExperience]  AS Tid
        ,@Msg +  'FieldworkExperienceSchoolId = ' + COALESCE(CAST(P.FieldworkExperienceSchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffFieldworkExperienceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[FieldworkExperienceSchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[FieldworkExperienceSchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffFieldworkExperience]' AS DataTableName
        ,P.[tid_StaffFieldworkExperience]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffFieldworkExperienceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffFieldworkExperience]' AS DataTableName
        ,P.[tid_StaffFieldworkExperience]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffFieldworkExperienceBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eStaffProfessionalDevelopmentEventAttendance]******/

CREATE PROCEDURE [t_extension].[usp_eStaffProfessionalDevelopmentEventAttendance]
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
        (SELECT '[t_extension].[StaffProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_StaffProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'AttendanceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AttendanceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_StaffProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'ProfessionalDevelopmentTitle = ' + COALESCE(CAST(P.ProfessionalDevelopmentTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProfessionalDevelopmentEventBySnapshotId](@Ids) F
        ON
        	P.[ProfessionalDevelopmentTitle] = F.[ProfessionalDevelopmentTitle]
        WHERE F.[ProfessionalDevelopmentTitle] IS NULL 
        AND P.[ProfessionalDevelopmentTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_StaffProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eStaffStudentGrowthMeasure]******/

CREATE PROCEDURE [t_extension].[usp_eStaffStudentGrowthMeasure]
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasure]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasure]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasure]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasure]  AS Tid
        ,@Msg +  'StudentGrowthTypeDescriptorCodeValue = ' + COALESCE(CAST(P.StudentGrowthTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StudentGrowthTypeDescriptorNamespace = ' + COALESCE(CAST(P.StudentGrowthTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetStudentGrowthTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[StudentGrowthTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[StudentGrowthTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[StudentGrowthTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[StudentGrowthTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eStaffStudentGrowthMeasureCourseAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eStaffStudentGrowthMeasureCourseAssociation]
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureCourseAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasureCourseAssociation]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasureCourseAssociation]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureCourseAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureCourseAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasureCourseAssociation]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasureCourseAssociation]  AS Tid
        ,@Msg +  'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureCourseAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetStaffStudentGrowthMeasureBySnapshotId](@Ids) F
        ON
        	P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
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

/******Procedure [t_extension].[usp_eStaffStudentGrowthMeasureEducationOrganizationAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eStaffStudentGrowthMeasureEducationOrganizationAssociation]
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasureEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasureEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureEducationOrganizationAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasureEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasureEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureEducationOrganizationAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetStaffStudentGrowthMeasureBySnapshotId](@Ids) F
        ON
        	P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
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

/******Procedure [t_extension].[usp_eStaffStudentGrowthMeasureSectionAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eStaffStudentGrowthMeasureSectionAssociation]
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasureSectionAssociation]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasureSectionAssociation]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureSectionAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffStudentGrowthMeasureSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffStudentGrowthMeasureSectionAssociation]' AS DataTableName
        ,P.[tid_StaffStudentGrowthMeasureSectionAssociation]  AS Tid
        ,@Msg +  'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffStudentGrowthMeasureSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetStaffStudentGrowthMeasureBySnapshotId](@Ids) F
        ON
        	P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[StaffUniqueId] = F.[StaffUniqueId]
        WHERE F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
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

/******Procedure [t_extension].[usp_eStaffTeacherPreparationProviderAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eStaffTeacherPreparationProviderAssociation]
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
        (SELECT '[t_extension].[StaffTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'ProgramAssignmentDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramAssignmentDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramAssignmentDescriptorNamespace = ' + COALESCE(CAST(P.ProgramAssignmentDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'TeacherPreparationProviderId = ' + COALESCE(CAST(P.TeacherPreparationProviderId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherPreparationProviderBySnapshotId](@Ids) F
        ON
        	P.[TeacherPreparationProviderId] = F.[TeacherPreparationProviderId]
        WHERE F.[TeacherPreparationProviderId] IS NULL 
        AND P.[TeacherPreparationProviderId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[StaffTeacherPreparationProviderAssociationGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProviderAssociationGradeLevel]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProviderAssociationGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProviderAssociationGradeLevelBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffTeacherPreparationProviderAssociationAcademicSubjectSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProviderAssociationAcademicSubject]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProviderAssociationAcademicSubject]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProviderAssociationAcademicSubjectBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eStaffTeacherPreparationProviderProgramAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eStaffTeacherPreparationProviderProgramAssociation]
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
        (SELECT '[t_extension].[StaffTeacherPreparationProviderProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProviderProgramAssociation]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProviderProgramAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProviderProgramAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[StaffTeacherPreparationProviderProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[StaffTeacherPreparationProviderProgramAssociation]' AS DataTableName
        ,P.[tid_StaffTeacherPreparationProviderProgramAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetStaffTeacherPreparationProviderProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherPreparationProviderProgramBySnapshotId](@Ids) F
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

/******Procedure [t_extension].[usp_eSurvey]******/

CREATE PROCEDURE [t_extension].[usp_eSurvey]
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
        (SELECT '[t_extension].[SurveySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Survey]' AS DataTableName
        ,P.[tid_Survey]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Survey]' AS DataTableName
        ,P.[tid_Survey]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Survey]' AS DataTableName
        ,P.[tid_Survey]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Survey]' AS DataTableName
        ,P.[tid_Survey]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Survey]' AS DataTableName
        ,P.[tid_Survey]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Survey]' AS DataTableName
        ,P.[tid_Survey]  AS Tid
        ,@Msg +  'SurveyCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.SurveyCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SurveyCategoryDescriptorNamespace = ' + COALESCE(CAST(P.SurveyCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[SurveyCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[SurveyCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[SurveyCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[SurveyCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SurveySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[Survey]' AS DataTableName
        ,P.[tid_Survey]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eSurveyQuestion]******/

CREATE PROCEDURE [t_extension].[usp_eSurveyQuestion]
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
        (SELECT '[t_extension].[SurveyQuestionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyQuestion]' AS DataTableName
        ,P.[tid_SurveyQuestion]  AS Tid
        ,@Msg +  'QuestionFormType = ' + COALESCE(CAST(P.QuestionFormType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyQuestionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[QuestionFormType] F
        ON
        	P.[QuestionFormType] = F.[Description]
        WHERE F.[Description] IS NULL 
        AND P.[QuestionFormType] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SurveyQuestionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyQuestion]' AS DataTableName
        ,P.[tid_SurveyQuestion]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyQuestionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SurveyQuestionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyQuestion]' AS DataTableName
        ,P.[tid_SurveyQuestion]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'SurveySectionTitle = ' + COALESCE(CAST(P.SurveySectionTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyQuestionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveySectionBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[SurveySectionTitle] = F.[SurveySectionTitle]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        AND F.[SurveySectionTitle] IS NULL 
        AND P.[SurveySectionTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eSurveyQuestionResponse]******/

CREATE PROCEDURE [t_extension].[usp_eSurveyQuestionResponse]
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
        (SELECT '[t_extension].[SurveyQuestionResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyQuestionResponse]' AS DataTableName
        ,P.[tid_SurveyQuestionResponse]  AS Tid
        ,@Msg +  'QuestionCode = ' + COALESCE(CAST(P.QuestionCode AS NVARCHAR(128)), '') +  '|'+'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyQuestionResponseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyQuestionBySnapshotId](@Ids) F
        ON
        	P.[QuestionCode] = F.[QuestionCode]
        	AND P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[QuestionCode] IS NULL 
        AND P.[QuestionCode] IS NOT NULL
        AND F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SurveyQuestionResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyQuestionResponse]' AS DataTableName
        ,P.[tid_SurveyQuestionResponse]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'SurveyResponseIdentifier = ' + COALESCE(CAST(P.SurveyResponseIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyQuestionResponseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyResponseBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[SurveyResponseIdentifier] = F.[SurveyResponseIdentifier]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        AND F.[SurveyResponseIdentifier] IS NULL 
        AND P.[SurveyResponseIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eSurveyResponse]******/

CREATE PROCEDURE [t_extension].[usp_eSurveyResponse]
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
        (SELECT '[t_extension].[SurveyResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyResponse]' AS DataTableName
        ,P.[tid_SurveyResponse]  AS Tid
        ,@Msg +  'ApplicantIdentifier = ' + COALESCE(CAST(P.ApplicantIdentifier AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyResponseBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveyResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyResponse]' AS DataTableName
        ,P.[tid_SurveyResponse]  AS Tid
        ,@Msg +  'ParentUniqueId = ' + COALESCE(CAST(P.ParentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyResponseBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveyResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyResponse]' AS DataTableName
        ,P.[tid_SurveyResponse]  AS Tid
        ,@Msg +  'ProspectEducationOrganizationId = ' + COALESCE(CAST(P.ProspectEducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProspectIdentifier = ' + COALESCE(CAST(P.ProspectIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyResponseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProspectBySnapshotId](@Ids) F
        ON
        	P.[ProspectEducationOrganizationId] = F.[ProspectEducationOrganizationId]
        	AND P.[ProspectIdentifier] = F.[ProspectIdentifier]
        WHERE F.[ProspectEducationOrganizationId] IS NULL 
        AND P.[ProspectEducationOrganizationId] IS NOT NULL
        AND F.[ProspectIdentifier] IS NULL 
        AND P.[ProspectIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SurveyResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyResponse]' AS DataTableName
        ,P.[tid_SurveyResponse]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyResponseBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveyResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyResponse]' AS DataTableName
        ,P.[tid_SurveyResponse]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyResponseBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[SurveyResponseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveyResponse]' AS DataTableName
        ,P.[tid_SurveyResponse]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveyResponseBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eSurveySection]******/

CREATE PROCEDURE [t_extension].[usp_eSurveySection]
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
        (SELECT '[t_extension].[SurveySectionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveySection]' AS DataTableName
        ,P.[tid_SurveySection]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveySectionBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eSurveySectionResponseRating]******/

CREATE PROCEDURE [t_extension].[usp_eSurveySectionResponseRating]
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
        (SELECT '[t_extension].[SurveySectionResponseRatingSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveySectionResponseRating]' AS DataTableName
        ,P.[tid_SurveySectionResponseRating]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'SurveyResponseIdentifier = ' + COALESCE(CAST(P.SurveyResponseIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveySectionResponseRatingBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveyResponseBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[SurveyResponseIdentifier] = F.[SurveyResponseIdentifier]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        AND F.[SurveyResponseIdentifier] IS NULL 
        AND P.[SurveyResponseIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[SurveySectionResponseRatingSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[SurveySectionResponseRating]' AS DataTableName
        ,P.[tid_SurveySectionResponseRating]  AS Tid
        ,@Msg +  'SurveyIdentifier = ' + COALESCE(CAST(P.SurveyIdentifier AS NVARCHAR(128)), '') +  '|'+'SurveySectionTitle = ' + COALESCE(CAST(P.SurveySectionTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetSurveySectionResponseRatingBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetSurveySectionBySnapshotId](@Ids) F
        ON
        	P.[SurveyIdentifier] = F.[SurveyIdentifier]
        	AND P.[SurveySectionTitle] = F.[SurveySectionTitle]
        WHERE F.[SurveyIdentifier] IS NULL 
        AND P.[SurveyIdentifier] IS NOT NULL
        AND F.[SurveySectionTitle] IS NULL 
        AND P.[SurveySectionTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidate]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidate]
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'CitizenshipStatusType = ' + COALESCE(CAST(P.CitizenshipStatusType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'BirthCountryDescriptorCodeValue = ' + COALESCE(CAST(P.BirthCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BirthCountryDescriptorNamespace = ' + COALESCE(CAST(P.BirthCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'EnglishLanguageExamDescriptorCodeValue = ' + COALESCE(CAST(P.EnglishLanguageExamDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EnglishLanguageExamDescriptorNamespace = ' + COALESCE(CAST(P.EnglishLanguageExamDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetEnglishLanguageExamDescriptorBySnapshotId](@Ids) F
        ON
        	P.[EnglishLanguageExamDescriptorCodeValue] = F.[CodeValue]
        	AND P.[EnglishLanguageExamDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[EnglishLanguageExamDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[EnglishLanguageExamDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'GenderType = ' + COALESCE(CAST(P.GenderType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'LimitedEnglishProficiencyDescriptorCodeValue = ' + COALESCE(CAST(P.LimitedEnglishProficiencyDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LimitedEnglishProficiencyDescriptorNamespace = ' + COALESCE(CAST(P.LimitedEnglishProficiencyDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'OldEthnicityType = ' + COALESCE(CAST(P.OldEthnicityType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'PreviousCareerDescriptorCodeValue = ' + COALESCE(CAST(P.PreviousCareerDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'PreviousCareerDescriptorNamespace = ' + COALESCE(CAST(P.PreviousCareerDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetPreviousCareerDescriptorBySnapshotId](@Ids) F
        ON
        	P.[PreviousCareerDescriptorCodeValue] = F.[CodeValue]
        	AND P.[PreviousCareerDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[PreviousCareerDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[PreviousCareerDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'SchoolFoodServicesEligibilityDescriptorCodeValue = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'SchoolFoodServicesEligibilityDescriptorNamespace = ' + COALESCE(CAST(P.SchoolFoodServicesEligibilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'SexType = ' + COALESCE(CAST(P.SexType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'BirthStateAbbreviationType = ' + COALESCE(CAST(P.BirthStateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidate]' AS DataTableName
        ,P.[tid_TeacherCandidate]  AS Tid
        ,@Msg +  'StudentUniqueId = ' + COALESCE(CAST(P.StudentUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateIdentificationCodeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateIdentificationCode]' AS DataTableName
        ,P.[tid_TeacherCandidateIdentificationCode]  AS Tid
        ,@Msg +  'StudentIdentificationSystemDescriptorCodeValue = ' + COALESCE(CAST(P.StudentIdentificationSystemDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StudentIdentificationSystemDescriptorNamespace = ' + COALESCE(CAST(P.StudentIdentificationSystemDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateIdentificationCodeBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateOtherNameSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateOtherName]' AS DataTableName
        ,P.[tid_TeacherCandidateOtherName]  AS Tid
        ,@Msg +  'OtherNameType = ' + COALESCE(CAST(P.OtherNameType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateOtherNameBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAddress]' AS DataTableName
        ,P.[tid_TeacherCandidateAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAddress]' AS DataTableName
        ,P.[tid_TeacherCandidateAddress]  AS Tid
        ,@Msg +  'StateAbbreviationType = ' + COALESCE(CAST(P.StateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateInternationalAddress]' AS DataTableName
        ,P.[tid_TeacherCandidateInternationalAddress]  AS Tid
        ,@Msg +  'AddressType = ' + COALESCE(CAST(P.AddressType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateInternationalAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateInternationalAddressSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateInternationalAddress]' AS DataTableName
        ,P.[tid_TeacherCandidateInternationalAddress]  AS Tid
        ,@Msg +  'CountryDescriptorCodeValue = ' + COALESCE(CAST(P.CountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'CountryDescriptorNamespace = ' + COALESCE(CAST(P.CountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateInternationalAddressBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateTelephoneSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTelephone]' AS DataTableName
        ,P.[tid_TeacherCandidateTelephone]  AS Tid
        ,@Msg +  'TelephoneNumberType = ' + COALESCE(CAST(P.TelephoneNumberType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTelephoneBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateElectronicMailSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateElectronicMail]' AS DataTableName
        ,P.[tid_TeacherCandidateElectronicMail]  AS Tid
        ,@Msg +  'ElectronicMailType = ' + COALESCE(CAST(P.ElectronicMailType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateElectronicMailBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateRaceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateRace]' AS DataTableName
        ,P.[tid_TeacherCandidateRace]  AS Tid
        ,@Msg +  'RaceType = ' + COALESCE(CAST(P.RaceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateRaceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateVisaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateVisa]' AS DataTableName
        ,P.[tid_TeacherCandidateVisa]  AS Tid
        ,@Msg +  'VisaType = ' + COALESCE(CAST(P.VisaType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateVisaBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCharacteristicSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCharacteristic]' AS DataTableName
        ,P.[tid_TeacherCandidateCharacteristic]  AS Tid
        ,@Msg +  'StudentCharacteristicDescriptorCodeValue = ' + COALESCE(CAST(P.StudentCharacteristicDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StudentCharacteristicDescriptorNamespace = ' + COALESCE(CAST(P.StudentCharacteristicDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCharacteristicBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateLanguageSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateLanguage]' AS DataTableName
        ,P.[tid_TeacherCandidateLanguage]  AS Tid
        ,@Msg +  'LanguageDescriptorCodeValue = ' + COALESCE(CAST(P.LanguageDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'LanguageDescriptorNamespace = ' + COALESCE(CAST(P.LanguageDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateLanguageBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateLanguageUseSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateLanguageUse]' AS DataTableName
        ,P.[tid_TeacherCandidateLanguageUse]  AS Tid
        ,@Msg +  'LanguageUseType = ' + COALESCE(CAST(P.LanguageUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateLanguageUseBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateDisability]' AS DataTableName
        ,P.[tid_TeacherCandidateDisability]  AS Tid
        ,@Msg +  'DisabilityDescriptorCodeValue = ' + COALESCE(CAST(P.DisabilityDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'DisabilityDescriptorNamespace = ' + COALESCE(CAST(P.DisabilityDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateDisabilitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateDisability]' AS DataTableName
        ,P.[tid_TeacherCandidateDisability]  AS Tid
        ,@Msg +  'DisabilityDeterminationSourceType = ' + COALESCE(CAST(P.DisabilityDeterminationSourceType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateDisabilityBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateProgramParticipationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateProgramParticipation]' AS DataTableName
        ,P.[tid_TeacherCandidateProgramParticipation]  AS Tid
        ,@Msg +  'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateProgramParticipationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateProgramParticipationProgramCharacteristicSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateProgramParticipationProgramCharacteristic]' AS DataTableName
        ,P.[tid_TeacherCandidateProgramParticipationProgramCharacteristic]  AS Tid
        ,@Msg +  'ProgramCharacteristicDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramCharacteristicDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramCharacteristicDescriptorNamespace = ' + COALESCE(CAST(P.ProgramCharacteristicDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateProgramParticipationProgramCharacteristicBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCohortYearSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCohortYear]' AS DataTableName
        ,P.[tid_TeacherCandidateCohortYear]  AS Tid
        ,@Msg +  'CohortYearType = ' + COALESCE(CAST(P.CohortYearType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCohortYearBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCohortYearSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCohortYear]' AS DataTableName
        ,P.[tid_TeacherCandidateCohortYear]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCohortYearBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCredentialSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCredential]' AS DataTableName
        ,P.[tid_TeacherCandidateCredential]  AS Tid
        ,@Msg +  'CredentialIdentifier = ' + COALESCE(CAST(P.CredentialIdentifier AS NVARCHAR(128)), '') +  '|'+'StateOfIssueStateAbbreviationType = ' + COALESCE(CAST(P.StateOfIssueStateAbbreviationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCredentialBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAidSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAid]' AS DataTableName
        ,P.[tid_TeacherCandidateAid]  AS Tid
        ,@Msg +  'AidTypeDescriptorCodeValue = ' + COALESCE(CAST(P.AidTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AidTypeDescriptorNamespace = ' + COALESCE(CAST(P.AidTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAidBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAidTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AidTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AidTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AidTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AidTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateBackgroundCheckSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateBackgroundCheck]' AS DataTableName
        ,P.[tid_TeacherCandidateBackgroundCheck]  AS Tid
        ,@Msg +  'BackgroundCheckStatusDescriptorCodeValue = ' + COALESCE(CAST(P.BackgroundCheckStatusDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BackgroundCheckStatusDescriptorNamespace = ' + COALESCE(CAST(P.BackgroundCheckStatusDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBackgroundCheckBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateBackgroundCheckSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateBackgroundCheck]' AS DataTableName
        ,P.[tid_TeacherCandidateBackgroundCheck]  AS Tid
        ,@Msg +  'BackgroundCheckTypeDescriptorCodeValue = ' + COALESCE(CAST(P.BackgroundCheckTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'BackgroundCheckTypeDescriptorNamespace = ' + COALESCE(CAST(P.BackgroundCheckTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateBackgroundCheckBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateIdentificationDocument]' AS DataTableName
        ,P.[tid_TeacherCandidateIdentificationDocument]  AS Tid
        ,@Msg +  'IssuerCountryDescriptorCodeValue = ' + COALESCE(CAST(P.IssuerCountryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'IssuerCountryDescriptorNamespace = ' + COALESCE(CAST(P.IssuerCountryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateIdentificationDocumentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateIdentificationDocument]' AS DataTableName
        ,P.[tid_TeacherCandidateIdentificationDocument]  AS Tid
        ,@Msg +  'IdentificationDocumentUseType = ' + COALESCE(CAST(P.IdentificationDocumentUseType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateIdentificationDocumentBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateIdentificationDocumentSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateIdentificationDocument]' AS DataTableName
        ,P.[tid_TeacherCandidateIdentificationDocument]  AS Tid
        ,@Msg +  'PersonalInformationVerificationType = ' + COALESCE(CAST(P.PersonalInformationVerificationType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateIdentificationDocumentBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eTeacherCandidateAcademicRecord]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateAcademicRecord]
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'CumulativeEarnedCreditType = ' + COALESCE(CAST(P.CumulativeEarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'CumulativeAttemptedCreditType = ' + COALESCE(CAST(P.CumulativeAttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'SessionEarnedCreditType = ' + COALESCE(CAST(P.SessionEarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'SessionAttemptedCreditType = ' + COALESCE(CAST(P.SessionAttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'ProgramGatewayDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramGatewayDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramGatewayDescriptorNamespace = ' + COALESCE(CAST(P.ProgramGatewayDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
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
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecord]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecord]  AS Tid
        ,@Msg +  'TPPDegreeTypeDescriptorCodeValue = ' + COALESCE(CAST(P.TPPDegreeTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TPPDegreeTypeDescriptorNamespace = ' + COALESCE(CAST(P.TPPDegreeTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTPPDegreeTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[TPPDegreeTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[TPPDegreeTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[TPPDegreeTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[TPPDegreeTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordAcademicHonorSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecordAcademicHonor]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecordAcademicHonor]  AS Tid
        ,@Msg +  'AcademicHonorCategoryType = ' + COALESCE(CAST(P.AcademicHonorCategoryType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordAcademicHonorBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordAcademicHonorSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecordAcademicHonor]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecordAcademicHonor]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordAcademicHonorBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordRecognitionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecordRecognition]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecordRecognition]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordRecognitionBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordRecognitionSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecordRecognition]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecordRecognition]  AS Tid
        ,@Msg +  'RecognitionType = ' + COALESCE(CAST(P.RecognitionType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordRecognitionBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecordDiploma]  AS Tid
        ,@Msg +  'AchievementCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AchievementCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AchievementCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AchievementCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordDiplomaBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecordDiploma]  AS Tid
        ,@Msg +  'DiplomaLevelType = ' + COALESCE(CAST(P.DiplomaLevelType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordDiplomaBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateAcademicRecordDiplomaSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateAcademicRecordDiploma]' AS DataTableName
        ,P.[tid_TeacherCandidateAcademicRecordDiploma]  AS Tid
        ,@Msg +  'DiplomaType = ' + COALESCE(CAST(P.DiplomaType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateAcademicRecordDiplomaBySnapshotId](@Ids) P
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
             
        END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO

/******Procedure [t_extension].[usp_eTeacherCandidateCourseTranscript]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateCourseTranscript]
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'CourseEducationOrganizationId = ' + COALESCE(CAST(P.CourseEducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'CourseAttemptResultType = ' + COALESCE(CAST(P.CourseAttemptResultType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'CourseRepeatCodeType = ' + COALESCE(CAST(P.CourseRepeatCodeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'AttemptedCreditType = ' + COALESCE(CAST(P.AttemptedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'EarnedCreditType = ' + COALESCE(CAST(P.EarnedCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'WhenTakenGradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.WhenTakenGradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'WhenTakenGradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.WhenTakenGradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'MethodCreditEarnedType = ' + COALESCE(CAST(P.MethodCreditEarnedType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscript]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscript]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateAcademicRecordBySnapshotId](@Ids) F
        ON
        	P.[EducationOrganizationId] = F.[EducationOrganizationId]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        	AND P.[TermDescriptorCodeValue] = F.[TermDescriptorCodeValue]
        	AND P.[TermDescriptorNamespace] = F.[TermDescriptorNamespace]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[EducationOrganizationId] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
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
        (SELECT '[t_extension].[TeacherCandidateCourseTranscriptEarnedAdditionalCreditsSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateCourseTranscriptEarnedAdditionalCredits]' AS DataTableName
        ,P.[tid_TeacherCandidateCourseTranscriptEarnedAdditionalCredits]  AS Tid
        ,@Msg +  'AdditionalCreditType = ' + COALESCE(CAST(P.AdditionalCreditType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateCourseTranscriptEarnedAdditionalCreditsBySnapshotId](@Ids) P
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

/******Procedure [t_extension].[usp_eTeacherCandidateFieldworkAbsenceEvent]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateFieldworkAbsenceEvent]
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
        (SELECT '[t_extension].[TeacherCandidateFieldworkAbsenceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateFieldworkAbsenceEvent]' AS DataTableName
        ,P.[tid_TeacherCandidateFieldworkAbsenceEvent]  AS Tid
        ,@Msg +  'AbsenceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AbsenceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AbsenceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AbsenceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateFieldworkAbsenceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetAbsenceEventCategoryDescriptorBySnapshotId](@Ids) F
        ON
        	P.[AbsenceEventCategoryDescriptorCodeValue] = F.[CodeValue]
        	AND P.[AbsenceEventCategoryDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[AbsenceEventCategoryDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[AbsenceEventCategoryDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateFieldworkAbsenceEventSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateFieldworkAbsenceEvent]' AS DataTableName
        ,P.[tid_TeacherCandidateFieldworkAbsenceEvent]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateFieldworkAbsenceEventBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateFieldworkExperience]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateFieldworkExperience]
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
        (SELECT '[t_extension].[TeacherCandidateFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateFieldworkExperience]' AS DataTableName
        ,P.[tid_TeacherCandidateFieldworkExperience]  AS Tid
        ,@Msg +  'FieldworkTypeDescriptorCodeValue = ' + COALESCE(CAST(P.FieldworkTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'FieldworkTypeDescriptorNamespace = ' + COALESCE(CAST(P.FieldworkTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateFieldworkExperienceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetFieldworkTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[FieldworkTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[FieldworkTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[FieldworkTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[FieldworkTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateFieldworkExperience]' AS DataTableName
        ,P.[tid_TeacherCandidateFieldworkExperience]  AS Tid
        ,@Msg +  'ProgramGatewayDescriptorCodeValue = ' + COALESCE(CAST(P.ProgramGatewayDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ProgramGatewayDescriptorNamespace = ' + COALESCE(CAST(P.ProgramGatewayDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateFieldworkExperienceBySnapshotId](@Ids) P
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
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateFieldworkExperience]' AS DataTableName
        ,P.[tid_TeacherCandidateFieldworkExperience]  AS Tid
        ,@Msg +  'FieldworkExperienceSchoolId = ' + COALESCE(CAST(P.FieldworkExperienceSchoolId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateFieldworkExperienceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetSchoolBySnapshotId](@Ids) F
        ON
        	P.[FieldworkExperienceSchoolId] = F.[SchoolId]
        WHERE F.[SchoolId] IS NULL 
        AND P.[FieldworkExperienceSchoolId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateFieldworkExperience]' AS DataTableName
        ,P.[tid_TeacherCandidateFieldworkExperience]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateFieldworkExperienceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateFieldworkExperienceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateFieldworkExperience]' AS DataTableName
        ,P.[tid_TeacherCandidateFieldworkExperience]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateFieldworkExperienceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateProfessionalDevelopmentEventAttendance]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateProfessionalDevelopmentEventAttendance]
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
        (SELECT '[t_extension].[TeacherCandidateProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_TeacherCandidateProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'AttendanceEventCategoryDescriptorCodeValue = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AttendanceEventCategoryDescriptorNamespace = ' + COALESCE(CAST(P.AttendanceEventCategoryDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_TeacherCandidateProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'ProfessionalDevelopmentTitle = ' + COALESCE(CAST(P.ProfessionalDevelopmentTitle AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetProfessionalDevelopmentEventBySnapshotId](@Ids) F
        ON
        	P.[ProfessionalDevelopmentTitle] = F.[ProfessionalDevelopmentTitle]
        WHERE F.[ProfessionalDevelopmentTitle] IS NULL 
        AND P.[ProfessionalDevelopmentTitle] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateProfessionalDevelopmentEventAttendanceSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateProfessionalDevelopmentEventAttendance]' AS DataTableName
        ,P.[tid_TeacherCandidateProfessionalDevelopmentEventAttendance]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateProfessionalDevelopmentEventAttendanceBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateStaffAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateStaffAssociation]
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
        (SELECT '[t_extension].[TeacherCandidateStaffAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStaffAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateStaffAssociation]  AS Tid
        ,@Msg +  'StaffUniqueId = ' + COALESCE(CAST(P.StaffUniqueId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStaffAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateStaffAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStaffAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateStaffAssociation]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStaffAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateStudentGrowthMeasure]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateStudentGrowthMeasure]
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasure]  AS Tid
        ,@Msg +  'ResultDatatypeType = ' + COALESCE(CAST(P.ResultDatatypeType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasure]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasure]  AS Tid
        ,@Msg +  'StudentGrowthTypeDescriptorCodeValue = ' + COALESCE(CAST(P.StudentGrowthTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'StudentGrowthTypeDescriptorNamespace = ' + COALESCE(CAST(P.StudentGrowthTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetStudentGrowthTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[StudentGrowthTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[StudentGrowthTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[StudentGrowthTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[StudentGrowthTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasure]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasure]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureCourseAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureCourseAssociation]
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureCourseAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasureCourseAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasureCourseAssociation]  AS Tid
        ,@Msg +  'CourseCode = ' + COALESCE(CAST(P.CourseCode AS NVARCHAR(128)), '') +  '|'+'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureCourseAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureCourseAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasureCourseAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasureCourseAssociation]  AS Tid
        ,@Msg +  'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureCourseAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureBySnapshotId](@Ids) F
        ON
        	P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation]
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureEducationOrganizationAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureEducationOrganizationAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation]  AS Tid
        ,@Msg +  'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureEducationOrganizationAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureBySnapshotId](@Ids) F
        ON
        	P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureSectionAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureSectionAssociation]
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasureSectionAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasureSectionAssociation]  AS Tid
        ,@Msg +  'ClassPeriodName = ' + COALESCE(CAST(P.ClassPeriodName AS NVARCHAR(128)), '') +  '|'+'ClassroomIdentificationCode = ' + COALESCE(CAST(P.ClassroomIdentificationCode AS NVARCHAR(128)), '') +  '|'+'LocalCourseCode = ' + COALESCE(CAST(P.LocalCourseCode AS NVARCHAR(128)), '') +  '|'+'SchoolId = ' + COALESCE(CAST(P.SchoolId AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'SequenceOfCourse = ' + COALESCE(CAST(P.SequenceOfCourse AS NVARCHAR(128)), '') +  '|'+'TermDescriptorCodeValue = ' + COALESCE(CAST(P.TermDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TermDescriptorNamespace = ' + COALESCE(CAST(P.TermDescriptorNamespace AS NVARCHAR(128)), '') +  '|'+'UniqueSectionCode = ' + COALESCE(CAST(P.UniqueSectionCode AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureSectionAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateStudentGrowthMeasureSectionAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateStudentGrowthMeasureSectionAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateStudentGrowthMeasureSectionAssociation]  AS Tid
        ,@Msg +  'FactAsOfDate = ' + COALESCE(CAST(P.FactAsOfDate AS NVARCHAR(128)), '') +  '|'+'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') +  '|'+'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureSectionAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateStudentGrowthMeasureBySnapshotId](@Ids) F
        ON
        	P.[FactAsOfDate] = F.[FactAsOfDate]
        	AND P.[SchoolYear] = F.[SchoolYear]
        	AND P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[FactAsOfDate] IS NULL 
        AND P.[FactAsOfDate] IS NOT NULL
        AND F.[SchoolYear] IS NULL 
        AND P.[SchoolYear] IS NOT NULL
        AND F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateTeacherPreparationProviderAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateTeacherPreparationProviderAssociation]
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
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'EntryTypeDescriptorCodeValue = ' + COALESCE(CAST(P.EntryTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'EntryTypeDescriptorNamespace = ' + COALESCE(CAST(P.EntryTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'ExitWithdrawTypeDescriptorCodeValue = ' + COALESCE(CAST(P.ExitWithdrawTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ExitWithdrawTypeDescriptorNamespace = ' + COALESCE(CAST(P.ExitWithdrawTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'SchoolYear = ' + COALESCE(CAST(P.SchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'ClassOfSchoolYear = ' + COALESCE(CAST(P.ClassOfSchoolYear AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderAssociation]  AS Tid
        ,@Msg +  'TeacherPreparationProviderId = ' + COALESCE(CAST(P.TeacherPreparationProviderId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherPreparationProviderBySnapshotId](@Ids) F
        ON
        	P.[TeacherPreparationProviderId] = F.[TeacherPreparationProviderId]
        WHERE F.[TeacherPreparationProviderId] IS NULL 
        AND P.[TeacherPreparationProviderId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherCandidateTeacherPreparationProviderProgramAssociation]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherCandidateTeacherPreparationProviderProgramAssociation]
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
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderProgramAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderProgramAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderProgramAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderProgramAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderProgramAssociation]  AS Tid
        ,@Msg +  'ReasonExitedDescriptorCodeValue = ' + COALESCE(CAST(P.ReasonExitedDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'ReasonExitedDescriptorNamespace = ' + COALESCE(CAST(P.ReasonExitedDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderProgramAssociationBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderProgramAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderProgramAssociation]  AS Tid
        ,@Msg +  'TeacherCandidateIdentifier = ' + COALESCE(CAST(P.TeacherCandidateIdentifier AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherCandidateBySnapshotId](@Ids) F
        ON
        	P.[TeacherCandidateIdentifier] = F.[TeacherCandidateIdentifier]
        WHERE F.[TeacherCandidateIdentifier] IS NULL 
        AND P.[TeacherCandidateIdentifier] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherCandidateTeacherPreparationProviderProgramAssociationSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherCandidateTeacherPreparationProviderProgramAssociation]' AS DataTableName
        ,P.[tid_TeacherCandidateTeacherPreparationProviderProgramAssociation]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') +  '|'+'ProgramName = ' + COALESCE(CAST(P.ProgramName AS NVARCHAR(128)), '') +  '|'+'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherCandidateTeacherPreparationProviderProgramAssociationBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTeacherPreparationProviderProgramBySnapshotId](@Ids) F
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

/******Procedure [t_extension].[usp_eTeacherPreparationProvider]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherPreparationProvider]
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
        (SELECT '[t_extension].[TeacherPreparationProviderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProvider]' AS DataTableName
        ,P.[tid_TeacherPreparationProvider]  AS Tid
        ,@Msg +  'TeacherPreparationProviderId = ' + COALESCE(CAST(P.TeacherPreparationProviderId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[TeacherPreparationProviderId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[TeacherPreparationProviderId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherPreparationProviderSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProvider]' AS DataTableName
        ,P.[tid_TeacherPreparationProvider]  AS Tid
        ,@Msg +  'UniversityId = ' + COALESCE(CAST(P.UniversityId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetUniversityBySnapshotId](@Ids) F
        ON
        	P.[UniversityId] = F.[UniversityId]
        WHERE F.[UniversityId] IS NULL 
        AND P.[UniversityId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eTeacherPreparationProviderProgram]******/

CREATE PROCEDURE [t_extension].[usp_eTeacherPreparationProviderProgram]
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
        (SELECT '[t_extension].[TeacherPreparationProviderProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProviderProgram]' AS DataTableName
        ,P.[tid_TeacherPreparationProviderProgram]  AS Tid
        ,@Msg +  'EducationOrganizationId = ' + COALESCE(CAST(P.EducationOrganizationId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderProgramBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherPreparationProviderProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProviderProgram]' AS DataTableName
        ,P.[tid_TeacherPreparationProviderProgram]  AS Tid
        ,@Msg +  'ProgramType = ' + COALESCE(CAST(P.ProgramType AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderProgramBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherPreparationProviderProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProviderProgram]' AS DataTableName
        ,P.[tid_TeacherPreparationProviderProgram]  AS Tid
        ,@Msg +  'TeacherPreparationProgramTypeDescriptorCodeValue = ' + COALESCE(CAST(P.TeacherPreparationProgramTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TeacherPreparationProgramTypeDescriptorNamespace = ' + COALESCE(CAST(P.TeacherPreparationProgramTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderProgramBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherPreparationProviderProgramSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProviderProgram]' AS DataTableName
        ,P.[tid_TeacherPreparationProviderProgram]  AS Tid
        ,@Msg +  'TPPProgramPathwayDescriptorCodeValue = ' + COALESCE(CAST(P.TPPProgramPathwayDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TPPProgramPathwayDescriptorNamespace = ' + COALESCE(CAST(P.TPPProgramPathwayDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderProgramBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTPPProgramPathwayDescriptorBySnapshotId](@Ids) F
        ON
        	P.[TPPProgramPathwayDescriptorCodeValue] = F.[CodeValue]
        	AND P.[TPPProgramPathwayDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[TPPProgramPathwayDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[TPPProgramPathwayDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
            AND T.ErrorNumber = S.ErrorNumber
            AND T.ErrorMessage = S.ErrorMessage
            AND T.ErrorSeverity = S.ErrorSeverity
            AND T.ErrorSproc = S.ErrorSproc
            AND T.DataTableName = S.DataTableName
            AND T.Tid = S.Tid
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (SnapshotRecordTableName, SnapshotId, DataTableName, Tid, ErrorMessage, ErrorNumber, ErrorSeverity, ErrorSproc)
            VALUES (S.SnapshotRecordTableName, S.SnapshotId, S.DataTableName, S.Tid, S.ErrorMessage, S.ErrorNumber, S.ErrorSeverity, S.ErrorSproc);
            
        MERGE [tods].[AuditLog] AS T
        USING
        (SELECT '[t_extension].[TeacherPreparationProviderProgramGradeLevelSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProviderProgramGradeLevel]' AS DataTableName
        ,P.[tid_TeacherPreparationProviderProgramGradeLevel]  AS Tid
        ,@Msg +  'GradeLevelDescriptorCodeValue = ' + COALESCE(CAST(P.GradeLevelDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'GradeLevelDescriptorNamespace = ' + COALESCE(CAST(P.GradeLevelDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderProgramGradeLevelBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherPreparationProviderProgramTPPProgramDegreeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProviderProgramTPPProgramDegree]' AS DataTableName
        ,P.[tid_TeacherPreparationProviderProgramTPPProgramDegree]  AS Tid
        ,@Msg +  'AcademicSubjectDescriptorCodeValue = ' + COALESCE(CAST(P.AcademicSubjectDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'AcademicSubjectDescriptorNamespace = ' + COALESCE(CAST(P.AcademicSubjectDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderProgramTPPProgramDegreeBySnapshotId](@Ids) P
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
        (SELECT '[t_extension].[TeacherPreparationProviderProgramTPPProgramDegreeSnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[TeacherPreparationProviderProgramTPPProgramDegree]' AS DataTableName
        ,P.[tid_TeacherPreparationProviderProgramTPPProgramDegree]  AS Tid
        ,@Msg +  'TPPDegreeTypeDescriptorCodeValue = ' + COALESCE(CAST(P.TPPDegreeTypeDescriptorCodeValue AS NVARCHAR(128)), '') +  '|'+'TPPDegreeTypeDescriptorNamespace = ' + COALESCE(CAST(P.TPPDegreeTypeDescriptorNamespace AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetTeacherPreparationProviderProgramTPPProgramDegreeBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_extension].[ifn_GetTPPDegreeTypeDescriptorBySnapshotId](@Ids) F
        ON
        	P.[TPPDegreeTypeDescriptorCodeValue] = F.[CodeValue]
        	AND P.[TPPDegreeTypeDescriptorNamespace] = F.[Namespace]
        WHERE F.[CodeValue] IS NULL 
        AND P.[TPPDegreeTypeDescriptorCodeValue] IS NOT NULL
        AND F.[Namespace] IS NULL 
        AND P.[TPPDegreeTypeDescriptorNamespace] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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

/******Procedure [t_extension].[usp_eUniversity]******/

CREATE PROCEDURE [t_extension].[usp_eUniversity]
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
        (SELECT '[t_extension].[UniversitySnapshotRecord]' AS SnapshotRecordTableName
        , @pSnapshotId AS SnapshotId
        ,'[t_extension].[University]' AS DataTableName
        ,P.[tid_University]  AS Tid
        ,@Msg +  'UniversityId = ' + COALESCE(CAST(P.UniversityId AS NVARCHAR(128)), '') + ' not found' AS ErrorMessage
        ,@ErrorNumber AS ErrorNumber
        ,@ErrorSeverity AS ErrorSeverity
        ,@SprocName AS ErrorSproc
        FROM [t_extension].[ifn_GetUniversityBySnapshotId](@Ids) P
        LEFT OUTER JOIN [t_edfi].[ifn_GetEducationOrganizationBySnapshotId](@Ids) F
        ON
        	P.[UniversityId] = F.[EducationOrganizationId]
        WHERE F.[EducationOrganizationId] IS NULL 
        AND P.[UniversityId] IS NOT NULL
        
        ) AS S
            ON T.SnapshotId = S.SnapshotId
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
		EXEC [t_extension].[usp_eAnonymizedStudent] @pSnapshotId;

		EXEC [t_extension].[usp_eAnonymizedStudentAcademicRecord] @pSnapshotId;

		EXEC [t_extension].[usp_eAnonymizedStudentAssessment] @pSnapshotId;

		EXEC [t_extension].[usp_eAnonymizedStudentCourseAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eAnonymizedStudentCourseTranscript] @pSnapshotId;

		EXEC [t_extension].[usp_eAnonymizedStudentEducationOrganizationAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eAnonymizedStudentSectionAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eApplicant] @pSnapshotId;

		EXEC [t_extension].[usp_eCourseCourseTranscriptFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eCourseStudentAcademicRecordFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eCourseStudentAssessmentFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eCourseStudentFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eCourseSurveyQuestionResponseFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eCourseSurveyResponseFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eCourseSurveySectionResponseRatingFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eEducationOrganizationCourseTranscriptFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eEducationOrganizationFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eEducationOrganizationStudentAcademicRecordFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eEducationOrganizationStudentAssessmentFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eEducationOrganizationStudentFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eEducationOrganizationSurveyQuestionResponseFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eEducationOrganizationSurveyResponseFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eEducationOrganizationSurveySectionResponseRatingFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eExternalCourseTranscript] @pSnapshotId;

		EXEC [t_extension].[usp_eExternalEducationOrganization] @pSnapshotId;

		EXEC [t_extension].[usp_eExternalStudentAcademicRecord] @pSnapshotId;

		EXEC [t_extension].[usp_ePerformanceMeasure] @pSnapshotId;

		EXEC [t_extension].[usp_ePerformanceMeasureFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eProfessionalDevelopmentEvent] @pSnapshotId;

		EXEC [t_extension].[usp_eProspect] @pSnapshotId;

		EXEC [t_extension].[usp_eProspectProfessionalDevelopmentEventAttendance] @pSnapshotId;

		EXEC [t_extension].[usp_eRubric] @pSnapshotId;

		EXEC [t_extension].[usp_eRubricLevel] @pSnapshotId;

		EXEC [t_extension].[usp_eRubricLevelResponse] @pSnapshotId;

		EXEC [t_extension].[usp_eRubricLevelResponseFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eSchoolStudentGrowthMeasure] @pSnapshotId;

		EXEC [t_extension].[usp_eSectionCourseTranscriptFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eSectionStudentAcademicRecordFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eSectionStudentAssessmentFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eSectionStudentFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eSectionSurveyQuestionResponseFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eSectionSurveyResponseFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eSectionSurveySectionResponseRatingFacts] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffFieldworkAbsenceEvent] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffFieldworkExperience] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffProfessionalDevelopmentEventAttendance] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffStudentGrowthMeasure] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffStudentGrowthMeasureCourseAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffStudentGrowthMeasureEducationOrganizationAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffStudentGrowthMeasureSectionAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffTeacherPreparationProviderAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eStaffTeacherPreparationProviderProgramAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eSurvey] @pSnapshotId;

		EXEC [t_extension].[usp_eSurveyQuestion] @pSnapshotId;

		EXEC [t_extension].[usp_eSurveyQuestionResponse] @pSnapshotId;

		EXEC [t_extension].[usp_eSurveyResponse] @pSnapshotId;

		EXEC [t_extension].[usp_eSurveySection] @pSnapshotId;

		EXEC [t_extension].[usp_eSurveySectionResponseRating] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidate] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateAcademicRecord] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateCourseTranscript] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateFieldworkAbsenceEvent] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateFieldworkExperience] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateProfessionalDevelopmentEventAttendance] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateStaffAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateStudentGrowthMeasure] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureCourseAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureEducationOrganizationAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateStudentGrowthMeasureSectionAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateTeacherPreparationProviderAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherCandidateTeacherPreparationProviderProgramAssociation] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherPreparationProvider] @pSnapshotId;

		EXEC [t_extension].[usp_eTeacherPreparationProviderProgram] @pSnapshotId;

		EXEC [t_extension].[usp_eUniversity] @pSnapshotId;

	END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO