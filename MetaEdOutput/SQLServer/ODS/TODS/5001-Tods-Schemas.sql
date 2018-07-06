IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N't_edfi')
EXEC sys.sp_executesql N'CREATE SCHEMA [t_edfi]'
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'tods')
EXEC sys.sp_executesql N'CREATE SCHEMA [tods]'
GO
