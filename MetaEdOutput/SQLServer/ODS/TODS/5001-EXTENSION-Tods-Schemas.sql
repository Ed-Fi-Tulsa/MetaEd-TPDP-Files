IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N't_extension')
EXEC sys.sp_executesql N'CREATE SCHEMA [t_extension]'
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'tods')
EXEC sys.sp_executesql N'CREATE SCHEMA [tods]'
GO
