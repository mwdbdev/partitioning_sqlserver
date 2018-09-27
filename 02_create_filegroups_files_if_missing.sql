USE [master]
GO

--/****** Object:  Database [dba_utils] ******/
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'dba_utils')
  BEGIN
    RAISERROR('dba_utils database missing!  Fix first, then rerun script.', 16, 1);
    RETURN;
  END

IF NOT EXISTS( SELECT * FROM [sys].[sysfilegroups] WHERE ([groupname] = N'partition_tables') )
  BEGIN
    PRINT 'Creating the [partition_tables] filegroup';
    ALTER DATABASE [dba_utils] ADD FILEGROUP [partition_tables];
  END

IF NOT EXISTS( SELECT * FROM [sys].[sysfiles] WHERE ([name] = 'partition_tables_01') )
  BEGIN
    PRINT 'Creating the E:\SQL_Data\partition_tables_01.ndf file.'
    ALTER DATABASE [dba_utils] ADD FILE (NAME = 'partition_tables_01', FILENAME = N'E:\SQL_Data\partition_tables_01.ndf', MAXSIZE = 4096 MB , FILEGROWTH = 512 MB ) TO FILEGROUP [partition_tables];
  END

  IF NOT EXISTS( SELECT * FROM [sys].[sysfilegroups] WHERE ([groupname] = N'partition_indexes') )
  BEGIN
    PRINT 'Creating the [partition_indexes] filegroup';
    ALTER DATABASE [dba_utils] ADD FILEGROUP [partition_indexes] ;
  END

IF NOT EXISTS( SELECT * FROM [sys].[sysfiles] WHERE ([name] = 'partition_indexes_01') )
  BEGIN
    PRINT 'Creating the I:\SQL_Data\partition_indexes_01.ndf file.'
    ALTER DATABASE [dba_utils] ADD FILE (NAME = 'partition_indexes_01', FILENAME = N'I:\SQL_Data\partition_indexes_01.ndf', MAXSIZE = 4096 MB , FILEGROWTH = 256 MB ) TO FILEGROUP [partition_indexes];
  END


GO

--alter database file sizes
ALTER DATABASE dba_utils MODIFY FILE ( NAME = N'partition_tables_01' , SIZE = 512 MB ) ;
ALTER DATABASE dba_utils MODIFY FILE ( NAME = N'partition_indexes_01', SIZE = 256 MB ) ;
GO