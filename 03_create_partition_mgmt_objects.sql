/*
Run this script on each new server after the dba_utils database is created.

Script created by SQL Compare version 13.2.4.5728 from Red Gate Software Ltd at 4/12/2018 7:43:32 PM

*/
SET NUMERIC_ROUNDABORT OFF
GO
SET ANSI_PADDING, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
USE [dba_utils]
GO
SET XACT_ABORT ON
GO
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
GO
BEGIN TRANSACTION
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating schemas'
GO
IF SCHEMA_ID(N'part') IS NULL
EXEC sp_executesql N'CREATE SCHEMA [part]
AUTHORIZATION [dbo]'
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating partition functions'
GO
DECLARE 
  @start_dt NVARCHAR(23)

IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'pf_partition_activity_log')
CREATE PARTITION FUNCTION [pf_partition_activity_log] ([datetime]) 
AS RANGE RIGHT FOR VALUES 
(
  CONVERT(NVARCHAR(10), DATEADD(DAY, -3, GETDATE ()), 120)
, CONVERT(NVARCHAR(10), DATEADD(DAY, -2, GETDATE ()), 120)
, CONVERT(NVARCHAR(10), DATEADD(DAY, -1, GETDATE ()), 120)
, CONVERT(NVARCHAR(10), GETDATE (), 120)
, CONVERT(NVARCHAR(10), DATEADD(DAY, 1, GETDATE ()), 120) 
, CONVERT(NVARCHAR(10), DATEADD(DAY, 2, GETDATE ()), 120)
, CONVERT(NVARCHAR(10), DATEADD(DAY, 3, GETDATE ()), 120)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'pf_partition_activity_steps_log')
CREATE PARTITION FUNCTION [pf_partition_activity_steps_log] ([datetime]) 
AS RANGE RIGHT FOR VALUES 
(
  CONVERT(NVARCHAR(10), DATEADD(DAY, -3, GETDATE ()), 120)
, CONVERT(NVARCHAR(10), DATEADD(DAY, -2, GETDATE ()), 120)
, CONVERT(NVARCHAR(10), DATEADD(DAY, -1, GETDATE ()), 120)
, CONVERT(NVARCHAR(10), GETDATE (), 120)
, CONVERT(NVARCHAR(10), DATEADD(DAY, 1, GETDATE ()), 120) 
, CONVERT(NVARCHAR(10), DATEADD(DAY, 2, GETDATE ()), 120)
, CONVERT(NVARCHAR(10), DATEADD(DAY, 3, GETDATE ()), 120)
) ;
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating partition schemes'
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'ps_partition_activity_log')
CREATE PARTITION SCHEME [ps_partition_activity_log] 
AS PARTITION [pf_partition_activity_log] 
TO ([partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables]);
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'ps_partition_activity_steps_log')
CREATE PARTITION SCHEME [ps_partition_activity_steps_log] 
AS PARTITION [pf_partition_activity_steps_log] 
TO ([partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables], [partition_tables]);
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[partition_tbl_list]'
GO
IF OBJECT_ID(N'[part].[partition_tbl_list]', 'U') IS NULL
CREATE TABLE [part].[partition_tbl_list]
(
[id] [int] NOT NULL IDENTITY(1, 1),
[database_name] [sys].[sysname] NOT NULL,
[partition_column] [sys].[sysname] NOT NULL,
[partition_function] [sys].[sysname] NOT NULL,
[partition_scheme] [sys].[sysname] NOT NULL,
[partition_retention] [int] NOT NULL,
[table_schema] [sys].[sysname] NOT NULL CONSTRAINT [DF_partition_tbl_list_table_schema] DEFAULT ('dbo'),
[table_name] [sys].[sysname] NOT NULL,
[compress_table] [nvarchar] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_partition_tbl_list_compress_table] DEFAULT ('Y'),
[compress_index] [nvarchar] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_partition_tbl_list_compress_index] DEFAULT ('Y'),
[archive_flag] [nvarchar] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[last_archive_date] [datetime] NULL,
[is_active] [nvarchar] (1) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_partition_tbl_list_is_active] DEFAULT ('Y'),
[insert_datetime] [datetime] NOT NULL CONSTRAINT [DF_partition_tbl_list_insert_datetime] DEFAULT (getdate())
) ON [partition_tables]
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating primary key [PK_partition_tbl_list] on [part].[partition_tbl_list]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'PK_partition_tbl_list' AND object_id = OBJECT_ID(N'[part].[partition_tbl_list]'))
ALTER TABLE [part].[partition_tbl_list] ADD CONSTRAINT [PK_partition_tbl_list] PRIMARY KEY CLUSTERED  ([id]) ON [partition_tables]
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating index [uncidx_partition_tbl_list] on [part].[partition_tbl_list]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'uncidx_partition_tbl_list' AND object_id = OBJECT_ID(N'[part].[partition_tbl_list]'))
CREATE UNIQUE NONCLUSTERED INDEX [uncidx_partition_tbl_list] ON [part].[partition_tbl_list] ([database_name], [table_schema], [table_name]) ON [partition_indexes]
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[partition_activity_steps_log]'
GO
IF OBJECT_ID(N'[part].[partition_activity_steps_log]', 'U') IS NULL
CREATE TABLE [part].[partition_activity_steps_log]
(
[id] [int] NOT NULL IDENTITY(1, 1),
[group_action] [nvarchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[database_name] [sys].[sysname] NOT NULL,
[table_schema] [sys].[sysname] NOT NULL,
[table_name] [sys].[sysname] NOT NULL,
[step_last_run] [nvarchar] (1500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[last_activity] [datetime] NOT NULL CONSTRAINT [DF_partition_activity_steps_log_last_activity] DEFAULT (getdate()),
[insert_datetime] [datetime] NOT NULL CONSTRAINT [DF_partition_activity_steps_log_insert_datetime] DEFAULT (getdate())
) ON [ps_partition_activity_steps_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating index [clidx_partition_activity_steps_log] on [part].[partition_activity_steps_log]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'clidx_partition_activity_steps_log' AND object_id = OBJECT_ID(N'[part].[partition_activity_steps_log]'))
CREATE CLUSTERED INDEX [clidx_partition_activity_steps_log] ON [part].[partition_activity_steps_log] ([insert_datetime] DESC, [id] DESC) ON [ps_partition_activity_steps_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating primary key [PK_partition_activity_steps_log] on [part].[partition_activity_steps_log]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'PK_partition_activity_steps_log' AND object_id = OBJECT_ID(N'[part].[partition_activity_steps_log]'))
ALTER TABLE [part].[partition_activity_steps_log] ADD CONSTRAINT [PK_partition_activity_steps_log] PRIMARY KEY NONCLUSTERED  ([id], [insert_datetime]) ON [ps_partition_activity_steps_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[partition_activity_steps_log_insert]'
GO
IF OBJECT_ID(N'[part].[partition_activity_steps_log_insert]', 'P') IS NULL
EXEC sp_executesql N'/*============================================================================
Purpose:   Insert records into the part.partition_activity_steps_log table.
Modified:	
Date       Version    Developer      Comments

3/8/2018     1.0      M. Westrell    Original
============================================================================*/
CREATE PROCEDURE [part].[partition_activity_steps_log_insert]
  @group_action VARCHAR(50)
, @database_name sysname
, @table_schema sysname
, @tbl_name sysname
, @step_last_run VARCHAR(2000)
, @execute_datetime DATETIME
AS
BEGIN TRY

--Set defaults and environment values
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

--error check
  IF  ( 
          ( @group_action IS NULL OR @group_action = '''') 
      OR  ( @database_name IS NULL OR @database_name = '''')
      OR  ( @table_schema IS NULL OR @table_schema = '''') 
      OR  ( @tbl_name IS NULL OR @tbl_name = '''') 
      OR  ( @step_last_run IS NULL OR @step_last_run = '''') 
      ) 
    BEGIN
      RAISERROR(''The arguments for this procedure cannot be NULL or empty strings.  Log record could not be inserted.'', 16, 1);
    END

--set value if not passed
  IF (@execute_datetime IS NULL) 
    BEGIN
      SET @execute_datetime = CURRENT_TIMESTAMP;
    END

--insert record
  INSERT INTO [part].[partition_activity_steps_log]
             ([group_action]
             ,[database_name]
             ,[table_schema]
             ,[table_name]
             ,[step_last_run]
             ,[last_activity])
      VALUES ( @group_action
              ,@database_name 
              ,@table_schema 
              ,@tbl_name
              ,@step_last_run
              ,@execute_datetime 
             ); 

END TRY
BEGIN CATCH
	DECLARE 
		@errNum INT
	, @errSeverity INT
	, @errState INT
	, @errProc NVARCHAR(128)
	, @errLine INT
  , @errMsg NVARCHAR(4000)
	, @commitState NVARCHAR(100)
	, @returnedMsg NVARCHAR(2047)
	;
	SET @commitState = '''';
-- Process depending on whether the transaction is uncommittable or committable.
    IF (XACT_STATE()) = -1
    BEGIN
        SET @commitState = N''Transaction was in an uncommittable state and was rolled back. '' 
        ROLLBACK TRANSACTION;
    END;
    -- Test whether the transaction is committable.
    IF (XACT_STATE()) = 1
    BEGIN
        SET @commitState = N''Transaction was committable and was committed. '' 
        COMMIT TRANSACTION;   
    END;
	SELECT	@errNum = ERROR_NUMBER()
				, @errSeverity = ERROR_SEVERITY()
				, @errState = ERROR_STATE() 
				, @errProc = ERROR_PROCEDURE() 
				, @errLine = ERROR_LINE() 
				, @errMsg = ERROR_MESSAGE()
				;
		SELECT @returnedMsg = N''Error '' + CONVERT(VARCHAR(10), @errNum) + '' occurred on line '' + 
		CONVERT(VARCHAR(10), @errLine) + '' with error state '' + CONVERT(NVARCHAR(10), @errState)
		+ '' in procedure '' + @errProc + ''. '' + @commitState;
		SET @returnedMsg = @returnedMsg + SUBSTRING(@errMsg, 1, 2047 - LEN(@returnedMsg));
	IF (@errState = 0) 
		BEGIN
			SET @errState = 1;
		END  
	RAISERROR(@returnedMsg, @errSeverity, @errState);
END CATCH
'
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[partition_activity_log]'
GO
IF OBJECT_ID(N'[part].[partition_activity_log]', 'U') IS NULL
CREATE TABLE [part].[partition_activity_log]
(
[id] [int] NOT NULL IDENTITY(1, 1),
[proc_name] [sys].[sysname] NOT NULL,
[start_time] [datetime] NOT NULL,
[end_time] [datetime] NULL,
[start_or_stop] [char] (5) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[insert_datetime] [datetime] NOT NULL CONSTRAINT [DF_partition_activity_log_insert_datetime] DEFAULT (getdate())
) ON [ps_partition_activity_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating index [clidx_partition_activity_log] on [part].[partition_activity_log]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'clidx_partition_activity_log' AND object_id = OBJECT_ID(N'[part].[partition_activity_log]'))
CREATE CLUSTERED INDEX [clidx_partition_activity_log] ON [part].[partition_activity_log] ([insert_datetime] DESC) ON [ps_partition_activity_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating primary key [PK_partition_activity_log] on [part].[partition_activity_log]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'PK_partition_activity_log' AND object_id = OBJECT_ID(N'[part].[partition_activity_log]'))
ALTER TABLE [part].[partition_activity_log] ADD CONSTRAINT [PK_partition_activity_log] PRIMARY KEY NONCLUSTERED  ([id], [insert_datetime]) ON [ps_partition_activity_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[partition_activity_log_insert]'
GO
IF OBJECT_ID(N'[part].[partition_activity_log_insert]', 'P') IS NULL
EXEC sp_executesql N' 
/*==================================================================================================
Purpose:  Inserts records in the partition_activity_log table to monitor partition and archival times.

Modified:	
Date       Version    Developer      Comments

4/6/2017     1.1      M. Westrell    Added TRY - CATCH blocks and standard formatting
1/22/2015    1.0      B. Guarnieri   Original, based on Michael Bourgon code

Parameters:                                                                                      
             @object_name: Name of the object that''s being logged.                                    
             @timestamp_datetime : Datetime of this sproc''s execution                                 
             @start_or_stop : Is the process completing or stopping                                   
																								    
Usage: Exec [part].[partition_activity_log_insert] @object_name=''Table_Or_Sproc'', @start_or_stop=''start''       
===================================================================================================*/ 
CREATE procedure [part].[partition_activity_log_insert]  
		@object_name varchar(200), 
		@timestamp_datetime datetime = null, 
		@start_or_stop char(10) 
AS
BEGIN TRY 

  DECLARE 
    @max_id INT
  , @error NVARCHAR(100)
  ;

--Set defaults and environment values
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

--error check
  IF ( (ISNULL(@object_name, '''') = '''') OR (ISNULL(@start_or_stop, '''') = '''') ) 
    BEGIN
      RAISERROR(''The @object_name and @start_or_stop parameters cannot be NULL or empty strings.  Log record could not be inserted.'', 16, 1);
    END

  IF ( @timestamp_datetime IS NULL )
    BEGIN
      SET @timestamp_datetime = (
                                  SELECT GETDATE ()
                                ); 
    END

--insert start entry
  IF LOWER(@start_or_stop) = ''start''
    BEGIN 		 
      SET @start_or_stop = ''start''; 
				 
      INSERT  INTO part.partition_activity_log
              ( proc_name
              , start_time
              , start_or_stop )
      VALUES  ( @object_name
              , @timestamp_datetime
              , @start_or_stop ); 
    END; 

--update an existing entry with the stop datetime and process
  IF LOWER(@start_or_stop) = ''stop''
    BEGIN  
						 
      SET @max_id = (
                      SELECT MAX (id) 
                      FROM part.partition_activity_log
                      WHERE ( proc_name = @object_name )
                    ); 
 
      UPDATE  part.partition_activity_log
      SET     end_time = @timestamp_datetime
            , start_or_stop = ''stop''
      WHERE   ( proc_name = @object_name )
        AND   ( end_time IS NULL )
        AND   ( id = @max_id ) ; 
    END; 
		 
END TRY
BEGIN CATCH
	DECLARE 
		@errNum INT
	, @errSeverity INT
	, @errState INT
	, @errProc NVARCHAR(128)
	, @errLine INT
  , @errMsg NVARCHAR(4000)
	, @commitState NVARCHAR(100)
	, @returnedMsg NVARCHAR(2047)
	;
	SET @commitState = '''';
-- Process depending on whether the transaction is uncommittable or committable.
    IF (XACT_STATE()) = -1
    BEGIN
        SET @commitState = N''Transaction was in an uncommittable state and was rolled back. '' 
        ROLLBACK TRANSACTION;
    END;
    -- Test whether the transaction is committable.
    IF (XACT_STATE()) = 1
    BEGIN
        SET @commitState = N''Transaction was committable and was committed. '' 
        COMMIT TRANSACTION;   
    END;
	SELECT	@errNum = ERROR_NUMBER()
				, @errSeverity = ERROR_SEVERITY()
				, @errState = ERROR_STATE() 
				, @errProc = ERROR_PROCEDURE() 
				, @errLine = ERROR_LINE() 
				, @errMsg = ERROR_MESSAGE()
				;
		SELECT @returnedMsg = N''Error '' + CONVERT(VARCHAR(10), @errNum) + '' occurred on line '' + 
		CONVERT(VARCHAR(10), @errLine) + '' with error state '' + CONVERT(NVARCHAR(10), @errState)
		+ '' in procedure '' + @errProc + ''. '' + @commitState;
		SET @returnedMsg = @returnedMsg + SUBSTRING(@errMsg, 1, 2047 - LEN(@returnedMsg));
	IF (@errState = 0) 
		BEGIN
			SET @errState = 1;
		END  
	RAISERROR(@returnedMsg, @errSeverity, @errState);
END CATCH'
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[add_future_partition]'
GO
IF OBJECT_ID(N'[part].[add_future_partition]', 'P') IS NULL
EXEC sp_executesql N'/*======================================================================================
Purpose:   Adds partitions to partition schemes and partition functions based 
           on days.  Procdure finds the current partition and splits it.  The   
           initial execution take the first date entered into the function  
           when it i created and adds partitions to current date.																	    
Modified:	 
Date       Version    Developer      Comments 

3/8/2018     1.4      M. Westrell    Replaced insert statements with calls to the  
                                     [part].[partition_activity_steps_log_insert] 
                                     stored procedure.  Rewrote some statements as
                                     dynamic SQL to include the name of the database  
                                     hosting the table being evaluated.  This is 
                                     needed to maintain partitioned tables regardless
                                     of what database they exist in using partition 
                                     management objects in the [dba_utils] database.
                                     Also increased the number of future partitions 
                                     from 2 to 3, to provide a greater "margin for error".
5/1/2017     1.3      M. Westrell    Added increment number to the invalid records table
                                     name.  If the partition_activity_steps_log table 
                                     partitioning falls far enough behind, the process of 
                                     adding partitions inserts records into the rightmost 
                                     partition, causing the issue each time the loops 
                                     runs, and the datetime for the invalid records table 
                                     ends up being the same, causing the process to fail.
4/25/2017    1.2      M. Westrell    Added code to check for records in the rightmost  
                                     partition and remove them.  Such records have 
                                     caused the SPLIT partition process to fail when  
                                     the table is partitioned on a computed column. 
4/14/2017    1.1      M. Westrell    Added TRY - CATCH blocks, error check,  
                                     and standard documentation block.  Added 
                                     arguments so an individual table can be processed
                                     instead of the entire set of partitioned tables. 
1/9/2015     1.0      B. Guarnieri   Original, based on Michael Bourgon code 

Parameters:
@partitioned_table_database The database that hosts the table to be partitioned 
                            and related objects.  Default is NULL, which will process
                            all active records in the table.
@partitioned_table_schema   The schema for a specific table you want to add future
                            partitions to.  Default is NULL, which will process
                            all active records in the table.
@partitioned_table_name     The name of a specific table you want to add future
                            partitions to.  Default is NULL, which will process
                            all active records in the table.

If any of the @partitioned_table... parameters is NULL, a NULL value will be 
passed for all three values, which will process all active records in the table.   
This will not affect tables that have the appropriate number of future partitions, 
so this can be helpful if you are deploying several new partitioned tables at the 
same time.

Usage:     
option 1    EXEC [dbo].[add_future_partition] ;

option 2    EXEC [dbo].[add_future_partition] 
              @partitioned_table_database = N''Your database name''
            , @partitioned_table_schema = N''Your table schema''
            , @partitioned_table_name = N''Your table name''
            ;												    
======================================================================================*/ 
CREATE PROCEDURE [part].[add_future_partition] 
  @partitioned_table_database sysname = NULL
, @partitioned_table_schema sysname = NULL
, @partitioned_table_name sysname = NULL
AS   
BEGIN TRY 
  
  DECLARE  
    @counter int 
  , @max int 
  , @day DATETIME  			 
  , @sqlCmd NVARCHAR(MAX) 
  , @sqlCmd2 NVARCHAR(200) 
  , @step NVARCHAR(200)
  , @db_name sysname  
  , @tbl_schema sysname
  , @tbl_name sysname
  , @partition_function sysname
  , @partition_scheme sysname  
  , @filegroup_name sysname
  , @table_in_process nvarchar(261)  
  , @partition_column sysname
  , @ParmDefinition NVARCHAR(500)
  , @rec_ct INT
  , @invalid_records_table_name NVARCHAR(128)
  , @max_partition_day DATE
  , @LF CHAR(2) = CHAR(13) + CHAR(10)
  , @dt_now DATETIME 
  ; 

--Set defaults and environment values 
	SET NOCOUNT ON; 
	SET XACT_ABORT ON; 
 
  DECLARE @partition_list TABLE 
  ( 
    [id] INT NOT NULL IDENTITY (1, 1) 
  , [database_name] sysname NOT NULL
  , [table_schema] sysname NOT NULL
  , [table_name] sysname NOT NULL 
  , [partition_function] sysname NOT NULL 
  , [partition_scheme] sysname NOT NULL 
  , [partition_column] sysname NOT NULL
  ) ; 
			 
--load tables for dynamic processing 
  INSERT  INTO @partition_list 
          (
            [database_name]
          , [table_schema]  
          , [table_name] 
          , [partition_function] 
          , [partition_scheme]  
          , [partition_column]
          ) 
  SELECT DISTINCT
          [database_name] 
        , [table_schema]
        , [table_name] 
        , [partition_function] 
        , [partition_scheme] 
        , [partition_column]
  FROM    [part].[partition_tbl_list] 
  WHERE ( [is_active] = ''Y'' ) 
    AND ( 1 = CASE WHEN @partitioned_table_database IS NULL and @partitioned_table_schema IS NULL AND @partitioned_table_name IS NULL THEN 1 
                   WHEN [database_name] = @partitioned_table_database AND [table_schema] = @partitioned_table_schema AND [table_name] = @partitioned_table_name THEN 1
                   ELSE 0
              END
        ) ;
 
--error check 
  IF ( (SELECT COUNT([table_name]) FROM @partition_list [pl]) = 0 ) 
    BEGIN 
      RAISERROR (''No active records found in the [part].[partition_tbl_list] table.'', 16, 1) ; 
      RETURN; 
    END 
 
--set minimum and maximum counters for iteration			 
  SET @counter = (SELECT MIN([id]) FROM @partition_list) ; 
  SET @max = (SELECT MAX([id]) FROM @partition_list) ; 

--insert log entry 
  SET @dt_now = CURRENT_TIMESTAMP;

  EXEC [part].[partition_activity_log_insert] 
    @object_name = ''dbo.add_future_partition''
  , @timestamp_datetime = @dt_now
  , @start_or_stop = ''start'' ;
 
/*----------------------------------------OUTER LOOP START----------------------------------------*/ 
  WHILE ( @max + 1 ) > @counter --OUTER LOOP 
 
    BEGIN 
 
    --add partition function from list 
      SELECT  @db_name = [database_name] 	
            , @tbl_schema = [table_schema]
            , @tbl_name = [table_name]
            , @table_in_process = [database_name] + ''.'' + [table_schema] + ''.'' + [table_name] 
						, @partition_function = [partition_function] 
						, @partition_scheme = [partition_scheme] 
            , @partition_column = [partition_column]
				FROM  @partition_list  
       WHERE  ( [id] = @counter ) ; 
 
    --debug statements 
      --print @partition_function  
      --print @partition_scheme  
 
    --get the last value that was added to partition_ranges_values 
      SET @ParmDefinition = ''@day_out DATETIME OUTPUT'' ;

      SET @sqlCmd = 
      ''SET @day_out = 
       CAST((
            SELECT TOP 1 [value] 
            FROM ['' + @db_name + ''].[sys].[partition_range_values] 
            WHERE   function_id = 
            ( 
              SELECT  function_id 
              FROM ['' + @db_name + ''].[sys].[partition_functions] 
              WHERE   ( name = '''''' + @partition_function + '''''' ) 
            ) 
            ORDER BY [boundary_id] DESC
          ) AS DATETIME
      );'';

      EXEC [sys].[sp_executesql]
            @sqlCmd
          , @ParmDefinition
          , @day_out = @day OUTPUT 
          ;

      SET @max_partition_day = @day ;

    --debug statements 
      --print ''Seed'' 
      --print CONVERT(NVARCHAR(20), @day, 120) 
  
     SET @step = N''Partition Split Range Current Value to be split :@day: '' + CONVERT(NVARCHAR(20), @day, 120);
     SET @dt_now = CURRENT_TIMESTAMP;

    --insert log entry 
      EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
        @group_action = ''add future partitions''
      , @database_name = @db_name
      , @table_schema = @tbl_schema
      , @tbl_name = @tbl_name
      , @step_last_run = @step
      , @execute_datetime = @dt_now
      ;

    --Run through each day adding days to the partition schema splitting it by day 
      IF ( @day > GETDATE() + 3 ) 
 
        BEGIN 
 
          SET @step = ''@partition_scheme: '' + @partition_scheme + '' Range Split not performed '' + CONVERT(NVARCHAR(20), @day, 120)  + '' > '' + CONVERT(NVARCHAR(20), GETDATE() + 2, 120) + '' ;'' 
          SET @dt_now = CURRENT_TIMESTAMP;

        --insert log entry 
          EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
            @group_action = ''add future partitions''
          , @database_name = @db_name
          , @table_schema = @tbl_schema
          , @tbl_name = @tbl_name
          , @step_last_run = @step
          , @execute_datetime = @dt_now
          ;

        END
 
/*----------------------------------------INNER LOOP START----------------------------------------*/ 
      WHILE ( @day <= GETDATE() + 3 ) 
 
        BEGIN 
 
          SET @ParmDefinition = ''@filegroup_name_out NVARCHAR(128) OUTPUT'' ;

        --Get the name of the current filegroup in which to add the additional split 
          SET @sqlCmd =
          ''SET @filegroup_name_out =  
          ( 
              SELECT TOP 1 [fg].[name] AS [FileGroupName] 
              FROM ['' + @db_name + ''].[sys].[partitions] AS [p] 
              JOIN ['' + @db_name + ''].[sys].[indexes] AS [i] ON [i].[object_id] = [p].[object_id] AND [i].[index_id] = [p].[index_id] 
              JOIN ['' + @db_name + ''].[sys].[data_spaces] AS [ds] ON [ds].[data_space_id] = [i].[data_space_id] 
              JOIN ['' + @db_name + ''].[sys].[partition_schemes] AS [ps] ON [ps].[data_space_id] = [ds].[data_space_id] 
              JOIN ['' + @db_name + ''].[sys].[partition_functions] AS [pf] ON [pf].[function_id] = [ps].[function_id] 
              JOIN ['' + @db_name + ''].[sys].[destination_data_spaces] AS [dds2] ON [dds2].[partition_scheme_id] = [ps].[data_space_id] AND [dds2].[destination_id] = [p].[partition_number] 
              JOIN ['' + @db_name + ''].[sys].[filegroups] AS [fg] ON [fg].[data_space_id] = [dds2].[data_space_id] 
              WHERE  ( [ds].[name] = '''''' + @partition_scheme + '''''') 
              ORDER BY [fg].[data_space_id] DESC 
            ) ; '';
 
        EXEC [sys].[sp_executesql]
              @sqlCmd
            , @ParmDefinition
            , @filegroup_name_out = @filegroup_name OUTPUT ;

        --Assumes the partition function is uniquely tied to function in a 1:1 relationship 
        --If I should mix and match I just need to add another table variable and run seperately. 
          SET @day = ( 
                        SELECT DATEADD (DAY, 1, @day) 
                      ) ; 
          --changed to +1 instead of 3 so it would partition 1 day at a time 
 
          SET @sqlCmd2 = ''USE ['' + @db_name + '']; ALTER PARTITION SCHEME ['' + @partition_scheme + ''] NEXT USED ['' + @filegroup_name + ''];'';
          SET @step = ''@partition_scheme: '' + @partition_scheme + '' '' + @sqlCmd2; 
          SET @dt_now = CURRENT_TIMESTAMP;

        --insert log record 
          EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
            @group_action = ''add future partitions''
          , @database_name = @db_name
          , @table_schema = @tbl_schema
          , @tbl_name = @tbl_name
          , @step_last_run = @step
          , @execute_datetime = @dt_now
          ;

          PRINT @sqlCmd2 ; 
          EXEC sp_executesql @sqlCmd2 ;    
          
        --before trying to split the range, check for records in the rightmost partition and update
          SET @sqlCmd2 = ''SELECT @rec_ct_out = COUNT(*) FROM '' + @table_in_process 
            + '' WHERE ( ['' + @partition_column + ''] > N'''''' + CONVERT(NVARCHAR(25), @max_partition_day, 120) + '''''' ) ;''          
          SET @ParmDefinition = ''@rec_ct_out INT OUTPUT'' ;

          EXEC [sys].[sp_executesql]
            @sqlCmd2
          , @ParmDefinition
          , @rec_ct_out = @rec_ct OUTPUT ;

          IF ( @rec_ct > 0 )
            BEGIN

              BEGIN TRANSACTION

              --set table name
                SET @invalid_records_table_name = ''[eif_workspace].[dbo].['' + @db_name + ''_'' + @tbl_schema + ''_'' + @tbl_name + ''_invalid_records_'' 
                  + REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR(25), GETDATE(), 121), ''.'', ''''), '':'', ''''), ''-'', ''''), '' '', ''_'') + '']'';

              ----adjust name if table by that name already exists
              --  SELECT @rec_ct = COUNT([name]) FROM [eif_workspace].[sys].[tables] 
              --  WHERE ( [name] = @invalid_records_table_name ) ;

              --  SET @invalid_records_table_name = @invalid_records_table_name + CAST((@rec_ct + 1) AS NVARCHAR(10)) + '']'' ;

              --save invalid date records to a backup table
                SET @sqlCmd = 
                N''SELECT * INTO '' + @invalid_records_table_name 
                 + '' FROM '' + @table_in_process + '' WHERE ( ['' + @partition_column + ''] > N'''''' + CONVERT(NVARCHAR(25), @max_partition_day, 120) + '''''' ) ;'' + @LF +
                 + ''DELETE FROM '' + @table_in_process + '' WHERE ( ['' + @partition_column + ''] > N'''''' + CONVERT(NVARCHAR(25), @max_partition_day, 120) + '''''' ) ;'' ;
                PRINT @sqlCmd ;

              --insert log record 
                SET @step = N''invalid records removed: '' + SUBSTRING(@sqlCmd, 1, 500);
                SET @dt_now = CURRENT_TIMESTAMP;

                EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
                  @group_action = ''add future partitions''
                , @database_name = @db_name
                , @table_schema = @tbl_schema
                , @tbl_name = @tbl_name
                , @step_last_run = @step
                , @execute_datetime = @dt_now
                ;

              --remove records
                EXEC sp_executesql @sqlCmd ;

              COMMIT TRANSACTION

            END

        --split the partition
          SET @sqlCmd2 = ''USE '' + @db_name + ''; ALTER PARTITION FUNCTION '' + @partition_function + ''() split range('' + '''''''' + convert(nvarchar(20), convert(date, @Day, 120)) + '''''''' + '');'' 
          SET @step = N''@partition_function: '' + @partition_function + '' '' + @sqlCmd2;
          SET @dt_now = CURRENT_TIMESTAMP;

          EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
            @group_action = ''add future partitions''
          , @database_name = @db_name
          , @table_schema = @tbl_schema
          , @tbl_name = @tbl_name
          , @step_last_run = @step
          , @execute_datetime = @dt_now
          ;
 
--DEBUG:
--PRINT ''Inner Loop Day:'' 
--PRINT @day 

          PRINT @sqlCmd2 ; 
          EXEC sp_executesql @sqlCmd2 ;     
	 
        END --InnerLoop 
 /*----------------------------------------INNER LOOP END----------------------------------------*/ 

      SET @counter = @counter + 1 ; 
 
    END --Outerloop 
/*----------------------------------------OUTER LOOP END----------------------------------------*/
 
    SET @dt_now = CURRENT_TIMESTAMP;

  	EXEC [dba_utils].[part].[partition_activity_log_insert] 
      @object_name = ''dbo.add_future_partition''
  	, @timestamp_datetime = @dt_now
  	, @start_or_stop = ''stop''
    ;
 
END TRY  
BEGIN CATCH 
	DECLARE  
    @errNum INT 
	, @errSeverity INT 
	, @errState INT 
	, @errProc NVARCHAR(128) 
	, @errLine INT 
  , @errMsg NVARCHAR(4000) 
	, @commitState NVARCHAR(100) 
	, @returnedMsg NVARCHAR(2047) 
	; 
 
	SET @commitState = ''''; 
 
--Process depending on whether the transaction is uncommittable or committable. 
  IF (XACT_STATE()) = -1 
  BEGIN 
      SET @commitState = N''Transaction was in an uncommittable state and was rolled back. ''  
      ROLLBACK TRANSACTION; 
  END; 
 
--Test whether the transaction is committable. 
  IF (XACT_STATE()) = 1 
    BEGIN 
        SET @commitState = N''Transaction was committable and was committed. ''  
        COMMIT TRANSACTION;    
    END; 
 
	SELECT	@errNum = ERROR_NUMBER() 
				, @errSeverity = ERROR_SEVERITY() 
				, @errState = ERROR_STATE()  
				, @errProc = ERROR_PROCEDURE()  
				, @errLine = ERROR_LINE()  
				, @errMsg = ERROR_MESSAGE() 
				; 
 
	SELECT @returnedMsg = N''Error '' + CONVERT(VARCHAR(10), @errNum) + '' occurred on line '' +  
	CONVERT(VARCHAR(10), @errLine) + '' with error state '' + CONVERT(NVARCHAR(10), @errState) 
	+ '' in procedure '' + ISNULL(@errProc, ''script'') + ''. '' + @commitState; 
	SET @returnedMsg = @returnedMsg + SUBSTRING(@errMsg, 1, 2047 - LEN(@returnedMsg)); 
 
	IF (@errState = 0)  
		BEGIN 
			SET @errState = 1; 
		END 
 
	RAISERROR(@returnedMsg, @errSeverity, @errState); 
 
END CATCH 

'
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[drop_oldest_partition]'
GO
IF OBJECT_ID(N'[part].[drop_oldest_partition]', 'P') IS NULL
EXEC sp_executesql N'--IF OBJECT_ID(N''[part].[drop_oldest_partition]'', ''P'') IS NULL
--EXEC sp_executesql N''	 
/*==================================================================================================
Purpose:   Facilitates the switching of partition between an <table> and it''s "<table>_AUX" table 
           to truncate data on a daily parition. Data is pulled from the [part].[partition_tbl_list] 
           configuration table in the current database. After populating variables and setting up 
           loops, the paritioning code moves through 4 steps.	
           1:   Compress partition for "AUX" tables.												    
           2:   Check Table indexes and compression to match										    
           3:   Switch Partitions between Aux and Tables and truncate								    
           4:   Merge empty old partitions												

--notes from Michael''s code that are pertinent to this procedure
  -- mdb 2012/01/18 a more generic version of DEL PARTITION LEFT. Uses dynamic sql. 
  -- Note that this assumes your partitioning key is a date, and that you''re RANGE RIGHT. 
  -- Given a partition function, it''ll walk days, finding the tables in that DB with that fxn, 
  --  switching each out with its aux, truncating the aux, then MERGEing the function. 
 
Modified:
Date       Version    Developer      Comments
3/8/2018     1.3      M. Westrell    Replaced insert statements with calls to the  
                                     [part].[partition_activity_steps_log_insert] 
                                     stored procedure.  Rewrote some statements as
                                     dynamic SQL to include the name of the database  
                                     hosting the table being evaluated.  This is 
                                     needed to maintain partitioned tables regardless
                                     of what database they exist in using partition 
                                     management objects in the [dba_utils] database.
5/1/2017     1.2      M. Westrell    Added arguments so an individual table can be processed
                                     instead of the entire set of partitioned tables.
4/6/2017     1.1      M. Westrell    Added TRY - CATCH blocks. Formatted to improve readability.
1/22/2015    1.0      B. Guarnieri   Original, based on Michael Bourgon code.

Parameters: 
@partitioned_table_database The database that hosts the table to be partitioned 
                            and related objects.  Default is NULL, which will process
                            all active records in the table.
@partitioned_table_schema   The schema for a specific table you want to add future
                            partitions to.  Default is NULL, which will process
                            all active records in the table.
@partitioned_table_name     The name of a specific table you want to add future
                            partitions to.  Default is NULL, which will process
                            all active records in the table.

If any of the @partitioned_table... parameters is NULL, a NULL value will be passed for all 
three values, which will process all active records in the table.    

Usage: 

option 1    EXEC [dbo].[drop_oldest_partition] ;

option 2    EXEC [dbo].[drop_oldest_partition] 
              @partitioned_table_database = N''Your database name''
            , @partitioned_table_schema = N''Your table schema''
            , @partitioned_table_name = N''Your table name''
            ;												    

===================================================================================================*/ 
CREATE PROCEDURE [part].[drop_oldest_partition] 
  @partitioned_table_database sysname = NULL
, @partitioned_table_schema sysname = NULL
, @partitioned_table_name sysname = NULL
AS 
BEGIN TRY

  DECLARE 
    @Day DATETIME   
  , @Day_Next2 DATETIME   
  , @day_in DATE 
  , @Partition_Number INT             
  , @table_object_id BIGINT 
  , @table_object_aux_id BIGINT 
  , @tablemin INT
  , @tablemax INT 
  , @index_min INT
  , @index_max int 
  , @COUNTER INT 
  , @MAX INT 
  , @compression_state_main NVARCHAR(20) 
  , @compression_state_aux NVARCHAR(20)
  , @db_name sysname
  , @tbl_schema sysname                                                                                                                                                                    
  , @tbl_name sysname
  , @table_full_name NVARCHAR(257)
  , @error_msg VARCHAR(100) 
  , @ParmDefinition NVARCHAR(500)
  , @sql NVARCHAR(4000) 
  , @step NVARCHAR(200)
--  , @database_name NVARCHAR(100) --db name for partioning 
  , @partition_column NVARCHAR(100) --column used for partitioning 
  , @partition_function NVARCHAR(100) --function the code should use 
  , @partition_scheme NVARCHAR (100) --partition scheme tied to table 
  , @partition_retention INT --retention in days Not sure it''s needed 
  , @Compress_Table NVARCHAR(1)   --compress table Y or N 
	,	@Compress_Index NVARCHAR(1) 	--compress table Y or N 
	,	@partition_name NVARCHAR(100)  
  , @dt_now DATETIME 
  ;

--Set defaults and environment values
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

--insert log record
  SET @dt_now = CURRENT_TIMESTAMP;

  EXEC [dba_utils].[part].[partition_activity_log_insert] 
    @object_name = ''drop_oldest_partition''
  , @timestamp_datetime = @dt_now
  , @start_or_stop = ''start''
  ;

--create table variables
  DECLARE @list_of_tables TABLE
  (
    [id] INT NOT NULL IDENTITY(1, 1)
  , [database_name] sysname NOT NULL
  , [table_schema] sysname NOT NULL
  , [table_name] sysname NOT NULL
  , [partition_column] NVARCHAR(100)
  , [partition_function] NVARCHAR(100)
  , [partition_scheme] NVARCHAR(100)
  , [partition_retention] INT
  ) ;

  IF OBJECT_ID(''tempdb..#index_to_rebuild'') IS NOT NULL
    DROP TABLE #index_to_rebuild;

  CREATE TABLE #index_to_rebuild 
  (
    [id] INT NOT NULL IDENTITY(1, 1)
  , [index_name] sysname
  , [data_compression_desc] NVARCHAR(120)
  ) ;

--insert values into @list_of_tables for table information for partitioning
  INSERT INTO @list_of_tables 
  (
    [database_name]
  , [table_schema] 
  , [table_name]
  , [partition_function]
  , [partition_scheme]
  , [partition_column]
  , [partition_retention] 
  ) 
  SELECT  [database_name]
        , [table_schema]
        , [table_name]
        , [partition_function]
        , [partition_scheme]
        , [partition_column]
        , [partition_retention]
  FROM    [part].[partition_tbl_list] 
  WHERE ( [is_active] = ''Y'' ) 
    AND ( 1 = CASE WHEN @partitioned_table_schema IS NULL AND @partitioned_table_name IS NULL THEN 1 
                   WHEN [table_schema] = @partitioned_table_schema AND [table_name] = @partitioned_table_name THEN 1
                   ELSE 0
              END
        ) ;
 
--error check 
  IF ( (SELECT COUNT([table_name]) FROM @list_of_tables) = 0 ) 
    BEGIN 
      RAISERROR (''No active records found in the [part].[partition_tbl_list] table.'', 16, 1) ; 
      RETURN; 
    END 

--Set iteration counters to go through each row of @list_of Tables 
  SET @Max = ( SELECT COUNT(*) FROM @list_of_tables ) ;
  SET @counter = ( SELECT MIN([id]) FROM @list_of_tables ) ;

/*----------------------------------MASTER LOOP START---------------------------------*/
--Core loop for moving through data in @list_of_tables, which is loaded from part.Partition_tbl_List 
  WHILE ( (@Max + 1) > @counter)
	  BEGIN 

--DEBUG:
--PRINT ''Begin Master Loop'' 
 
    --Use table variable as part of original code 
      SELECT  @db_name = [database_name]
            , @tbl_schema = [table_schema]
            , @tbl_name = table_name
            , @partition_column = partition_column
            , @partition_function = partition_function
            , @partition_scheme = partition_scheme
            , @partition_retention = partition_retention
      FROM    @list_of_tables
      WHERE   ( id = @COUNTER ) ;

      SET @table_full_name = @db_name + ''.'' + @tbl_schema + ''.'' + @tbl_name ;

    --get the oldest date for that function 
      SET @ParmDefinition = ''@day_out DATETIME OUTPUT'' ;
      SET @sql = 
      ''SET @day_out = 
       CAST((
            SELECT TOP 1 [value] 
            FROM '' + QUOTENAME(@db_name) + ''.[sys].[partition_range_values] 
            WHERE   function_id = 
            ( 
              SELECT  function_id 
              FROM '' + QUOTENAME(@db_name) + ''.[sys].[partition_functions] 
              WHERE   ( name = '''''' + @partition_function + '''''' ) 
            ) 
            ORDER BY [boundary_id] ASC
          ) AS DATETIME
      );''; 

    --PRINT @sql;

    EXEC [sys].[sp_executesql]
          @sql
        , @ParmDefinition
        , @day_out = @day OUTPUT ;

    --set the number of days we want to keep. If partition value is positive, make value negative. 
      IF ( @partition_retention > 0 )
        BEGIN
		      SET @partition_retention = @partition_retention * -1 ;
        END

    --get today''s date and subtract partition retention, to force value to be negative 
      SELECT  @Day_Next2 = CONVERT(CHAR(10), DATEADD(DAY, @partition_retention, GETDATE()), 120); 

--DEBUG:
--PRINT ''Partition Retention'' 
--PRINT @partition_retention 

    --insert log record
      SET @step = N''@Day: '' + CONVERT(NVARCHAR(20), @Day, 120) + '' @DayNext2: '' + CONVERT(NVARCHAR(20), @Day_Next2, 120);
      SET @dt_now = CURRENT_TIMESTAMP;

    --insert log entry 
      EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
        @group_action = ''drop oldest partition''
      , @database_name = @db_name
      , @table_schema = @tbl_schema
      , @tbl_name = @tbl_name
      , @step_last_run = @step
      , @execute_datetime = @dt_now
      ;

--DEBUG:
--PRINT ''Start Day: FROM'' 
--PRINT CONVERT(NVARCHAR(20), @Day) 
--PRINT ''End Day: TO'' 
--PRINT  CONVERT(NVARCHAR(20), @Day_Next2)  -- as [To]  
--SELECT * FROM @list_of_tables 

/*--------------------------------OUTER DAY LOOP START--------------------------------*/
    --process one day at a time 
      WHILE ( @Day < @Day_Next2 )

        BEGIN

--DEBUG:
--PRINT ''start outer loop''

        --reset all our variables at the start of each loop
          SELECT @partition_number = NULL
               , @sql = NULL
               , @tablemin = NULL
               , @tablemax = NULL 
               ;                                      

        --dynamically find the partition_number
          SELECT @sql = ''USE '' + QUOTENAME(@db_name) + ''; SELECT @partition_number_out = $partition.'' + @partition_function + ''(@day_in)'' ;
          SET @ParmDefinition = N''@day_in date, @partition_number_out int OUTPUT'' ;
          SET @day_in = CAST(@Day AS DATE);

--DEBUG:
--declare @partition_number_out int 
--select @sql, @ParmDefinition  
--select @day_in = @day, @partition_number_out = @partition_number

        --insert a log record
        --@step shows the @day_in value that will be passed in to the @day_in variable
          SET @step = ''USE '' + QUOTENAME(@db_name) + ''; SELECT @partition_number_out = $partition.'' + @partition_function + ''(N'''''' + CONVERT(NVARCHAR(25), @day_in, 120) + ''''''); @ParamDefinition: '' + @ParmDefinition;
          SET @dt_now = CURRENT_TIMESTAMP;

        --insert log entry 
          EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
            @group_action = ''drop oldest partition''
          , @database_name = @db_name
          , @table_schema = @tbl_schema
          , @tbl_name = @tbl_name
          , @step_last_run = @step
          , @execute_datetime = @dt_now
          ;

        --execute the dynamic sql to return the partition number
          EXECUTE sp_executesql 
            @sql
          , @ParmDefinition
          , @day_in = @day_in
          , @partition_number_out = @Partition_Number OUTPUT 
          ;
     
        --use the list of tables attached to the partition_function to walk each prior to merging the function. 
        --Variables left in case more tables are ever added to a partition_scheme/Partition_function  
          SELECT @tablemin = MIN([id]) FROM @list_of_tables where ( [id] = @counter ) ;
          SELECT @tablemax = MAX([id]) FROM @list_of_tables where ( [id] = @counter ) ; 

 /*-------------------------------INNER TABLE LOOP START-------------------------------*/
      --process one table at a time. 
        WHILE ( @tablemin <= @tablemax ) --table loop start

          BEGIN   --begin inner table loop 
--DEBUG:
--PRINT ''begin @table loop'' 

            SET @table_object_id = NULL ;
            SET @table_object_aux_id = NULL ;

          --Value for table set above 
            SELECT @table_object_id = OBJECT_ID(@table_full_name) ;
            SELECT @table_object_aux_id = OBJECT_ID(@table_full_name + ''_aux'') ;
						 
--DEBUG:
--PRINT ''@table_object ID'' 
--PRINT @table_object_id 
--PRINT ''@table_object_aux ID'' 
--PRINT @table_object_aux_id  
				 
--/*========================================================================
--Step 1:  Ensure that aux partition matches compresssion of main
--========================================================================*/
--PRINT ''Step 1''

            SET @compression_state_main = NULL ;
            SET @compression_state_aux = NULL ;
								 
          --Get the compression for the base table.  We need to match this, as the _aux 
          --table will be empty and therefore we can compress it instantly. 
          --index_id types:  1 is the clustered index, 0 is a heap
            SET @sql =
            ''SELECT TOP 1 @compression_state_main_out = [data_compression_desc] '' 
            + ''FROM '' + QUOTENAME(@db_name) + ''.[sys].[partitions] ''
            + ''WHERE   ( [object_id] = '' + CAST(@table_object_id AS NVARCHAR(20))
            + '' ) AND   ( [partition_number] = '' + CAST(@Partition_Number AS NVARCHAR(10))
            + '' ) AND   ( [index_id] IN ( 0, 1 ) ) ;'';

            SET @ParmDefinition = N''@compression_state_main_out NVARCHAR(20) OUTPUT'';

          --execute the dynamic sql to return the @compression_state_main value
            EXECUTE sp_executesql 
              @sql
            , @ParmDefinition
            , @compression_state_main_out = @compression_state_main OUTPUT
            ;

          --Get the compression for the AUX table.
            SET @sql = ''SELECT TOP 1 @compression_state_aux_out = data_compression_desc ''
            + ''FROM '' + QUOTENAME(@db_name) + ''.[sys].[partitions] ''
            + ''WHERE   ( [object_id] = '' + CAST(@table_object_aux_id AS NVARCHAR(20))
            + '' ) AND   ( [partition_number] = '' + CAST(@Partition_Number AS NVARCHAR(10))
            + '' ) AND   ( [index_id] IN ( 0, 1 ) ) ;'';

            SET @ParmDefinition = N''@compression_state_aux_out NVARCHAR(20) OUTPUT'';

          --execute the dynamic sql to return the @compression_state_main value
            EXECUTE sp_executesql 
              @sql
            , @ParmDefinition
            , @compression_state_aux_out = @compression_state_aux OUTPUT
            ;

--DEBUG:
--PRINT ''A'' 
--PRINT ''@compression_state_main'' 
--PRINT @compression_state_main 
--PRINT ''@compression_state_aux'' 
--PRINT @compression_state_aux 
--select * from sys.partitions 

            SET @step = N''Compression_state_main: '' + @compression_state_main + '' Compression_state_main_aux: '' + @compression_state_aux ;
            SET @dt_now = CURRENT_TIMESTAMP;

          --insert log entry 
            EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
              @group_action = ''drop oldest partition''
            , @database_name = @db_name
            , @table_schema = @tbl_schema
            , @tbl_name = @tbl_name
            , @step_last_run = @step
            , @execute_datetime = @dt_now
            ;

          --isnull is in case the partition doesn''t exist; don''t need it now, but might later. 
            PRINT ''Begin Index Rebuild'';
           
            IF ( ISNULL(@compression_state_aux, ''NULL'') <> ISNULL(@compression_state_main, ''NULL'') )
            --fix it!  Make the aux table the same since that''ll go a lot quicker. 
              BEGIN

                SELECT @sql = NULL ;

              --Compress the table 
                SELECT @sql = ''ALTER TABLE '' + @table_full_name + ''_aux REBUILD PARTITION = '' + CONVERT(VARCHAR(10),@partition_number) 
				        + '' WITH (DATA_COMPRESSION = '' + @compression_state_main + '')''; --set to the SCD value

                SET @dt_now = CURRENT_TIMESTAMP;

              --insert log entry 
                EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
                  @group_action = ''drop oldest partition''
                , @database_name = @db_name
                , @table_schema = @tbl_schema
                , @tbl_name = @tbl_name
                , @step_last_run = @sql
                , @execute_datetime = @dt_now
                ;

--DEBUG:
--PRINT @sql 

                EXEC (@sql) ;

              --now they should match, and we can switch the table partition out. 

              END

/*============================================================================
Step 2:  Verify index compression matches.  Assumes indexes with same name.
============================================================================*/
--PRINT ''B''
--PRINT ''Step 2''

          --Dump data for each iteration
            TRUNCATE TABLE #index_to_rebuild;  

          --Get list of indexes that don''t have same compression level   
            SELECT @sql = 
            ''INSERT INTO #index_to_rebuild (index_name, data_compression_desc) ''
            + ''SELECT  [aux].name, main.data_compression_desc FROM ''
            + ''( SELECT  idx.name, part.data_compression_desc ''
            + ''FROM '' + QUOTENAME(@db_name) + ''.sys.partitions part ''
            + ''INNER JOIN '' + QUOTENAME(@db_name) + ''.sys.indexes idx ON part.object_id = idx.object_id AND part.index_id = idx.index_id ''
            + ''AND part.object_id = OBJECT_ID('''''' + @table_full_name
            + '''''') AND part.partition_number = '' + CAST(@Partition_Number AS NVARCHAR(10))
            + '' AND ( idx.index_id IN ( 0, 1 ) ) ) [main] ''
            + ''INNER JOIN ( SELECT  idx.name, part.data_compression_desc ''
            + ''FROM '' + QUOTENAME(@db_name) + ''.sys.partitions part ''
            + ''INNER JOIN '' + QUOTENAME(@db_name) + ''.sys.indexes idx ON part.object_id = idx.object_id AND part.index_id = idx.index_id ''
            + ''AND part.object_id = OBJECT_ID('''''' + @table_full_name + ''_aux'''') ''  
            + ''AND part.partition_number = '' + CAST(@Partition_Number AS NVARCHAR(10))
            + '' AND idx.index_id NOT IN ( 0, 1 ) ) [aux] ''
            + ''ON  ( main.name = aux.name ) AND ( main.data_compression_desc <> aux.data_compression_desc ) ''
            + ''AND ( aux.data_compression_desc <> ''''COLUMNSTORE'''' ) ;'';

            EXEC [sys].[sp_executesql] @sql;
--DEBUG:
--PRINT ''C'' 

          --if there are indexes that don''t match our set compression, alter _compression so that it matches.   
          --could do an ALTER INDEX ALL, but if any are compressed it would recompress them - wasting time, IO, & CPU.   
            SELECT  @index_min = NULL
                  , @index_max = NULL
                  ;

          --If more than one index doesn''t match get indexes
            SELECT  @index_min = MIN(id)
                  , @index_max= MAX(id) 
            FROM    #index_to_rebuild ;

          --loop throgh the indexes and compress any that need to be compressed
/*------------------------------index check loop start------------------------------*/
            WHILE ( @index_min <= @index_max )
              BEGIN   
--DEBUG:
--PRINT ''Index Loop''

                SELECT @sql = 
                ''ALTER INDEX '' + index_name    
                  + '' ON '' + @table_full_name + ''_aux'' --stg table always since it''s small   
                  + '' Rebuild Partition = '' + CONVERT(VARCHAR(10), @partition_number)   
                  + '' With (Data_Compression = '' + data_compression_desc + '')''   
                  FROM #index_to_rebuild  
                  WHERE ( [id] = @index_min ) ;
 
              --insert log record
                SET @step = ''Rebuild Index: '' + @sql;
                SET @dt_now = CURRENT_TIMESTAMP;

                EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
                  @group_action = ''drop oldest partition''
                , @database_name = @db_name
                , @table_schema = @tbl_schema
                , @tbl_name = @tbl_name
                , @step_last_run = @step
                , @execute_datetime = @dt_now
                ;

--DEBUG:
--PRINT @sql ;
                EXEC (@sql) ;

                SET @index_min = @index_min + 1 ;
                    
						  END  --End index check loop 
/*------------------------------ index check loop end ------------------------------*/

/*============================================================================
Step 3:  Move the data for 2nd FAR LEFT Partition from table to aux table.
============================================================================*/
--DEBUG:
--PRINT ''D'' 
--PRINT ''Step 3''
--PRINT ''Switch partition''
 
						SELECT @sql = NULL ; 
            SELECT @sql = ''USE '' + QUOTENAME(@db_name) + ''; SET DEADLOCK_PRIORITY HIGH; ALTER TABLE '' + @table_full_name 
                        + '' SWITCH PARTITION '' + CONVERT(VARCHAR(10), @partition_number) 
                        + '' TO '' + @table_full_name + ''_aux PARTITION '' + CONVERT(VARCHAR(10), @partition_number) 
                        + ''; SET DEADLOCK_PRIORITY NORMAL'' ;

          --using deadlock_priority to ensure that this kills any queries, and not the other way around. 
          --twice to ensure it actually runs properly; outside the SQL didn''t always work, 
          --but couldn''t duplicate with inside. 
            BEGIN TRY 

            --insert log record
              SET @step = ''Switch Table '' + @sql;
              SET @dt_now = CURRENT_TIMESTAMP;

              EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
                @group_action = ''drop oldest partition''
              , @database_name = @db_name
              , @table_schema = @tbl_schema
              , @tbl_name = @tbl_name
              , @step_last_run = @step
              , @execute_datetime = @dt_now
              ;

              SET DEADLOCK_PRIORITY HIGH 
              EXEC (@sql) ;
              SET DEADLOCK_PRIORITY NORMAL 

            END TRY  
            BEGIN CATCH     

              SELECT 
              --  ERROR_NUMBER() AS ErrorNumber 
              --, ERROR_SEVERITY() AS ErrorSeverity 
              --, ERROR_STATE() AS ErrorState 
              --, ERROR_PROCEDURE() AS ErrorProcedure 
              --, ERROR_LINE() AS ErrorLine 
                @error_msg = ERROR_MESSAGE() --AS ErrorMessage 
              --, @sql AS [sql_statement]
              ; 

            --set @error_msg =  
              SET @error_msg = @error_msg + ''Error SWITCHING partition out for '' 
              + @table_full_name + ''.  Step 3 executed code: '' + @SQL ;

            --insert log record
              SET @step = ''Switch Table '' + @sql;
              SET @dt_now = CURRENT_TIMESTAMP;

              EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
                @group_action = ''drop oldest partition''
              , @database_name = @db_name
              , @table_schema = @tbl_schema
              , @tbl_name = @tbl_name
              , @step_last_run = @error_msg
              , @execute_datetime = @dt_now
              ;

              RAISERROR (@error_msg, 11, 1) ;
              RETURN    --return here so that it doesn''t continue.
 
            END CATCH 

--DEBUG:
--PRINT ''E''

          --Delete the data on the auxillary table.  
          --Do before merge so that it''s faster; metadata change only 
            SELECT @sql = NULL ;

--DEBUG:
--PRINT ''Truncate @table_full_name'' 
--PRINT @table_full_name 
 
					--TRUNCATE AUX Table, must be empty before switching
            SELECT @sql =  ''TRUNCATE TABLE '' + @table_full_name + ''_aux'' ;

          --insert log record
            SET @dt_now = CURRENT_TIMESTAMP;

            EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
              @group_action = ''drop oldest partition''
            , @database_name = @db_name
            , @table_schema = @tbl_schema
            , @tbl_name = @tbl_name
            , @step_last_run = @sql
            , @execute_datetime = @dt_now
            ;

            EXEC (@sql) ;
 
          --go to the next table 
            SET @tablemin = @tablemin + 1 ;

--DEBUG:
--Print ''F'' 
--print ''end loop for tables'' 

          END
/*--------------------------------INNER TABLE LOOP END--------------------------------*/

/*============================================================================
Step 4:  Merge the 1st and 2nd partitions of the function.
         Now that all tables have been emptied, this will be fast.
============================================================================*/
        SET @sql = ''USE '' + QUOTENAME(@db_name) + ''; SET DEADLOCK_PRIORITY HIGH; ALTER PARTITION FUNCTION '' 
                 + @partition_function + ''() MERGE RANGE (@Day_In) ; SET DEADLOCK_PRIORITY NORMAL; '' ;
        SET @ParmDefinition = N''@day_in DATE'' ;

        BEGIN TRY  
        --in here twice because even when deadlock_priority was within the @sql it failed 

--DEBUG: 
--PRINT ''G'' 
--PRINT ''Alter Partition Function'' 
--PRINT @Day 
--PRINT @sql 

        --insert log record
          SET @step = @sql + ''@ParmDefinition : @day_in = '' + CONVERT(NVARCHAR(20), @Day, 120) ;
          SET @dt_now = CURRENT_TIMESTAMP;

          EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
            @group_action = ''drop oldest partition''
          , @database_name = @db_name
          , @table_schema = @tbl_schema
          , @tbl_name = @tbl_name
          , @step_last_run = @sql
          , @execute_datetime = @dt_now
          ;

	      --insert log record
          SET @step = ''Merge: '' + @sql ;

          EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
            @group_action = ''drop oldest partition''
          , @database_name = @db_name
          , @table_schema = @tbl_schema
          , @tbl_name = @tbl_name
          , @step_last_run = @sql
          , @execute_datetime = @dt_now
          ;
			
          SET DEADLOCK_PRIORITY HIGH; 
          EXECUTE sp_executesql 
            @sql
          , @ParmDefinition
          , @day_in = @day 
          ;
				  SET DEADLOCK_PRIORITY NORMAL ;  

        END TRY 
        BEGIN CATCH                                   

          DECLARE 
            @ErrorMessage NVARCHAR(MAX) 
          , @ErrorSeverity INT
          , @ErrorState INT 
					;			 

          SELECT @ErrorSeverity = ERROR_SEVERITY()
               , @ErrorState = ERROR_STATE() 
               , @errormessage = ERROR_MESSAGE() + ''Step 4 (merge partition range) failed. CODE: '' + @SQL 
               ;

          RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState) ; 
          RETURN ;
                                
        END CATCH
        
        IF ( @@error = 0 )
          BEGIN

          --insert log record reporting success
            SET @step = CONVERT(CHAR(8), @Day, 112) + '' has been successfully merged for function '' + @partition_function ;
            SET @dt_now = CURRENT_TIMESTAMP;

            EXEC [dba_utils].[part].[partition_activity_steps_log_insert]
              @group_action = ''drop oldest partition''
            , @database_name = @db_name
            , @table_schema = @tbl_schema
            , @tbl_name = @tbl_name
            , @step_last_run = @step
            , @execute_datetime = @dt_now
            ;

          END

          PRINT @step ;

--DEBUG:
--Print ''H''
 
            SET @Day = (SELECT DATEADD(dd , 1, @Day) ); 
  
          END  
/*---------------------------------OUTER DAY LOOP END---------------------------------*/

      SET @counter = @counter + 1 ;
      SET @Day_Next2 = NULL ;

    END
/*-----------------------------------MASTER LOOP END----------------------------------*/

  --update log record created when the master loop started
    SET @dt_now = CURRENT_TIMESTAMP;

  	EXEC [dba_utils].[part].[partition_activity_log_insert] 
      @object_name = ''drop_oldest_partition''
  	, @timestamp_datetime = @dt_now
  	, @start_or_stop = ''stop''
    ;

--DEBUG:
--Print ''I'' 

END TRY  
BEGIN CATCH 
	DECLARE  
    @errNum INT 
	, @errSeverity INT 
	, @errState INT 
	, @errProc NVARCHAR(128) 
	, @errLine INT 
  , @errMsg NVARCHAR(4000) 
	, @commitState NVARCHAR(100) 
	, @returnedMsg NVARCHAR(2047) 
	; 
 
	SET @commitState = ''''; 
 
--Process depending on whether the transaction is uncommittable or committable. 
  IF (XACT_STATE()) = -1 
  BEGIN 
      SET @commitState = N''Transaction was in an uncommittable state and was rolled back. ''  
      ROLLBACK TRANSACTION; 
  END; 
 
--Test whether the transaction is committable. 
  IF (XACT_STATE()) = 1 
    BEGIN 
        SET @commitState = N''Transaction was committable and was committed. ''  
        COMMIT TRANSACTION;    
    END; 
 
	SELECT	@errNum = ERROR_NUMBER() 
				, @errSeverity = ERROR_SEVERITY() 
				, @errState = ERROR_STATE()  
				, @errProc = ERROR_PROCEDURE()  
				, @errLine = ERROR_LINE()  
				, @errMsg = ERROR_MESSAGE() 
				; 
 
	SELECT @returnedMsg = N''Error '' + CONVERT(VARCHAR(10), @errNum) + '' occurred on line '' +  
	CONVERT(VARCHAR(10), @errLine) + '' with error state '' + CONVERT(NVARCHAR(10), @errState) 
	+ '' in procedure '' + ISNULL(@errProc, ''/script'') + ''. '' + @commitState; 
	SET @returnedMsg = @returnedMsg + SUBSTRING(@errMsg, 1, 2047 - LEN(@returnedMsg)); 
 
	IF (@errState = 0)  
		BEGIN 
			SET @errState = 1; 
		END 
 
	RAISERROR(@returnedMsg, @errSeverity, @errState); 
 
END CATCH 
'
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[partition_activity_log_aux]'
GO
IF OBJECT_ID(N'[part].[partition_activity_log_aux]', 'U') IS NULL
CREATE TABLE [part].[partition_activity_log_aux]
(
[id] [int] NOT NULL IDENTITY(1, 1),
[proc_name] [sys].[sysname] NOT NULL,
[start_time] [datetime] NOT NULL,
[end_time] [datetime] NULL,
[start_or_stop] [char] (5) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[insert_datetime] [datetime] NOT NULL CONSTRAINT [DF_partition_activity_log_aux_insert_datetime] DEFAULT (getdate())
) ON [ps_partition_activity_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating index [clidx_partition_activity_log_aux] on [part].[partition_activity_log_aux]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'clidx_partition_activity_log_aux' AND object_id = OBJECT_ID(N'[part].[partition_activity_log_aux]'))
CREATE CLUSTERED INDEX [clidx_partition_activity_log_aux] ON [part].[partition_activity_log_aux] ([insert_datetime] DESC) ON [ps_partition_activity_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating primary key [PK_partition_activity_log_aux] on [part].[partition_activity_log_aux]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'PK_partition_activity_log_aux' AND object_id = OBJECT_ID(N'[part].[partition_activity_log_aux]'))
ALTER TABLE [part].[partition_activity_log_aux] ADD CONSTRAINT [PK_partition_activity_log_aux] PRIMARY KEY NONCLUSTERED  ([id], [insert_datetime]) ON [ps_partition_activity_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [part].[partition_activity_steps_log_aux]'
GO
IF OBJECT_ID(N'[part].[partition_activity_steps_log_aux]', 'U') IS NULL
CREATE TABLE [part].[partition_activity_steps_log_aux]
(
[id] [int] NOT NULL IDENTITY(1, 1),
[group_action] [nvarchar] (100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
[database_name] [sys].[sysname] NOT NULL,
[table_schema] [sys].[sysname] NOT NULL,
[table_name] [sys].[sysname] NOT NULL,
[step_last_run] [nvarchar] (1500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[last_activity] [datetime] NOT NULL CONSTRAINT [DF_partition_activity_steps_log_aux_last_activity] DEFAULT (getdate()),
[insert_datetime] [datetime] NOT NULL CONSTRAINT [DF_partition_activity_steps_log_aux_insert_datetime] DEFAULT (getdate())
) ON [ps_partition_activity_steps_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating index [clidx_partition_activity_steps_log_aux] on [part].[partition_activity_steps_log_aux]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'clidx_partition_activity_steps_log_aux' AND object_id = OBJECT_ID(N'[part].[partition_activity_steps_log_aux]'))
CREATE CLUSTERED INDEX [clidx_partition_activity_steps_log_aux] ON [part].[partition_activity_steps_log_aux] ([insert_datetime] DESC, [id] DESC) ON [ps_partition_activity_steps_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating primary key [PK_partition_activity_steps_log_aux] on [part].[partition_activity_steps_log_aux]'
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'PK_partition_activity_steps_log_aux' AND object_id = OBJECT_ID(N'[part].[partition_activity_steps_log_aux]'))
ALTER TABLE [part].[partition_activity_steps_log_aux] ADD CONSTRAINT [PK_partition_activity_steps_log_aux] PRIMARY KEY NONCLUSTERED  ([id], [insert_datetime]) ON [ps_partition_activity_steps_log] ([insert_datetime])
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
COMMIT TRANSACTION
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
DECLARE @Success AS BIT
SET @Success = 1
SET NOEXEC OFF
IF (@Success = 1) PRINT 'The database update succeeded'
ELSE BEGIN
	IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
	PRINT 'The database update failed'
END
GO
