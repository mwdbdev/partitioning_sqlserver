USE [msdb]
GO

/****** Object:  Job [Partition Maintenance]    Script Date: 4/12/2018 8:27:37 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
, @sa_name sysname
, @int_date INT
;

--set variable values
SELECT @sa_name = [name] 
FROM [sys].[server_principals] [sp]
WHERE ([sp].[sid] = 0x01) ;

SELECT @int_date = CAST(CAST(YEAR(GETDATE()) AS CHAR(4)) + RIGHT('0' + CAST(MONTH(GETDATE()) AS VARCHAR(2)), 2) + RIGHT('0' + CAST(DAY(GETDATE()) AS VARCHAR(2)), 2) AS INT);

SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 4/12/2018 8:27:37 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Database Maintenance' AND category_class=1)
BEGIN
  EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Database Maintenance';
  IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback;
END

DECLARE @jobId BINARY(16);
EXEC @ReturnCode = msdb.dbo.sp_add_job 
    @job_name=N'Partition Maintenance', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'[Priority2] Dynamic partitioning process adds and removes partitions.  Job can be rerun.  Notify Pharmacy Data Services if job fails more than three times for a reason other than deadlock.', 
		@category_name=N'Database Maintenance', 
		@owner_login_name=@sa_name, 
    @job_id = @jobId OUTPUT;
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Add future partitions]    Script Date: 4/12/2018 8:27:38 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
    @job_id=@jobId, 
    @step_name=N'Add future partitions', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE [dba_utils].[part].[add_future_partition] @partitioned_table_database = NULL, @partitioned_table_schema = NULL, @partitioned_table_name = NULL;', 
		@database_name=N'dba_utils', 
		@output_file_name=N'E:\SQL_Log\partition_maint_01_add_future_partitions', 
		@flags=2;
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Delete old partitions]    Script Date: 4/12/2018 8:27:38 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep 
    @job_id=@jobId, 
    @step_name=N'Delete old partitions', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'EXECUTE [dba_utils].[part].[drop_oldest_partition] @partitioned_table_database = NULL, @partitioned_table_schema = NULL, @partitioned_table_name = NULL;', 
		@database_name=N'dba_utils', 
		@output_file_name=N'E:\SQL_Log\partition_maint_02_drop_old_partitions.log', 
		@flags=2;
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule 
    @job_id=@jobId, 
    @name=N'Run Daily at 12:05:55 am', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=@int_date, 
		@active_end_date=99991231, 
		@active_start_time=555, 
		@active_end_time=235959;
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)';
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


