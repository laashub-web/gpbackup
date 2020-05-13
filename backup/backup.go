package backup

import (
	"fmt"
	"os"
	path "path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/report"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/cobra"
)

// This function handles setup that can be done before parsing flags.
func DoInit(cmd *cobra.Command) {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gpbackup", "")
	SetCmdFlags(cmd.Flags())
	_ = cmd.MarkFlagRequired(options.DBNAME)
	utils.InitializeSignalHandler(DoCleanup, "backup process", &wasTerminated)
	objectCounts = make(map[string]int)
}

func DoFlagValidation(cmd *cobra.Command) {
	validateFlagCombinations(cmd.Flags())
	validateFlagValues()
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	gplog.Verbose("Backup Command: %s", os.Args)

	utils.CheckGpexpandRunning(utils.BackupPreventedByGpexpandMessage)
	timestamp := history.CurrentTimestamp()
	createBackupLockFile(timestamp)
	initializeConnectionPool()

	gplog.Info("Starting backup of database %s", MustGetFlagString(options.DBNAME))
	opts, err := options.NewOptions(cmdFlags)
	gplog.FatalOnError(err)

	validateFilterLists(opts)

	err = opts.ExpandIncludesForPartitions(connectionPool, cmdFlags)
	gplog.FatalOnError(err)

	segConfig := cluster.MustGetSegmentConfiguration(connectionPool)
	globalCluster = cluster.NewCluster(segConfig)
	segPrefix := filepath.GetSegPrefix(connectionPool)
	globalFPInfo = filepath.NewFilePathInfo(globalCluster, MustGetFlagString(options.BACKUP_DIR), timestamp, segPrefix)
	if MustGetFlagBool(options.METADATA_ONLY) {
		_, err = globalCluster.ExecuteLocalCommand(fmt.Sprintf("mkdir -p %s", globalFPInfo.GetDirForContent(-1)))
		gplog.FatalOnError(err)
	} else {
		createBackupDirectoriesOnAllHosts()
	}
	globalTOC = &toc.TOC{}
	globalTOC.InitializeMetadataEntryMap()
	utils.InitializePipeThroughParameters(!MustGetFlagBool(options.NO_COMPRESSION), MustGetFlagInt(options.COMPRESSION_LEVEL))
	getQuotedRoleNames(connectionPool)

	pluginConfigFlag := MustGetFlagString(options.PLUGIN_CONFIG)

	if pluginConfigFlag != "" {
		pluginConfig, err = utils.ReadPluginConfig(pluginConfigFlag)
		gplog.FatalOnError(err)
		configFilename := path.Base(pluginConfig.ConfigPath)
		configDirname := path.Dir(pluginConfig.ConfigPath)
		pluginConfig.ConfigPath = path.Join(configDirname, timestamp+"_"+configFilename)
		_ = cmdFlags.Set(options.PLUGIN_CONFIG, pluginConfig.ConfigPath)
		gplog.Debug("Plugin config path: %s", pluginConfig.ConfigPath)
	}

	initializeBackupReport(*opts)

	if pluginConfigFlag != "" {
		backupReport.PluginVersion = pluginConfig.CheckPluginExistsOnAllHosts(globalCluster)
		pluginConfig.CopyPluginConfigToAllHosts(globalCluster)
		pluginConfig.SetupPluginForBackup(globalCluster, globalFPInfo)
	}
}

func DoBackup() {
	gplog.Info("Backup Timestamp = %s", globalFPInfo.Timestamp)
	gplog.Info("Backup Database = %s", connectionPool.DBName)
	gplog.Verbose("Backup Parameters: {%s}", strings.ReplaceAll(backupReport.BackupParamsString, "\n", ", "))

	pluginConfigFlag := MustGetFlagString(options.PLUGIN_CONFIG)
	targetBackupTimestamp := ""
	var targetBackupFPInfo filepath.FilePathInfo
	if MustGetFlagBool(options.INCREMENTAL) {
		targetBackupTimestamp = GetTargetBackupTimestamp()
		targetBackupFPInfo = filepath.NewFilePathInfo(globalCluster, globalFPInfo.UserSpecifiedBackupDir,
			targetBackupTimestamp, globalFPInfo.UserSpecifiedSegPrefix)

		if pluginConfigFlag != "" {
			// These files need to be downloaded from the remote system into the local filesystem
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetConfigFilePath())
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetTOCFilePath())
			pluginConfig.MustRestoreFile(targetBackupFPInfo.GetPluginConfigPath())
		}
	}

	gplog.Info("Gathering table state information")
	metadataTables, dataTables := RetrieveAndProcessTables()
	if !(MustGetFlagBool(options.METADATA_ONLY) || MustGetFlagBool(options.DATA_ONLY)) {
		backupIncrementalMetadata()
	}
	CheckTablesContainData(dataTables)
	metadataFilename := globalFPInfo.GetMetadataFilePath()
	gplog.Info("Metadata will be written to %s", metadataFilename)
	metadataFile := utils.NewFileWithByteCountFromFile(metadataFilename)

	backupSessionGUC(metadataFile)
	if !MustGetFlagBool(options.DATA_ONLY) {
		isFullBackup := len(MustGetFlagStringArray(options.INCLUDE_RELATION)) == 0
		if isFullBackup && !MustGetFlagBool(options.WITHOUT_GLOBALS) {
			backupGlobals(metadataFile)
		}

		isFilteredBackup := !isFullBackup
		backupPredata(metadataFile, metadataTables, isFilteredBackup)
		backupPostdata(metadataFile)
	}

	/*
	 * We check this in the backup report rather than the flag because we
	 * perform a metadata only backup if the database contains no tables
	 * or only external tables
	 */
	if !backupReport.MetadataOnly {
		backupSetTables := dataTables

		targetBackupRestorePlan := make([]history.RestorePlanEntry, 0)
		if targetBackupTimestamp != "" {
			gplog.Info("Basing incremental backup off of backup with timestamp = %s", targetBackupTimestamp)

			targetBackupTOC := toc.NewTOC(targetBackupFPInfo.GetTOCFilePath())
			targetBackupRestorePlan = history.ReadConfigFile(targetBackupFPInfo.GetConfigFilePath()).RestorePlan
			backupSetTables = FilterTablesForIncremental(targetBackupTOC, globalTOC, dataTables)
		}

		backupReport.RestorePlan = PopulateRestorePlan(backupSetTables, targetBackupRestorePlan, dataTables)
		backupData(backupSetTables)
	}

	if MustGetFlagBool(options.WITH_STATS) {
		backupStatistics(metadataTables)
	}

	globalTOC.WriteToFileAndMakeReadOnly(globalFPInfo.GetTOCFilePath())
	for connNum := 0; connNum < connectionPool.NumConns; connNum++ {
		// COMMIT TRANSACTION
		connectionPool.MustCommit(connNum)
	}
	metadataFile.Close()
	if pluginConfigFlag != "" {
		pluginConfig.MustBackupFile(metadataFilename)
		pluginConfig.MustBackupFile(globalFPInfo.GetTOCFilePath())
		if MustGetFlagBool(options.WITH_STATS) {
			pluginConfig.MustBackupFile(globalFPInfo.GetStatisticsFilePath())
		}
		_ = utils.CopyFile(pluginConfigFlag, globalFPInfo.GetPluginConfigPath())
		pluginConfig.MustBackupFile(globalFPInfo.GetPluginConfigPath())
	}

	err := history.WriteBackupHistory(globalFPInfo.GetBackupHistoryFilePath(), &backupReport.BackupConfig)
	gplog.FatalOnError(err)
}

func backupGlobals(metadataFile *utils.FileWithByteCount) {
	gplog.Info("Writing global database metadata")

	backupResourceQueues(metadataFile)
	backupResourceGroups(metadataFile)
	backupRoles(metadataFile)
	backupRoleGrants(metadataFile)
	backupTablespaces(metadataFile)
	backupCreateDatabase(metadataFile)
	backupDatabaseGUCs(metadataFile)
	backupRoleGUCs(metadataFile)

	logCompletionMessage("Global database metadata backup")
}

func backupPredata(metadataFile *utils.FileWithByteCount, tables []Table, tableOnly bool) {
	if wasTerminated {
		return
	}
	gplog.Info("Writing pre-data metadata")

	objects := make([]Sortable, 0)
	metadataMap := make(MetadataMap)
	objects = append(objects, convertToSortableSlice(tables)...)
	relationMetadata := GetMetadataForObjectType(connectionPool, TYPE_RELATION)
	addToMetadataMap(relationMetadata, metadataMap)
	functions, funcInfoMap := retrieveFunctions(&objects, metadataMap)
	protocols := retrieveProtocols(&objects, metadataMap)

	if !tableOnly {
		backupSchemas(metadataFile, createAlteredPartitionSchemaSet(tables))
		backupExtensions(metadataFile)
		backupCollations(metadataFile)
		retrieveAndBackupTypes(metadataFile, &objects, metadataMap)

		if len(MustGetFlagStringArray(options.INCLUDE_SCHEMA)) == 0 {
			backupProceduralLanguages(metadataFile, functions, funcInfoMap, metadataMap)
			retrieveFDWObjects(&objects, metadataMap)
		}

		retrieveTSObjects(&objects, metadataMap)
		backupOperatorFamilies(metadataFile)
		retrieveOperatorObjects(&objects, metadataMap)
		retrieveAggregates(&objects, metadataMap)
		retrieveCasts(&objects, metadataMap)
	}

	retrieveViews(&objects)
	sequences := retrieveAndBackupSequences(metadataFile, relationMetadata)
	constraints, conMetadata := retrieveConstraints()

	backupDependentObjects(metadataFile, tables, protocols, metadataMap, constraints, objects, sequences, funcInfoMap, tableOnly)

	backupConversions(metadataFile)
	backupConstraints(metadataFile, constraints, conMetadata)

	logCompletionMessage("Pre-data metadata metadata backup")
}

func backupData(tables []Table) {
	if len(tables) == 0 {
		// No incremental data changes to backup
		gplog.Info("No tables to backup")
		gplog.Info("Data backup complete")
		return
	}

	if MustGetFlagBool(options.SINGLE_DATA_FILE) {
		gplog.Verbose("Initializing pipes and gpbackup_helper on segments for single data file backup")
		utils.VerifyHelperVersionOnSegments(version, globalCluster)
		oidList := make([]string, 0, len(tables))
		for _, table := range tables {
			if !table.SkipDataBackup() {
				oidList = append(oidList, fmt.Sprintf("%d", table.Oid))
			}
		}
		utils.WriteOidListToSegments(oidList, globalCluster, globalFPInfo)
		utils.CreateFirstSegmentPipeOnAllHosts(oidList[0], globalCluster, globalFPInfo)
		compressStr := fmt.Sprintf(" --compression-level %d", MustGetFlagInt(options.COMPRESSION_LEVEL))
		if MustGetFlagBool(options.NO_COMPRESSION) {
			compressStr = " --compression-level 0"
		}
		// Do not pass through the --on-error-continue flag because it does not apply to gpbackup
		utils.StartGpbackupHelpers(globalCluster, globalFPInfo, "--backup-agent",
			MustGetFlagString(options.PLUGIN_CONFIG), compressStr, false, false)
	}
	gplog.Info("Writing data to file")
	rowsCopiedMaps := backupDataForAllTables(tables)
	AddTableDataEntriesToTOC(tables, rowsCopiedMaps)
	if MustGetFlagBool(options.SINGLE_DATA_FILE) && MustGetFlagString(options.PLUGIN_CONFIG) != "" {
		pluginConfig.BackupSegmentTOCs(globalCluster, globalFPInfo)
	}

	logCompletionMessage("Data backup")
}

func backupPostdata(metadataFile *utils.FileWithByteCount) {
	if wasTerminated {
		return
	}
	gplog.Info("Writing post-data metadata")

	backupIndexes(metadataFile)
	backupRules(metadataFile)
	backupTriggers(metadataFile)
	if connectionPool.Version.AtLeast("6") {
		backupDefaultPrivileges(metadataFile)
		if len(MustGetFlagStringArray(options.INCLUDE_SCHEMA)) == 0 {
			backupEventTriggers(metadataFile)
		}
	}

	logCompletionMessage("Post-data metadata backup")
}

func backupStatistics(tables []Table) {
	if wasTerminated {
		return
	}
	statisticsFilename := globalFPInfo.GetStatisticsFilePath()
	gplog.Info("Writing query planner statistics to %s", statisticsFilename)
	statisticsFile := utils.NewFileWithByteCountFromFile(statisticsFilename)
	defer statisticsFile.Close()
	backupTableStatistics(statisticsFile, tables)

	logCompletionMessage("Query planner statistics backup")
}

func DoTeardown() {
	backupFailed := false
	defer func() {
		DoCleanup(backupFailed)

		errorCode := gplog.GetErrorCode()
		if errorCode == 0 {
			gplog.Info("Backup completed successfully")
		}
		os.Exit(errorCode)
	}()

	errStr := ""
	if err := recover(); err != nil {
		// gplog's Fatal will cause a panic with error code 2
		if gplog.GetErrorCode() != 2 {
			gplog.Error(fmt.Sprintf("%v: %s", err, debug.Stack()))
			gplog.SetErrorCode(2)
		} else {
			errStr = fmt.Sprintf("%v", err)
		}
		backupFailed = true
	}
	if wasTerminated {
		/*
		 * Don't print an error or create a report file if the backup was canceled,
		 * as the signal handler will take care of cleanup and return codes.  Just
		 * wait until the signal handler's DoCleanup completes so the main goroutine
		 * doesn't exit while cleanup is still in progress.
		 */
		CleanupGroup.Wait()
		backupFailed = true
		return
	}
	if errStr != "" {
		fmt.Println(errStr)
	}
	errMsg := report.ParseErrorMessage(errStr)

	/*
	 * Only create a report file if we fail after the cluster is initialized
	 * and a backup directory exists in which to create the report file.
	 */
	if globalFPInfo.Timestamp != "" {
		_, statErr := os.Stat(globalFPInfo.GetDirForContent(-1))
		if statErr != nil { // Even if this isn't os.IsNotExist, don't try to write a report file in case of further errors
			return
		}
		reportFilename := globalFPInfo.GetBackupReportFilePath()
		configFilename := globalFPInfo.GetConfigFilePath()

		time.Sleep(time.Second) // We sleep for 1 second to ensure multiple backups do not start within the same second.

		if backupReport != nil {
			backupReport.ConstructBackupParamsString()
			history.WriteConfigFile(&backupReport.BackupConfig, configFilename)
			if backupReport.BackupConfig.EndTime == "" {
				backupReport.BackupConfig.EndTime = history.CurrentTimestamp()
			}
			endtime, _ := time.ParseInLocation("20060102150405", backupReport.BackupConfig.EndTime, operating.System.Local)
			backupReport.WriteBackupReportFile(reportFilename, globalFPInfo.Timestamp, endtime, objectCounts, errMsg)
			report.EmailReport(globalCluster, globalFPInfo.Timestamp, reportFilename, "gpbackup")
			if pluginConfig != nil {
				err := pluginConfig.BackupFile(configFilename)
				if err != nil {
					gplog.Error(fmt.Sprintf("%v", err))
					return
				}
				err = pluginConfig.BackupFile(reportFilename)
				if err != nil {
					gplog.Error(fmt.Sprintf("%v", err))
					return
				}
			}
		}
		if pluginConfig != nil {
			pluginConfig.CleanupPluginForBackup(globalCluster, globalFPInfo)
			pluginConfig.DeletePluginConfigWhenEncrypting(globalCluster)
		}
	}
}

func DoCleanup(backupFailed bool) {
	defer func() {
		if err := recover(); err != nil {
			gplog.Warn("Encountered error during cleanup: %v", err)
		}
		gplog.Verbose("Cleanup complete")
		CleanupGroup.Done()
	}()

	gplog.Verbose("Beginning cleanup")
	if globalFPInfo.Timestamp != "" {
		if MustGetFlagBool(options.SINGLE_DATA_FILE) {
			if backupFailed {
				// Cleanup only if terminated or fataled
				utils.CleanUpSegmentHelperProcesses(globalCluster, globalFPInfo, "backup")
			}
			if wasTerminated {
				// It is possible for the COPY command to become orphaned if an agent process is killed
				utils.TerminateHangingCopySessions(connectionPool, globalFPInfo, "gpbackup")
			}
			utils.CleanUpHelperFilesOnAllHosts(globalCluster, globalFPInfo)
		}
	}
	err := backupLockFile.Unlock()
	if err != nil && backupLockFile != "" {
		gplog.Warn("Failed to remove lock file %s.", backupLockFile)
	}
	if connectionPool != nil {
		// The connection pool might still have an ongoing transaction. Try
		// to cancel it. We need to queue a ROLLBACK to ensure the transaction
		// cancel actually happened because the Golang Context cancel function
		// does not block... nor is there a cancel acknowledgement function.
		if queryCancelFunc != nil {
			queryCancelFunc()
			connectionPool.MustExec("ROLLBACK")
		}

		connectionPool.Close()
	}
}

func GetVersion() string {
	return version
}

func logCompletionMessage(msg string) {
	if wasTerminated {
		gplog.Info("%s incomplete", msg)
	} else {
		gplog.Info("%s complete", msg)
	}
}
