package restore

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/greenplum-db/gpbackup/backup_filepath"
	"github.com/greenplum-db/gpbackup/backup_history"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains wrapper functions that group together functions relating
 * to querying and restoring metadata, so that the logic for each object type
 * can all be in one place and restore.go can serve as a high-level look at the
 * overall restore flow.
 */

/*
 * Setup and validation wrapper functions
 */

func SetLoggerVerbosity() {
	if MustGetFlagBool(utils.QUIET) {
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if MustGetFlagBool(utils.DEBUG) {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if MustGetFlagBool(utils.VERBOSE) {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

func CreateConnectionPool(unquotedDBName string) {
	connectionPool = dbconn.NewDBConnFromEnvironment(unquotedDBName)
	connectionPool.MustConnect(MustGetFlagInt(utils.JOBS))
	utils.ValidateGPDBVersionCompatibility(connectionPool)
}

func InitializeConnectionPool(unquotedDBName string) {
	CreateConnectionPool(unquotedDBName)
	setupQuery := `
SET application_name TO 'gprestore';
SET search_path TO pg_catalog;
SET gp_default_storage_options='';
SET statement_timeout = 0;
SET check_function_bodies = false;
SET client_min_messages = error;
SET standard_conforming_strings = on;
SET default_with_oids = off;
`
	if connectionPool.Version.Is("4") {
		setupQuery += "SET gp_strict_xml_parse = off;\n"
	}
	if connectionPool.Version.AtLeast("5") {
		setupQuery += "SET gp_ignore_error_table = on;\n"
	}
	if connectionPool.Version.Before("6") {
		setupQuery += "SET allow_system_table_mods = 'DML';\n"
	}
	if connectionPool.Version.AtLeast("6") {
		setupQuery += "SET allow_system_table_mods = true;\n"
		setupQuery += "SET lock_timeout = 0;\n"
		setupQuery += "SET default_transaction_read_only = off;\n"

		// If the backup is from a GPDB version less than 6.0,
		// we need to use legacy hash operators when restoring
		// the tables.
		backupConfigMajorVer, _ := strconv.Atoi(strings.Split(backupConfig.DatabaseVersion, ".")[0])
		if backupConfigMajorVer < 6 {
			setupQuery += "SET gp_use_legacy_hashops = on;\n"
			gplog.Warn("This backup set was taken on a version of Greenplum prior to 6.x. This restore will use the legacy hash operators when loading data.")
			gplog.Warn("To use the new Greenplum 6.x default hash operators, these tables will need to be redistributed.")
			gplog.Warn("For more information, refer to the migration guide located as https://docs.greenplum.org/latest/install_guide/migrate.html.")
		}
	}
	setupQuery += SetMaxCsvLineLengthQuery(connectionPool)

	for i := 0; i < connectionPool.NumConns; i++ {
		connectionPool.MustExec(setupQuery, i)
	}
}

func SetMaxCsvLineLengthQuery(connectionPool *dbconn.DBConn) string {
	if connectionPool.Version.AtLeast("6") {
		return ""
	}

	var maxLineLength int
	if connectionPool.Version.Is("4") && connectionPool.Version.AtLeast("4.3.30") {
		maxLineLength = 1024 * 1024 * 1024 // 1GB
	} else if connectionPool.Version.Is("5") && connectionPool.Version.AtLeast("5.11.0") {
		maxLineLength = 1024 * 1024 * 1024
	} else {
		maxLineLength = 4 * 1024 * 1024 // 4MB
	}

	return fmt.Sprintf("SET gp_max_csv_line_length = %d;\n", maxLineLength)
}

func InitializeBackupConfig() {
	backupConfig = backup_history.ReadConfigFile(globalFPInfo.GetConfigFilePath())
	utils.InitializePipeThroughParameters(backupConfig.Compressed, 0)
	utils.EnsureBackupVersionCompatibility(backupConfig.BackupVersion, version)
	utils.EnsureDatabaseVersionCompatibility(backupConfig.DatabaseVersion, connectionPool.Version)
}

func InitializeFilterLists() {
	if MustGetFlagString(utils.INCLUDE_RELATION_FILE) != "" {
		includeRelations := strings.Join(iohelper.MustReadLinesFromFile(MustGetFlagString(utils.INCLUDE_RELATION_FILE)), ",")
		err := cmdFlags.Set(utils.INCLUDE_RELATION, includeRelations)
		gplog.FatalOnError(err)
	}
	if MustGetFlagString(utils.EXCLUDE_RELATION_FILE) != "" {
		excludeRelations := strings.Join(iohelper.MustReadLinesFromFile(MustGetFlagString(utils.EXCLUDE_RELATION_FILE)), ",")
		err := cmdFlags.Set(utils.EXCLUDE_RELATION, excludeRelations)
		gplog.FatalOnError(err)
	}
}

func BackupConfigurationValidation() {
	InitializeFilterLists()

	if !backupConfig.MetadataOnly {
		gplog.Verbose("Gathering information on backup directories")
		VerifyBackupDirectoriesExistOnAllHosts()
	}

	VerifyMetadataFilePaths(MustGetFlagBool(utils.WITH_STATS))

	tocFilename := globalFPInfo.GetTOCFilePath()
	globalTOC = utils.NewTOC(tocFilename)
	globalTOC.InitializeMetadataEntryMap()

	// Legacy backups prior to the incremental feature would have no restoreplan yaml element
	if isLegacyBackup := backupConfig.RestorePlan == nil; isLegacyBackup {
		SetRestorePlanForLegacyBackup(globalTOC, globalFPInfo.Timestamp, backupConfig)
	}

	ValidateBackupFlagCombinations()

	validateFilterListsInBackupSet()
}

func SetRestorePlanForLegacyBackup(toc *utils.TOC, backupTimestamp string, backupConfig *backup_history.BackupConfig) {
	tableFQNs := make([]string, 0, len(toc.DataEntries))
	for _, entry := range toc.DataEntries {
		entryFQN := utils.MakeFQN(entry.Schema, entry.Name)
		tableFQNs = append(tableFQNs, entryFQN)
	}
	backupConfig.RestorePlan = []backup_history.RestorePlanEntry{
		{Timestamp: backupTimestamp, ChangedTables: tableFQNs},
	}
}

func RecoverMetadataFilesUsingPlugin() {
	var err error
	pluginConfig, err = utils.ReadPluginConfig(MustGetFlagString(utils.PLUGIN_CONFIG))
	gplog.FatalOnError(err)
	configFilename := filepath.Base(pluginConfig.ConfigPath)
	configDirname := filepath.Dir(pluginConfig.ConfigPath)
	pluginConfig.ConfigPath = filepath.Join(configDirname, backup_history.CurrentTimestamp()+"_"+configFilename)
	_ = cmdFlags.Set(utils.PLUGIN_CONFIG, pluginConfig.ConfigPath)
	gplog.Info("plugin config path: %s", pluginConfig.ConfigPath)

	pluginConfig.CheckPluginExistsOnAllHosts(globalCluster)

	timestamp := MustGetFlagString(utils.TIMESTAMP)
	historicalPluginVersion := FindHistoricalPluginVersion(timestamp)
	pluginConfig.SetBackupPluginVersion(timestamp, historicalPluginVersion)

	pluginConfig.CopyPluginConfigToAllHosts(globalCluster)
	pluginConfig.SetupPluginForRestore(globalCluster, globalFPInfo)

	metadataFiles := []string{globalFPInfo.GetConfigFilePath(), globalFPInfo.GetMetadataFilePath(),
		globalFPInfo.GetBackupReportFilePath()}
	if MustGetFlagBool(utils.WITH_STATS) {
		metadataFiles = append(metadataFiles, globalFPInfo.GetStatisticsFilePath())
	}
	for _, filename := range metadataFiles {
		pluginConfig.MustRestoreFile(filename)
	}

	InitializeBackupConfig()

	var fpInfoList []backup_filepath.FilePathInfo
	if backupConfig.MetadataOnly {
		fpInfoList = []backup_filepath.FilePathInfo{globalFPInfo}
	} else {
		fpInfoList = GetBackupFPInfoListFromRestorePlan()
	}

	for _, fpInfo := range fpInfoList {
		pluginConfig.MustRestoreFile(fpInfo.GetTOCFilePath())
		if backupConfig.SingleDataFile {
			pluginConfig.RestoreSegmentTOCs(globalCluster, fpInfo)
		}
	}
}

func FindHistoricalPluginVersion(timestamp string) string {
	// in order for plugins to implement backwards compatibility,
	// first, read history from master and provide the historical version
	// of the plugin that was used to create the original backup

	// adapted from incremental GetLatestMatchingBackupTimestamp
	var historicalPluginVersion string
	if iohelper.FileExistsAndIsReadable(globalFPInfo.GetBackupHistoryFilePath()) {
		history, err := backup_history.NewHistory(globalFPInfo.GetBackupHistoryFilePath())
		gplog.FatalOnError(err)
		foundBackupConfig := history.FindBackupConfig(timestamp)
		if foundBackupConfig != nil {
			historicalPluginVersion = foundBackupConfig.PluginVersion
		}
	}
	return historicalPluginVersion
}

/*
 * Metadata and/or data restore wrapper functions
 */

func GetUserFilters() utils.Filters {
	filters := utils.NewFilters(
		MustGetFlagStringSlice(utils.INCLUDE_SCHEMA),
		MustGetFlagStringSlice(utils.EXCLUDE_SCHEMA),
		MustGetFlagStringSlice(utils.INCLUDE_RELATION),
		MustGetFlagStringSlice(utils.EXCLUDE_RELATION))
	return filters
}

func BuildRestoreMetadataFilters(relationsToRestore []string) utils.Filters {
	userFilters := GetUserFilters()

	var schemasToCreate [] string
	var relationsToCreate [] string
	var schemasExcludedByUserInput [] string
	var relationsExcludedByUserInput [] string
	var inSchemas, exSchemas, inRelations, exRelations [] string

	existingSchemas, err := GetExistingSchemas()
	gplog.FatalOnError(err)
	existingTableFQNs, err := GetExistingTableFQNs()
	gplog.FatalOnError(err)

	existingTablesMap := make(map[string]Empty)
	for _, table := range existingTableFQNs{
		existingTablesMap[table] = Empty{}
	}

	for _, table := range relationsToRestore {
		schemaName := strings.Split(table,".")[0]
		if utils.SchemaIsExcluded(userFilters.IncludeSchemas, userFilters.ExcludeSchemas, schemaName){
			if !utils.Exists(schemasExcludedByUserInput, schemaName) {
				schemasExcludedByUserInput = append(schemasExcludedByUserInput, schemaName)
			}
			relationsExcludedByUserInput = append(relationsExcludedByUserInput, table)
			continue
		}

		if _, exists := existingTablesMap[table]; !exists {
			if utils.RelationIsExcluded(userFilters.IncludeRelations, userFilters.ExcludeRelations, table) {
				relationsExcludedByUserInput = append(relationsExcludedByUserInput, table)
			} else {
				if!utils.Exists(schemasToCreate, schemaName){
					schemasToCreate = append(schemasToCreate, schemaName)
				}
				relationsToCreate = append(relationsToCreate, table)
			}
		}
	}

	if len(schemasToCreate) == 0 { // no new schemas
		exSchemas = append(existingSchemas, schemasExcludedByUserInput...)
	} else {
		inSchemas = schemasToCreate
	}

	if len(relationsToCreate) == 0 { // no new tables
		exRelations = append(existingTableFQNs, relationsExcludedByUserInput...)
	} else {
		inRelations = relationsToCreate
	}

	filters := utils.NewFilters(inSchemas, exSchemas, inRelations, exRelations)
	return filters
}

func GetRestoreMetadataStatements(section string, filename string, includeObjectTypes []string, excludeObjectTypes []string) []utils.StatementWithType {
	var statements []utils.StatementWithType
	statements = GetRestoreMetadataStatementsFiltered(section, filename, includeObjectTypes, excludeObjectTypes, utils.Filters{})
	return statements
}

func GetRestoreMetadataStatementsFiltered(section string, filename string, includeObjectTypes []string, excludeObjectTypes []string, filters utils.Filters) []utils.StatementWithType {
	metadataFile := iohelper.MustOpenFileForReading(filename)
	var statements []utils.StatementWithType
	var inSchemas, exSchemas, inRelations, exRelations []string
	if !utils.FiltersEmpty(filters)  {
		inSchemas = filters.IncludeSchemas
		exSchemas = filters.ExcludeSchemas
		inRelations = filters.IncludeRelations
		exRelations = filters.ExcludeRelations
		fpInfoList := GetBackupFPInfoListFromRestorePlan()
		for _, fpInfo := range fpInfoList {
			tocFilename := fpInfo.GetTOCFilePath()
			toc := utils.NewTOC(tocFilename)
			inRelations = append(inRelations, utils.GetIncludedPartitionRoots(toc.DataEntries, inRelations)...)
		}
		// Update include schemas for schema restore if include table is set
		if utils.Exists(includeObjectTypes, "SCHEMA") {
			for _, inRelation := range inRelations {
				schema := inRelation[:strings.Index(inRelation, ".")]
				if !utils.Exists(inSchemas, schema) {
					inSchemas = append(inSchemas, schema)
				}
			}
			// reset relation list as these were required only to extract schemas from inRelations
			inRelations = nil
			exRelations = nil
		}
	}
	statements = globalTOC.GetSQLStatementForObjectTypes(section, metadataFile, includeObjectTypes, excludeObjectTypes, inSchemas, exSchemas, inRelations, exRelations)
	return statements
}

func ExecuteRestoreMetadataStatements(statements []utils.StatementWithType, objectsTitle string, progressBar utils.ProgressBar, showProgressBar int, executeInParallel bool) {
	if progressBar == nil {
		ExecuteStatementsAndCreateProgressBar(statements, objectsTitle, showProgressBar, executeInParallel)
	} else {
		ExecuteStatements(statements, progressBar, executeInParallel)
	}
}

func GetBackupFPInfoListFromRestorePlan() []backup_filepath.FilePathInfo {
	fpInfoList := make([]backup_filepath.FilePathInfo, 0)
	for _, entry := range backupConfig.RestorePlan {
		segPrefix := backup_filepath.ParseSegPrefix(MustGetFlagString(utils.BACKUP_DIR), entry.Timestamp)

		fpInfo := backup_filepath.NewFilePathInfo(globalCluster, MustGetFlagString(utils.BACKUP_DIR), entry.Timestamp, segPrefix)
		fpInfoList = append(fpInfoList, fpInfo)
	}

	return fpInfoList
}

func GetBackupFPInfoForTimestamp(timestamp string) backup_filepath.FilePathInfo {
	segPrefix := backup_filepath.ParseSegPrefix(MustGetFlagString(utils.BACKUP_DIR), timestamp)
	fpInfo := backup_filepath.NewFilePathInfo(globalCluster, MustGetFlagString(utils.BACKUP_DIR), timestamp, segPrefix)
	return fpInfo
}

/*
 * The first time this function is called, it retrieves the session GUCs from the
 * predata file and processes them appropriately, then it returns them so they
 * can be used in later calls without the file access and processing overhead.
 */
func setGUCsForConnection(gucStatements []utils.StatementWithType, whichConn int) []utils.StatementWithType {
	if gucStatements == nil {
		objectTypes := []string{"SESSION GUCS"}
		gucStatements = GetRestoreMetadataStatements("global", globalFPInfo.GetMetadataFilePath(), objectTypes, []string{})
	}
	ExecuteStatementsAndCreateProgressBar(gucStatements, "", utils.PB_NONE, false, whichConn)
	return gucStatements
}

func RestoreSchemas(schemaStatements []utils.StatementWithType, progressBar utils.ProgressBar) {
	numErrors := 0
	for _, schema := range schemaStatements {
		_, err := connectionPool.Exec(schema.Statement, 0)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				gplog.Warn("Schema %s already exists", schema.Name)
			} else {
				errMsg := fmt.Sprintf("Error encountered while creating schema %s", schema.Name)
				if MustGetFlagBool(utils.ON_ERROR_CONTINUE) {
					gplog.Verbose(fmt.Sprintf("%s: %s", errMsg, err.Error()))
					numErrors++
				} else {
					gplog.Fatal(err, errMsg)
				}
			}
		}
		progressBar.Increment()
	}
	if numErrors > 0 {
		gplog.Error("Encountered %d errors during schema restore; see log file %s for a list of errors.", numErrors, gplog.GetLogFilePath())
	}
}

func GetExistingTableFQNs() ([]string,  error) {
	existingTableFQNs := make([]string, 0)

	query := `SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname)
			  FROM pg_catalog.pg_class c
				LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			  WHERE c.relkind IN ('r','')
			  AND c.relstorage IN ('h', 'a', 'c','')
				 AND n.nspname <> 'pg_catalog'
				 AND n.nspname <> 'information_schema'
				 AND n.nspname !~ '^pg_toast'
			  ORDER BY 1;`

	err := connectionPool.Select(&existingTableFQNs, query)
	return existingTableFQNs, err
}

func GetExistingSchemas()([]string, error){
	existingSchemas := make([]string, 0)

	query := `SELECT n.nspname AS "Name"
			  FROM pg_catalog.pg_namespace n
			  WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
			  ORDER BY 1;`

	err := connectionPool.Select(&existingSchemas, query)
	return existingSchemas, err
}

func DropRelations(tables []string) error {
	query := `DROP TABLE IF EXISTS `
	query += strings.Join(tables, ",")
	query += " CASCADE;"
	gplog.Info("QUERY TO DROP: %s", query)
	_, err := connectionPool.Exec(query)
	return err
}

func TruncateTablesBeforeRestore(entries []utils.MasterDataEntry) error {
	query := `TRUNCATE `
	tableFQNs := make([]string, 0)
	for _, entry := range entries {
		tableFQN := utils.MakeFQN(entry.Schema, entry.Name)
		tableFQNs = append(tableFQNs, tableFQN)
	}
	query += strings.Join(tableFQNs, ",")
	query += ";"
	_, err := connectionPool.Exec(query)
	return err
}
