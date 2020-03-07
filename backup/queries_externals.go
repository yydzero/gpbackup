package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_externals.go.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

const (
	// Type of external table
	READABLE = iota
	READABLE_WEB
	WRITABLE
	WRITABLE_WEB
	// Protocol external table is using
	FILE
	GPFDIST
	GPHDFS
	HTTP
	S3
)

type ExternalTableDefinition struct {
	Oid             uint32
	Type            int
	Protocol        int
	Location        string
	ExecLocation    string
	FormatType      string
	FormatOpts      string
	Options         string
	Command         string
	RejectLimit     int
	RejectLimitType string
	ErrTableName    string
	ErrTableSchema  string
	Encoding        string
	Writable        bool
	URIs            []string
}

func (etd ExternalTableDefinition) GetTableStatement(tableName string) string {
	statement := ""
	if etd.Type != WRITABLE_WEB {
		if len(etd.URIs) > 0 {
			statement += fmt.Sprintf("LOCATION (\n\t'%s'\n)", strings.Join(etd.URIs, "',\n\t'"))
		}
	}
	if etd.Type == READABLE || (etd.Type == WRITABLE_WEB && etd.Protocol == S3) {
		if etd.ExecLocation == "MASTER_ONLY" {
			statement += fmt.Sprintf(" ON MASTER")
		}
	}
	if etd.Type == READABLE_WEB || etd.Type == WRITABLE_WEB {
		if etd.Command != "" {
			statement += fmt.Sprint(etd.generateExecuteStatement())
		}
	}
	statement += fmt.Sprintln() + fmt.Sprintln(etd.GenerateFormatStatement())
	if etd.Options != "" {
		statement += fmt.Sprintf("OPTIONS (\n\t%s\n)\n", etd.Options)
	}
	statement += fmt.Sprintf("ENCODING '%s'", etd.Encoding)
	if etd.Type == READABLE || etd.Type == READABLE_WEB {
		statement += fmt.Sprint(etd.generateLogErrorStatement(tableName))
	}
	return statement
}

/*
 * If an external table is created using LOG ERRORS instead of LOG ERRORS INTO [tablename],
 * the value of pg_exttable.fmterrtbl will match the table's own name.
 */
func (etd ExternalTableDefinition) generateLogErrorStatement(tableFQN string) string {
	var logErrorStatement string
	errTableFQN := utils.MakeFQN(etd.ErrTableSchema, etd.ErrTableName)
	if errTableFQN == tableFQN {
		logErrorStatement += "\nLOG ERRORS"
	} else if etd.ErrTableName != "" {
		logErrorStatement += fmt.Sprintf("\nLOG ERRORS INTO %s", errTableFQN)
	}
	if etd.RejectLimit != 0 {
		logErrorStatement += fmt.Sprintf("\nSEGMENT REJECT LIMIT %d ", etd.RejectLimit)
		switch etd.RejectLimitType {
		case "r":
			logErrorStatement += "ROWS"
		case "p":
			logErrorStatement += "PERCENT"
		}
	}
	return logErrorStatement
}

func (etd ExternalTableDefinition) GenerateFormatStatement() string {
	formatType := ""
	switch etd.FormatType {
	case "t":
		formatType = "TEXT"
	case "c":
		formatType = "CSV"
	case "b":
		formatType = "CUSTOM"
	case "a":
		formatType = "AVRO"
	case "p":
		formatType = "PARQUET"
	}
	formatStatement := fmt.Sprintf("FORMAT '%s'", formatType)

	if etd.FormatOpts != "" {
		formatTokens := tokenizeAndEscapeFormatOpts(strings.TrimSpace(etd.FormatOpts))
		formatOptsString := ""
		if formatType == "TEXT" || formatType == "CSV" {
			formatOptsString = strings.Join(formatTokens, " ")
		} else {
			formatOptsString = makeCustomFormatOpts(formatTokens)
		}
		formatStatement += fmt.Sprintf(" (%s)", formatOptsString)
	}

	return formatStatement
}

func (etd ExternalTableDefinition) GetTableCharacteristics() (int, int) {
	isWritable := etd.Writable
	var tableType int
	tableProtocol := -1
	if etd.Location == "" { // EXTERNAL WEB tables may have EXECUTE instead of LOCATION
		tableProtocol = HTTP
		if isWritable {
			tableType = WRITABLE_WEB
		} else {
			tableType = READABLE_WEB
		}
	} else {
		/*
		 * All data sources must use the same protocol, so we can use Location to determine
		 * the table's protocol even though it only holds one data source URI.
		 */
		isWeb := strings.HasPrefix(etd.Location, "http")
		if isWeb && isWritable {
			tableType = WRITABLE_WEB
		} else if isWeb && !isWritable {
			tableType = READABLE_WEB
		} else if !isWeb && isWritable {
			tableType = WRITABLE
		} else {
			tableType = READABLE
		}
		prefix := etd.Location[0:strings.Index(etd.Location, "://")]
		switch prefix {
		case "file":
			tableProtocol = FILE
		case "gpfdist":
			tableProtocol = GPFDIST
		case "gpfdists":
			tableProtocol = GPFDIST
		case "gphdfs":
			tableProtocol = GPHDFS
		case "http":
			tableProtocol = HTTP
		case "https":
			tableProtocol = HTTP
		case "s3":
			tableProtocol = S3
		}
	}
	return tableType, tableProtocol
}

func (etd ExternalTableDefinition) generateExecuteStatement() string {
	var executeStatement string

	etd.Command = strings.Replace(etd.Command, `'`, `''`, -1)
	executeStatement += fmt.Sprintf("EXECUTE '%s'", etd.Command)
	execType := strings.Split(etd.ExecLocation, ":")
	switch execType[0] {
	case "ALL_SEGMENTS": // Default case, don't print anything else
	case "HOST":
		executeStatement += fmt.Sprintf(" ON HOST '%s'", execType[1])
	case "MASTER_ONLY":
		executeStatement += " ON MASTER"
	case "PER_HOST":
		executeStatement += " ON HOST"
	case "SEGMENT_ID":
		executeStatement += fmt.Sprintf(" ON SEGMENT %s", execType[1])
	case "TOTAL_SEGS":
		executeStatement += fmt.Sprintf(" ON %s", execType[1])
	}

	return executeStatement
}

/*
 * This function is adapted from dumputils.c
 *
 * Escape backslashes and apostrophes in EXTERNAL TABLE format strings.
 * Returns a list of unquoted keyword and escaped quoted string tokens
 *
 * The fmtopts field of a pg_exttable tuple has an odd encoding -- it is
 * partially parsed and contains "string" values that aren't legal SQL.
 * Each string value is delimited by apostrophes and is usually, but not
 * always, a single character.	The fmtopts field is typically something
 * like {delimiter '\x09' null '\N' escape '\'} or
 * {delimiter ',' null '' escape '\' quote '''}.  Each backslash and
 * apostrophe in a string must be escaped and each string must be
 * prepended with an 'E' denoting an "escape syntax" string.
 *
 * Usage note: A field value containing an apostrophe followed by a space
 * will throw this algorithm off -- it presumes no embedded spaces.
 */
func tokenizeAndEscapeFormatOpts(formatOpts string) []string {
	inString := false
	resultList := make([]string, 0)
	currString := ""

	for i := 0; i < len(formatOpts); i++ {
		switch formatOpts[i] {
		case '\'':
			if inString {
				/*
				 * Escape apostrophes *within* the string.	If the
				 * apostrophe is at the end of the source string or is
				 * followed by a space, it is presumed to be a closing
				 * apostrophe and is not escaped.
				 */
				if (i+1) == len(formatOpts) || formatOpts[i+1] == ' ' {
					inString = false
				} else {
					currString += "\\"
				}
			} else {
				currString = "E"
				inString = true
			}
		case '\\':
			currString += "\\"
		case ' ':
			if !inString {
				resultList = append(resultList, currString)
				currString = ""
				continue
			}
		}
		currString += string(formatOpts[i])
	}
	resultList = append(resultList, currString)

	return resultList
}

/*
 * Format options to use `a = b` format because this format is required
 * when using CUSTOM format.
 *
 * We do this for CUSTOM, AVRO and PARQUET, but not CSV or TEXT because
 * CSV and TEXT have some multi-word options that are difficult
 * to parse into this format
 */
func makeCustomFormatOpts(tokens []string) string {
	var key string
	var value string
	resultOpts := make([]string, 0)

	for i := 0; i < len(tokens)-1; i += 2 {
		key = tokens[i]
		value = tokens[i+1]
		opt := fmt.Sprintf(`%s = %s`, key, value)
		resultOpts = append(resultOpts, opt)
	}
	return strings.Join(resultOpts, ", ")
}

func GetExternalTableDefinitions(connectionPool *dbconn.DBConn) map[uint32]ExternalTableDefinition {
	gplog.Verbose("Retrieving external table information")

	location := `CASE WHEN urilocation IS NOT NULL THEN unnest(urilocation) ELSE '' END AS location,
		array_to_string(execlocation, ',') AS execlocation,`
	options := `array_to_string(ARRAY(SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
			FROM pg_options_to_table(options) ORDER BY option_name), E',\n\t') AS options,`
	errTable := `coalesce(quote_ident(c.relname),'') AS errtablename,
		coalesce((SELECT quote_ident(nspname) FROM pg_namespace n WHERE n.oid = c.relnamespace), '') AS errtableschema,`
	errColumn := `fmterrtbl`

	if connectionPool.Version.Before("5") {
		execOptions := "'ALL_SEGMENTS', 'HOST', 'MASTER_ONLY', 'PER_HOST', 'SEGMENT_ID', 'TOTAL_SEGS'"
		location = fmt.Sprintf(`CASE WHEN split_part(location[1], ':', 1) NOT IN (%s) THEN unnest(location) ELSE '' END AS location,
		CASE WHEN split_part(location[1], ':', 1) IN (%s) THEN unnest(location) ELSE 'ALL_SEGMENTS' END AS execlocation,`, execOptions, execOptions)
		options = "'' AS options,"
	} else if !connectionPool.Version.Before("6") {
		errTable = `CASE WHEN logerrors = 'false' THEN '' ELSE quote_ident(c.relname) END AS errtablename,
		CASE WHEN logerrors = 'false' THEN '' ELSE coalesce(
			(SELECT quote_ident(nspname) FROM pg_namespace n WHERE n.oid = c.relnamespace), '') END AS errtableschema,`
		errColumn = `reloid`
	}

	query := fmt.Sprintf(`
	SELECT reloid AS oid,
		%s
		fmttype AS formattype,
		fmtopts AS formatopts,
		%s
		coalesce(command, '') AS command,
		coalesce(rejectlimit, 0) AS rejectlimit,
		coalesce(rejectlimittype, '') AS rejectlimittype,
		%s
		pg_encoding_to_char(encoding) AS encoding,
		writable
	FROM pg_exttable e
		LEFT JOIN pg_class c ON e.%s = c.oid`, location, options, errTable, errColumn)

	results := make([]ExternalTableDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]ExternalTableDefinition)
	var extTableDef ExternalTableDefinition
	for _, result := range results {
		if resultMap[result.Oid].Oid != 0 {
			extTableDef = resultMap[result.Oid]
		} else {
			extTableDef = result
		}
		if result.Location != "" {
			extTableDef.URIs = append(extTableDef.URIs, result.Location)
		}
		resultMap[result.Oid] = extTableDef
	}
	return resultMap
}

type ExternalProtocol struct {
	Oid           uint32
	Name          string
	Owner         string
	Trusted       bool   `db:"ptctrusted"`
	ReadFunction  uint32 `db:"ptcreadfn"`
	WriteFunction uint32 `db:"ptcwritefn"`
	Validator     uint32 `db:"ptcvalidatorfn"`
}

func (p ExternalProtocol) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            p.Name,
			ObjectType:      "PROTOCOL",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (p ExternalProtocol) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_EXTPROTOCOL_OID, Oid: p.Oid}
}

func (p ExternalProtocol) FQN() string {
	return p.Name
}

func (p ExternalProtocol) GetCreateStatement(funcInfoMap map[uint32]FunctionInfo) string {
	funcOidList := []uint32{p.ReadFunction, p.WriteFunction, p.Validator}
	hasUserDefinedFunc := false
	for _, funcOid := range funcOidList {
		if funcInfo, ok := funcInfoMap[funcOid]; ok && !funcInfo.IsInternal {
			hasUserDefinedFunc = true
		}
	}
	if !hasUserDefinedFunc {
		return ""
	}

	protocolFunctions := make([]string, 0)
	if p.ReadFunction != 0 {
		protocolFunctions = append(protocolFunctions,
			fmt.Sprintf("readfunc = %s", funcInfoMap[p.ReadFunction].QualifiedName))
	}
	if p.WriteFunction != 0 {
		protocolFunctions = append(protocolFunctions,
			fmt.Sprintf("writefunc = %s", funcInfoMap[p.WriteFunction].QualifiedName))
	}
	if p.Validator != 0 {
		protocolFunctions = append(protocolFunctions,
			fmt.Sprintf("validatorfunc = %s", funcInfoMap[p.Validator].QualifiedName))
	}

	statement := fmt.Sprintf("\n\nCREATE ")
	if p.Trusted {
		statement += fmt.Sprintf("TRUSTED ")
	}
	statement += fmt.Sprintf("PROTOCOL %s (%s);\n", p.Name, strings.Join(protocolFunctions, ", "))
	return statement
}

func GetExternalProtocols(connectionPool *dbconn.DBConn) []ExternalProtocol {
	results := make([]ExternalProtocol, 0)
	query := `
	SELECT p.oid,
		quote_ident(p.ptcname) AS name,
		pg_get_userbyid(p.ptcowner) AS owner,
		p.ptctrusted,
		p.ptcreadfn,
		p.ptcwritefn,
		p.ptcvalidatorfn
	FROM pg_extprotocol p`
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type PartitionInfo struct {
	PartitionRuleOid       uint32
	PartitionParentRuleOid uint32
	ParentRelationOid      uint32
	ParentSchema           string
	ParentRelationName     string
	RelationOid            uint32
	PartitionName          string
	PartitionRank          int
	IsExternal             bool
}

func (pi PartitionInfo) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          pi.ParentSchema,
			Name:            pi.ParentRelationName,
			ObjectType:      "EXCHANGE PARTITION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (pi PartitionInfo) GetExchangeStatement(
	partInfoMap map[uint32]PartitionInfo, extPartRelationName string) string {
	parentRelationName := utils.MakeFQN(pi.ParentSchema, pi.ParentRelationName)
	alterPartitionStr := ""
	currentPartition := pi
	for currentPartition.PartitionParentRuleOid != 0 {
		parent := partInfoMap[currentPartition.PartitionParentRuleOid]
		if parent.PartitionName == "" {
			alterPartitionStr = fmt.Sprintf("ALTER PARTITION FOR (RANK(%d)) ", parent.PartitionRank) + alterPartitionStr
		} else {
			alterPartitionStr = fmt.Sprintf("ALTER PARTITION %s ", parent.PartitionName) + alterPartitionStr
		}
		currentPartition = parent
	}
	statement := fmt.Sprintf("\n\nALTER TABLE %s %s", parentRelationName, alterPartitionStr)
	if pi.PartitionName == "" {
		statement += fmt.Sprintf("EXCHANGE PARTITION FOR (RANK(%d)) ", pi.PartitionRank)
	} else {
		statement += fmt.Sprintf("EXCHANGE PARTITION %s ", pi.PartitionName)
	}
	statement += fmt.Sprintf("WITH TABLE %s WITHOUT VALIDATION;", extPartRelationName)
	statement += fmt.Sprintf("\n\nDROP TABLE %s;", extPartRelationName)
	return statement
}

func GetExternalPartitionInfo(connectionPool *dbconn.DBConn) ([]PartitionInfo, map[uint32]PartitionInfo) {
	results := make([]PartitionInfo, 0)
	query := `
	SELECT pr1.oid AS partitionruleoid,
		pr1.parparentrule AS partitionparentruleoid,
		cl.oid AS parentrelationoid,
		quote_ident(n.nspname) AS parentschema,
		quote_ident(cl.relname) AS parentrelationname,
		pr1.parchildrelid AS relationoid,
		CASE WHEN pr1.parname = '' THEN '' ELSE quote_ident(pr1.parname) END AS partitionname,
		CASE WHEN pp.parkind <> 'r'::"char" OR pr1.parisdefault THEN 0
			ELSE pg_catalog.rank() OVER (PARTITION BY pp.oid, cl.relname, pp.parlevel, cl3.relname
				ORDER BY pr1.parisdefault, pr1.parruleord) END AS partitionrank,
		CASE WHEN e.reloid IS NOT NULL then 't' ELSE 'f' END AS isexternal
	FROM pg_namespace n, pg_namespace n2, pg_class cl
		LEFT JOIN pg_tablespace sp ON cl.reltablespace = sp.oid, pg_class cl2
		LEFT JOIN pg_tablespace sp3 ON cl2.reltablespace = sp3.oid, pg_partition pp, pg_partition_rule pr1
		LEFT JOIN pg_partition_rule pr2 ON pr1.parparentrule = pr2.oid
		LEFT JOIN pg_class cl3 ON pr2.parchildrelid = cl3.oid
		LEFT JOIN pg_exttable e ON e.reloid = pr1.parchildrelid
	WHERE pp.paristemplate = false
		AND pp.parrelid = cl.oid
		AND pr1.paroid = pp.oid
		AND cl2.oid = pr1.parchildrelid
		AND cl.relnamespace = n.oid
		AND cl2.relnamespace = n2.oid`
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)

	extPartitions := make([]PartitionInfo, 0)
	partInfoMap := make(map[uint32]PartitionInfo, len(results))
	for _, partInfo := range results {
		if partInfo.IsExternal {
			extPartitions = append(extPartitions, partInfo)
		}
		partInfoMap[partInfo.PartitionRuleOid] = partInfo
	}

	return extPartitions, partInfoMap
}
