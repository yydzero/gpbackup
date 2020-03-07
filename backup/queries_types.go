package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_types.go.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func GetTypeMetadataEntry(schema string, name string) (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          schema,
			Name:            name,
			ObjectType:      "TYPE",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

type BaseType struct {
	Oid             uint32
	Schema          string
	Name            string
	Input           string
	Output          string
	Receive         string
	Send            string
	ModIn           string
	ModOut          string
	InternalLength  int
	IsPassedByValue bool
	Alignment       string
	Storage         string
	DefaultVal      string
	Element         string
	Category        string
	Preferred       bool
	Delimiter       string
	StorageOptions  string
	Collatable      bool
	Collation       string
}

func (t BaseType) GetMetadataEntry() (string, toc.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t BaseType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t BaseType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func (t BaseType) GetCreateStatement() string {
	statement := fmt.Sprintf("\n\nCREATE TYPE %s (\n", t.FQN())

	// All of the following functions are stored in quoted form and don't need to be quoted again
	statement += fmt.Sprintf("\tINPUT = %s,\n\tOUTPUT = %s", t.Input, t.Output)
	if t.Receive != "" {
		statement += fmt.Sprintf(",\n\tRECEIVE = %s", t.Receive)
	}
	if t.Send != "" {
		statement += fmt.Sprintf(",\n\tSEND = %s", t.Send)
	}
	if connectionPool.Version.AtLeast("5") {
		if t.ModIn != "" {
			statement += fmt.Sprintf(",\n\tTYPMOD_IN = %s", t.ModIn)
		}
		if t.ModOut != "" {
			statement += fmt.Sprintf(",\n\tTYPMOD_OUT = %s", t.ModOut)
		}
	}
	if t.InternalLength > 0 {
		statement += fmt.Sprintf(",\n\tINTERNALLENGTH = %d", t.InternalLength)
	}
	if t.IsPassedByValue {
		statement += fmt.Sprintf(",\n\tPASSEDBYVALUE")
	}
	if t.Alignment != "" {
		switch t.Alignment {
		case "d":
			statement += fmt.Sprintf(",\n\tALIGNMENT = double")
		case "i":
			statement += fmt.Sprintf(",\n\tALIGNMENT = int4")
		case "s":
			statement += fmt.Sprintf(",\n\tALIGNMENT = int2")
		case "c": // Default case, don't print anything else
		}
	}
	if t.Storage != "" {
		switch t.Storage {
		case "e":
			statement += fmt.Sprintf(",\n\tSTORAGE = external")
		case "m":
			statement += fmt.Sprintf(",\n\tSTORAGE = main")
		case "x":
			statement += fmt.Sprintf(",\n\tSTORAGE = extended")
		case "p": // Default case, don't print anything else
		}
	}
	if t.DefaultVal != "" {
		statement += fmt.Sprintf(",\n\tDEFAULT = '%s'", t.DefaultVal)
	}
	if t.Element != "" {
		statement += fmt.Sprintf(",\n\tELEMENT = %s", t.Element)
	}
	if t.Delimiter != "" {
		statement += fmt.Sprintf(",\n\tDELIMITER = '%s'", t.Delimiter)
	}
	if t.Category != "U" {
		statement += fmt.Sprintf(",\n\tCATEGORY = '%s'", t.Category)
	}
	if t.Preferred {
		statement += fmt.Sprintf(",\n\tPREFERRED = true")
	}
	if t.Collatable {
		statement += fmt.Sprintf(",\n\tCOLLATABLE = true")
	}
	statement += fmt.Sprintln("\n);")
	if t.StorageOptions != "" {
		statement += fmt.Sprintf("\nALTER TYPE %s\n\tSET DEFAULT ENCODING (%s);", t.FQN(), t.StorageOptions)
	}

	return statement
}

func GetBaseTypes(connectionPool *dbconn.DBConn) []BaseType {
	gplog.Verbose("Getting base types")
	version4query := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name,
		t.typinput AS input,
		t.typoutput AS output,
		t.typreceive AS receive,
		t.typsend AS send,
		t.typlen AS internallength,
		t.typbyval AS ispassedbyvalue,
		CASE WHEN t.typalign = '-' THEN '' ELSE t.typalign END AS alignment,
		t.typstorage AS storage,
		coalesce(t.typdefault, '') AS defaultval,
		CASE WHEN t.typelem != 0::regproc THEN pg_catalog.format_type(t.typelem, NULL) ELSE '' END AS element,
		'U' AS category,
		t.typdelim AS delimiter,
		coalesce(array_to_string(e.typoptions, ', '), '') AS storageoptions
	FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		LEFT JOIN pg_type_encoding e ON t.oid = e.typid
		/*
		 * Identify if this is an automatically generated array type and exclude it if so.
		 * In GPDB 4, all automatically-generated array types are guaranteed to be
		 * the name of the corresponding base type prepended with an underscore.
		 */
		LEFT JOIN pg_type ut ON ( --ut for underlying type
			t.typelem = ut.oid
			AND length(t.typname) > 1
			AND t.typname[0] = '_'
			AND substring(t.typname FROM 2) = ut.typname)
	WHERE %s
		AND t.typtype = 'b'
		AND ut.oid IS NULL
		AND %s`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	version5query := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name,
		t.typinput AS input,
		t.typoutput AS output,
		CASE WHEN t.typreceive = '-'::regproc THEN '' ELSE t.typreceive::regproc::text END AS receive,
		CASE WHEN t.typsend = '-'::regproc THEN '' ELSE t.typsend::regproc::text END AS send,
		CASE WHEN t.typmodin = '-'::regproc THEN '' ELSE t.typmodin::regproc::text END AS modin,
		CASE WHEN t.typmodout = '-'::regproc THEN '' ELSE t.typmodout::regproc::text END AS modout,
		t.typlen AS internallength,
		t.typbyval AS ispassedbyvalue,
		CASE WHEN t.typalign = '-' THEN '' ELSE t.typalign END AS alignment,
		t.typstorage AS storage,
		coalesce(t.typdefault, '') AS defaultval,
		CASE WHEN t.typelem != 0::regproc THEN pg_catalog.format_type(t.typelem, NULL) ELSE '' END AS element,
		'U' AS category,
		t.typdelim AS delimiter,
		coalesce(array_to_string(e.typoptions, ', '), '') AS storageoptions
	FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		LEFT JOIN pg_type_encoding e ON t.oid = e.typid
		/*
		 * Identify if this is an automatically generated array type and exclude it if so.
		 * In GPDB 5 and 6 we use the typearray field to identify these array types.
		 */
		LEFT JOIN pg_type ut ON t.oid = ut.typarray
	WHERE %s
		AND t.typtype = 'b'
		AND ut.oid IS NULL
		AND %s`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	masterQuery := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name,
		t.typinput AS input,
		t.typoutput AS output,
		CASE WHEN t.typreceive = '-'::regproc THEN '' ELSE t.typreceive::regproc::text END AS receive,
		CASE WHEN t.typsend = '-'::regproc THEN '' ELSE t.typsend::regproc::text END AS send,
		CASE WHEN t.typmodin = '-'::regproc THEN '' ELSE t.typmodin::regproc::text END AS modin,
		CASE WHEN t.typmodout = '-'::regproc THEN '' ELSE t.typmodout::regproc::text END AS modout,
		t.typlen AS internallength,
		t.typbyval AS ispassedbyvalue,
		CASE WHEN t.typalign = '-' THEN '' ELSE t.typalign END AS alignment,
		t.typstorage AS storage,
		coalesce(t.typdefault, '') AS defaultval,
		CASE WHEN t.typelem != 0::regproc THEN pg_catalog.format_type(t.typelem, NULL) ELSE '' END AS element,
		t.typcategory AS category,
		t.typispreferred AS preferred,
		t.typdelim AS delimiter,
		(t.typcollation <> 0) AS collatable,
		coalesce(array_to_string(typoptions, ', '), '') AS storageoptions
	FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		LEFT JOIN pg_type_encoding e ON t.oid = e.typid
		/*
		 * Identify if this is an automatically generated array type and exclude it if so.
		 * In GPDB 5 and 6 we use the typearray field to identify these array types.
		 */
		LEFT JOIN pg_type ut ON t.oid = ut.typarray
	WHERE %s
		AND t.typtype = 'b'
		AND ut.oid IS NULL
		AND %s`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	results := make([]BaseType, 0)
	var err error
	if connectionPool.Version.Is("4") {
		err = connectionPool.Select(&results, version4query)
	} else if connectionPool.Version.Is("5") {
		err = connectionPool.Select(&results, version5query)
	} else {
		err = connectionPool.Select(&results, masterQuery)
	}
	gplog.FatalOnError(err)
	/*
	 * GPDB 4.3 has no built-in regproc-to-text cast and uses "-" in place of
	 * NULL for several fields, so to avoid dealing with hyphens later on we
	 * replace those with empty strings here.
	 */
	if connectionPool.Version.Before("5") {
		for i := range results {
			if results[i].Send == "-" {
				results[i].Send = ""
			}
			if results[i].Receive == "-" {
				results[i].Receive = ""
			}
		}
	}
	return results
}

type CompositeType struct {
	Oid        uint32
	Schema     string
	Name       string
	Attributes []Attribute
}

func (t CompositeType) GetMetadataEntry() (string, toc.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t CompositeType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t CompositeType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func (t CompositeType) GetCreateStatement() string {
	var attributeList []string
	for _, att := range t.Attributes {
		collationStr := ""
		if att.Collation != "" {
			collationStr = fmt.Sprintf(" COLLATE %s", att.Collation)
		}
		attributeList = append(attributeList, fmt.Sprintf("\t%s %s%s", att.Name, att.Type, collationStr))
	}

	statement := fmt.Sprintf("\n\nCREATE TYPE %s AS (\n", t.FQN())
	statement += fmt.Sprintln(strings.Join(attributeList, ",\n"))
	statement += fmt.Sprintf(");")

	return statement
}

func GetCompositeTypes(connectionPool *dbconn.DBConn) []CompositeType {
	gplog.Verbose("Getting composite types")
	query := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name
	FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		/*
		 * We join with pg_class to check if a type is truly a composite type
		 * (relkind='c') or implicitly generated from a relation
		 */
		JOIN pg_class c ON t.typrelid = c.oid
	WHERE %s
		AND t.typtype = 'c'
		AND c.relkind = 'c'
		AND %s`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	compTypes := make([]CompositeType, 0)
	err := connectionPool.Select(&compTypes, query)
	gplog.FatalOnError(err)

	attributeMap := getCompositeTypeAttributes(connectionPool)

	for i, compType := range compTypes {
		compTypes[i].Attributes = attributeMap[compType.Oid]
	}
	return compTypes
}

type Attribute struct {
	CompositeTypeOid uint32
	Name             string
	Type             string
	Comment          string
	Collation        string
}

func getCompositeTypeAttributes(connectionPool *dbconn.DBConn) map[uint32][]Attribute {
	gplog.Verbose("Getting composite type attributes")

	compositeAttributeQuery := `
	SELECT t.oid AS compositetypeoid,
		quote_ident(a.attname) AS name,
		pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
		coalesce(quote_literal(d.description),'') AS comment
	FROM pg_type t
		JOIN pg_class c ON t.typrelid = c.oid
		JOIN pg_attribute a ON t.typrelid = a.attrelid
		LEFT JOIN pg_description d ON (d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum)
	WHERE t.typtype = 'c'
		AND c.relkind = 'c'
	ORDER BY t.oid, a.attnum`

	if connectionPool.Version.AtLeast("6") {
		compositeAttributeQuery = `
		SELECT t.oid AS compositetypeoid,
			quote_ident(a.attname) AS name,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
			coalesce(quote_literal(d.description),'') AS comment,
			CASE
				WHEN at.typcollation <> a.attcollation
				THEN quote_ident(cn.nspname) || '.' || quote_ident(coll.collname) ELSE ''
			END AS collation
		FROM pg_type t
			JOIN pg_class c ON t.typrelid = c.oid
			JOIN pg_attribute a ON t.typrelid = a.attrelid
			LEFT JOIN pg_description d ON (d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum)
			LEFT JOIN pg_type at ON at.oid = a.atttypid
			LEFT JOIN pg_collation coll ON a.attcollation = coll.oid
			LEFT JOIN pg_namespace cn ON coll.collnamespace = cn.oid
		WHERE t.typtype = 'c'
			AND c.relkind = 'c'
		ORDER BY t.oid, a.attnum`
	}

	results := make([]Attribute, 0)
	var err error
	err = connectionPool.Select(&results, compositeAttributeQuery)
	gplog.FatalOnError(err)

	attributeMap := make(map[uint32][]Attribute)
	for _, att := range results {
		attributeMap[att.CompositeTypeOid] = append(attributeMap[att.CompositeTypeOid], att)
	}
	return attributeMap
}

type Domain struct {
	Oid        uint32
	Schema     string
	Name       string
	DefaultVal string
	Collation  string
	BaseType   string
	NotNull    bool
}

func (t Domain) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          t.Schema,
			Name:            t.Name,
			ObjectType:      "DOMAIN",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (t Domain) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t Domain) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func (t Domain) GetCreateStatement() string {
	statement := fmt.Sprintf("\nCREATE DOMAIN %s AS %s", t.FQN(), t.BaseType)
	if t.DefaultVal != "" {
		statement += fmt.Sprintf(" DEFAULT %s", t.DefaultVal)
	}
	if t.Collation != "" {
		statement += fmt.Sprintf(" COLLATE %s", t.Collation)
	}
	if t.NotNull {
		statement += fmt.Sprintf(" NOT NULL")
	}
	for _, constraint := range constraints {
		statement += fmt.Sprintf("\n\tCONSTRAINT %s %s", constraint.Name, constraint.ConDef)
	}
	statement += fmt.Sprintln(";")

	return statement
}

func GetDomainTypes(connectionPool *dbconn.DBConn) []Domain {
	gplog.Verbose("Getting domain types")
	results := make([]Domain, 0)
	before6query := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name,
		coalesce(t.typdefault, '') AS defaultval,
		format_type(t.typbasetype, t.typtypmod) AS basetype,
		t.typnotnull AS notnull
	FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
	WHERE %s
		AND t.typtype = 'd'
		AND %s
	ORDER BY n.nspname, t.typname`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	masterQuery := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name,
		coalesce(t.typdefault, '') AS defaultval,
		CASE
			WHEN t.typcollation <> u.typcollation
			THEN quote_ident(cn.nspname) || '.' || quote_ident(c.collname)
			ELSE ''
		END AS collation,
		format_type(t.typbasetype, t.typtypmod) AS basetype,
		t.typnotnull AS notnull
	FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
		LEFT JOIN pg_type u ON t.typbasetype = u.oid
		LEFT JOIN pg_collation c ON t.typcollation = c.oid
		LEFT JOIN pg_namespace cn ON c.collnamespace = cn.oid
	WHERE %s
		AND t.typtype = 'd'
		AND %s
	ORDER BY n.nspname, t.typname`, SchemaFilterClause("n"), ExtensionFilterClause("t"))
	var err error

	if connectionPool.Version.Before("6") {
		err = connectionPool.Select(&results, before6query)
	} else {
		err = connectionPool.Select(&results, masterQuery)
	}

	gplog.FatalOnError(err)
	return results
}

type EnumType struct {
	Oid        uint32
	Schema     string
	Name       string
	EnumLabels string
}

func (t EnumType) GetMetadataEntry() (string, toc.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t EnumType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t EnumType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func (t EnumType) GetCreateStatement() string {
	return fmt.Sprintf("\n\nCREATE TYPE %s AS ENUM (\n\t%s\n);\n", t.FQN(), t.EnumLabels)
}

func GetEnumTypes(connectionPool *dbconn.DBConn) []EnumType {
	enumSortClause := "ORDER BY e.enumsortorder"
	if connectionPool.Version.Is("5") {
		enumSortClause = "ORDER BY e.oid"
	}
	query := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name,
		enumlabels
	FROM pg_type t
		LEFT JOIN pg_namespace n ON t.typnamespace = n.oid
		LEFT JOIN (SELECT e.enumtypid,string_agg(quote_literal(e.enumlabel), E',\n\t' %s) AS enumlabels
			FROM pg_enum e GROUP BY enumtypid) e ON t.oid = e.enumtypid
	WHERE %s
		AND t.typtype = 'e'
		AND %s
	ORDER BY n.nspname, t.typname`, enumSortClause, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	results := make([]EnumType, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type RangeType struct {
	Oid            uint32
	Schema         string
	Name           string
	SubType        string
	Collation      string
	SubTypeOpClass string
	Canonical      string
	SubTypeDiff    string
}

func (t RangeType) GetMetadataEntry() (string, toc.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t RangeType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t RangeType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func (t RangeType) GetCreateStatement() string {
	statement := fmt.Sprintf("\n\nCREATE TYPE %s AS RANGE (\n\tSUBTYPE = %s", t.FQN(), t.SubType)

	if t.SubTypeOpClass != "" {
		statement += fmt.Sprintf(",\n\tSUBTYPE_OPCLASS = %s", t.SubTypeOpClass)
	}
	if t.Collation != "" {
		statement += fmt.Sprintf(",\n\tCOLLATION = %s", t.Collation)
	}
	if t.Canonical != "" {
		statement += fmt.Sprintf(",\n\tCANONICAL = %s", t.Canonical)
	}
	if t.SubTypeDiff != "" {
		statement += fmt.Sprintf(",\n\tSUBTYPE_DIFF = %s", t.SubTypeDiff)
	}
	statement += fmt.Sprintf("\n);\n")

	return statement
}

func GetRangeTypes(connectionPool *dbconn.DBConn) []RangeType {
	results := make([]RangeType, 0)
	if !connectionPool.Version.AtLeast("6") {
		return results
	}
	gplog.Verbose("Retrieving range types")
	query := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name,
		format_type(st.oid, st.typtypmod) AS subtype,
		CASE
			WHEN c.collname IS NULL THEN ''
			ELSE quote_ident(nc.nspname) || '.' || quote_ident(c.collname)
		END AS collation,
		CASE
			WHEN opc.opcname IS NULL THEN ''
			ELSE quote_ident(nopc.nspname) || '.' || quote_ident(opc.opcname)
		END AS subtypeopclass,
		CASE
			WHEN r.rngcanonical = '-'::regproc THEN ''
			ELSE r.rngcanonical::regproc::text
		END AS canonical,
		CASE
			WHEN r.rngsubdiff = '-'::regproc THEN ''
			ELSE r.rngsubdiff::regproc::text
		END AS subtypediff
	FROM pg_range r
		JOIN pg_type t ON t.oid = r.rngtypid
		JOIN pg_namespace n ON t.typnamespace = n.oid
		JOIN pg_type st ON st.oid = r.rngsubtype
		LEFT JOIN pg_collation c ON c.oid = r.rngcollation
		LEFT JOIN pg_namespace nc ON nc.oid = c.collnamespace
		LEFT JOIN pg_opclass opc ON opc.oid = r.rngsubopc
		LEFT JOIN pg_namespace nopc ON nopc.oid = opc.opcnamespace
	WHERE %s
		AND t.typtype = 'r'
		AND %s`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type ShellType struct {
	Oid    uint32
	Schema string
	Name   string
}

func (t ShellType) GetMetadataEntry() (string, toc.MetadataEntry) {
	return GetTypeMetadataEntry(t.Schema, t.Name)
}

func (t ShellType) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_TYPE_OID, Oid: t.Oid}
}

func (t ShellType) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
}

func GetShellTypes(connectionPool *dbconn.DBConn) []ShellType {
	gplog.Verbose("Getting shell types")
	query := fmt.Sprintf(`
	SELECT t.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(t.typname) AS name
	FROM pg_type t
		JOIN pg_namespace n ON t.typnamespace = n.oid
	WHERE %s
		AND t.typtype = 'p'
		AND %s
	ORDER BY n.nspname, t.typname`, SchemaFilterClause("n"), ExtensionFilterClause("t"))

	results := make([]ShellType, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type Collation struct {
	Oid     uint32
	Schema  string
	Name    string
	Collate string
	Ctype   string
}

func (c Collation) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          c.Schema,
			Name:            c.Name,
			ObjectType:      "COLLATION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (c Collation) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_COLLATION_OID, Oid: c.Oid}
}

func (c Collation) FQN() string {
	return utils.MakeFQN(c.Schema, c.Name)
}

func (c Collation) GetCreateStatement() string {
	return fmt.Sprintf("\nCREATE COLLATION %s (LC_COLLATE = '%s', LC_CTYPE = '%s');", c.FQN(), c.Collate, c.Ctype)
}

func GetCollations(connectionPool *dbconn.DBConn) []Collation {
	query := fmt.Sprintf(`
	SELECT c.oid,
		quote_ident(n.nspname) AS schema,
		quote_ident(c.collname) AS name,
		c.collcollate AS collate,
		c.collctype AS ctype
	FROM pg_collation c
		JOIN pg_namespace n ON c.collnamespace = n.oid
	WHERE %s`, SchemaFilterClause("n"))

	results := make([]Collation, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}
