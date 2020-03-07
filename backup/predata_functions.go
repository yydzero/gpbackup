package backup

/*
 * This file contains structs and functions related to backing up function
 * metadata, and metadata closely related to functions such as aggregates
 * and casts, that needs to be restored before data is restored.
 */

import (
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateFunctionStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, funcDef Function, funcMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	funcFQN := utils.MakeFQN(funcDef.Schema, funcDef.Name)
	metadataFile.MustPrintf("\n\nCREATE FUNCTION %s(%s) RETURNS ", funcFQN, funcDef.Arguments)
	metadataFile.MustPrintf("%s AS", funcDef.ResultType)
	PrintFunctionBodyOrPath(metadataFile, funcDef)
	metadataFile.MustPrintf("LANGUAGE %s", funcDef.Language)
	PrintFunctionModifiers(metadataFile, funcDef)
	metadataFile.MustPrintln(";")

	section, entry := funcDef.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, funcMetadata, funcDef, "")
}

/*
 * This function either prints a path to an executable function (for C and
 * internal functions) or a function definition (for functions in other languages).
 */
func PrintFunctionBodyOrPath(metadataFile *utils.FileWithByteCount, funcDef Function) {
	/*
	 * pg_proc.probin uses either NULL (in this case an empty string) or "-"
	 * to signify an unused path, for historical reasons.  See dumpFunc in
	 * pg_dump.c for details.
	 */
	if funcDef.BinaryPath != "" && funcDef.BinaryPath != "-" {
		metadataFile.MustPrintf("\n'%s', '%s'\n", funcDef.BinaryPath, funcDef.FunctionBody)
	} else {
		metadataFile.MustPrintf("\n%s\n", utils.DollarQuoteString(funcDef.FunctionBody))
	}
}

func PrintFunctionModifiers(metadataFile *utils.FileWithByteCount, funcDef Function) {
	metadataFile.MustPrintf(funcDef.GetFunctionModifiers())
}

func PrintCreateAggregateStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, aggDef Aggregate, funcInfoMap map[uint32]FunctionInfo, aggMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(aggDef.GetCreateStatement(funcInfoMap))
	section, entry := aggDef.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, aggMetadata, aggDef, "")
}

func PrintCreateCastStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, castDef Cast, castMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(castDef.GetCreateStatement())
	section, entry := castDef.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, castMetadata, castDef, "")
}

func PrintCreateExtensionStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC, extensionDefs []Extension, extensionMetadata MetadataMap) {
	for _, extensionDef := range extensionDefs {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("\n\nSET search_path=%s,pg_catalog;\nCREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s;\nSET search_path=pg_catalog;", extensionDef.Schema, extensionDef.Name, extensionDef.Schema)

		section, entry := extensionDef.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, extensionMetadata[extensionDef.GetUniqueID()], extensionDef, "")
	}
}

/*
 * This function separates out functions related to procedural languages from
 * any other functions, so that language-related functions can be backed up before
 * the languages themselves and we can avoid sorting languages and functions
 * together to resolve dependencies.
 */
func ExtractLanguageFunctions(funcDefs []Function, procLangs []ProceduralLanguage) ([]Function, []Function) {
	isLangFuncMap := make(map[uint32]bool)
	for _, procLang := range procLangs {
		for _, funcDef := range funcDefs {
			isLangFuncMap[funcDef.Oid] = isLangFuncMap[funcDef.Oid] ||
				funcDef.Oid == procLang.Handler ||
				funcDef.Oid == procLang.Inline ||
				funcDef.Oid == procLang.Validator
		}
	}
	langFuncs := make([]Function, 0)
	otherFuncs := make([]Function, 0)
	for _, funcDef := range funcDefs {
		if isLangFuncMap[funcDef.Oid] {
			langFuncs = append(langFuncs, funcDef)
		} else {
			otherFuncs = append(otherFuncs, funcDef)
		}
	}
	return langFuncs, otherFuncs
}

func PrintCreateLanguageStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC,
	procLangs []ProceduralLanguage, funcInfoMap map[uint32]FunctionInfo, procLangMetadata MetadataMap) {
	for _, procLang := range procLangs {
		start := metadataFile.ByteCount
		metadataFile.MustPrint(procLang.GetCreateStatement(funcInfoMap))

		section, entry := procLang.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, procLangMetadata[procLang.GetUniqueID()], procLang, "")
	}
}

func PrintCreateConversionStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, conversions []Conversion, conversionMetadata MetadataMap) {
	for _, conversion := range conversions {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(conversion.GetCreateStatement())

		section, entry := conversion.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, conversionMetadata[conversion.GetUniqueID()], conversion, "")
	}
}

func PrintCreateForeignDataWrapperStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC,
	fdw ForeignDataWrapper, funcInfoMap map[uint32]FunctionInfo, fdwMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(fdw.GetCreateStatement(funcInfoMap))

	section, entry := fdw.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, fdwMetadata, fdw, "")
}

func PrintCreateServerStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, server ForeignServer, serverMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(server.GetCreateStatement())

	//NOTE: We must specify SERVER when creating and dropping, but FOREIGN SERVER when granting and revoking
	section, entry := server.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, serverMetadata, server, "")
}

func PrintCreateUserMappingStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, mapping UserMapping) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(mapping.GetCreateStatement())

	section, entry := mapping.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
}
