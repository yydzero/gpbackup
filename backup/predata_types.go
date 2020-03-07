package backup

/*
 * This file contains structs and functions related to backing up type
 * metadata on the master that needs to be restored before data is restored.
 */

import (
	"fmt"
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Functions to print to the predata file
 */

func PrintCreateTypeStatements(metadataFile *utils.FileWithByteCount, tocfile *toc.TOC, shellTypes []ShellType, baseTypes []BaseType, rangeTypes []RangeType) {
	metadataFile.MustPrintf("\n\n")

	types := make([]toc.TOCObjectWithMetadata, 0)
	for _, shellType := range shellTypes {
		types = append(types, toc.TOCObjectWithMetadata(shellType))
	}
	for _, baseType := range baseTypes {
		types = append(types, toc.TOCObjectWithMetadata(baseType))
	}
	for _, rangeType := range rangeTypes {
		types = append(types, toc.TOCObjectWithMetadata(rangeType))
	}

	for _, typ := range types {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf("CREATE TYPE %s;\n", typ.FQN())

		section, entry := typ.GetMetadataEntry()
		tocfile.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	}
}

func PrintCreateDomainStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, domain Domain, typeMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(domain.GetCreateStatement())

	section, entry := domain.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, typeMetadata, domain, "")
}

func PrintCreateBaseTypeStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, base BaseType, typeMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(base.GetCreateStatement())

	section, entry := base.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, typeMetadata, base, "")
}

func PrintCreateCompositeTypeStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, composite CompositeType, typeMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(composite.GetCreateStatement())

	section, entry := composite.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	printPostCreateCompositeTypeStatement(metadataFile, toc, composite, typeMetadata)
}

func printPostCreateCompositeTypeStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, composite CompositeType, typeMetadata ObjectMetadata) {
	PrintObjectMetadata(metadataFile, toc, typeMetadata, composite, "")
	statements := make([]string, 0)
	for _, att := range composite.Attributes {
		if att.Comment != "" {
			statements = append(statements, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;", composite.FQN(), att.Name, att.Comment))
		}
	}
	PrintStatements(metadataFile, toc, composite, statements)
}

func PrintCreateEnumTypeStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, enums []EnumType, typeMetadata MetadataMap) {
	for _, enum := range enums {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(enum.GetCreateStatement())

		section, entry := enum.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, typeMetadata[enum.GetUniqueID()], enum, "")
	}
}

func PrintCreateRangeTypeStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, rangeType RangeType, typeMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(rangeType.GetCreateStatement())

	section, entry := rangeType.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, typeMetadata, rangeType, "")
}

func PrintCreateCollationStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, collations []Collation, collationMetadata MetadataMap) {
	for _, collation := range collations {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(collation.GetCreateStatement())

		section, entry := collation.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, collationMetadata[collation.GetUniqueID()], collation, "")
	}
}
