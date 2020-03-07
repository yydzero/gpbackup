package backup

/*
 * This file contains structs and functions related to backing up metadata on the
 * master for objects that don't fall under any other predata categorization,
 * such as procedural languages and constraints, that needs to be restored
 * before data is restored.
 */

import (
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateOperatorStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, operator Operator, operatorMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(operator.GetCreateStatement())

	section, entry := operator.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, operatorMetadata, operator, "")
}

/*
 * Operator families are not supported in GPDB 4.3, so this function
 * is not used in a 4.3 backup.
 */
func PrintCreateOperatorFamilyStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, operatorFamilies []OperatorFamily, operatorFamilyMetadata MetadataMap) {
	for _, operatorFamily := range operatorFamilies {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(operatorFamily.GetCreateStatement())

		section, entry := operatorFamily.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, operatorFamilyMetadata[operatorFamily.GetUniqueID()], operatorFamily, "")
	}
}

func PrintCreateOperatorClassStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, operatorClass OperatorClass, operatorClassMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(operatorClass.GetCreateStatement())

	section, entry := operatorClass.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, operatorClassMetadata, operatorClass, "")
}
