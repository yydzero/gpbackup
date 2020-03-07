package backup

/*
 * This file contains structs and functions related to backing up metadata on the
 * master for objects that connect to external data (external tables and external
 * protocols).
 */

import (
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintExternalTableCreateStatement(metadataFile *utils.FileWithByteCount, toc *toc.TOC, table Table) {
	start := metadataFile.ByteCount
	tableTypeStrMap := map[int]string{
		READABLE:     "READABLE EXTERNAL",
		READABLE_WEB: "READABLE EXTERNAL WEB",
		WRITABLE:     "WRITABLE EXTERNAL",
		WRITABLE_WEB: "WRITABLE EXTERNAL WEB",
	}
	extTableDef := table.ExtTableDef
	extTableDef.Type, extTableDef.Protocol = extTableDef.GetTableCharacteristics()
	metadataFile.MustPrintf("\n\nCREATE %s TABLE %s (\n", tableTypeStrMap[extTableDef.Type], table.FQN())
	printColumnDefinitions(metadataFile, table.ColumnDefs, "")
	metadataFile.MustPrintf(") ")
	PrintExternalTableStatements(metadataFile, table.FQN(), extTableDef)
	if extTableDef.Writable {
		metadataFile.MustPrintf("\n%s", table.DistPolicy)
	}
	metadataFile.MustPrintf(";")
	if toc != nil {
		section, entry := table.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	}
}

func PrintExternalTableStatements(metadataFile *utils.FileWithByteCount,
	tableName string, extTableDef ExternalTableDefinition) {
	metadataFile.MustPrint(extTableDef.GetTableStatement(tableName))
}

func PrintCreateExternalProtocolStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, protocol ExternalProtocol, funcInfoMap map[uint32]FunctionInfo, protoMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(protocol.GetCreateStatement(funcInfoMap))

	section, entry := protocol.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, protoMetadata, protocol, "")
}

func PrintExchangeExternalPartitionStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, extPartitions []PartitionInfo, partInfoMap map[uint32]PartitionInfo, tables []Table) {
	tableNameMap := make(map[uint32]string, len(tables))
	for _, table := range tables {
		tableNameMap[table.Oid] = table.FQN()
	}
	for _, externalPartition := range extPartitions {
		extPartRelationName := tableNameMap[externalPartition.RelationOid]
		if extPartRelationName == "" {
			continue //Not included in the list of tables to back up
		}
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(externalPartition.GetExchangeStatement(partInfoMap, extPartRelationName))

		section, entry := externalPartition.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	}
}
