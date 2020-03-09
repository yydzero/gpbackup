package backup

/*
 * This file contains structs and functions related to backing up "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateIndexStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, indexes []IndexDefinition, indexMetadata MetadataMap) {
	for _, index := range indexes {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(index.GetCreateStatement())

		section, entry := index.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, indexMetadata[index.GetUniqueID()], index, "")
	}
}

func PrintCreateRuleStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, rules []RuleDefinition, ruleMetadata MetadataMap) {
	for _, rule := range rules {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(rule.GetCreateStatement())

		section, entry := rule.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		tableFQN := utils.MakeFQN(rule.OwningSchema, rule.OwningTable)
		PrintObjectMetadata(metadataFile, toc, ruleMetadata[rule.GetUniqueID()], rule, tableFQN)
	}
}

func PrintCreateTriggerStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, triggers []TriggerDefinition, triggerMetadata MetadataMap) {
	for _, trigger := range triggers {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(trigger.GetCreateStatement())

		section, entry := trigger.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		tableFQN := utils.MakeFQN(trigger.OwningSchema, trigger.OwningTable)
		PrintObjectMetadata(metadataFile, toc, triggerMetadata[trigger.GetUniqueID()], trigger, tableFQN)
	}
}

func PrintCreateEventTriggerStatements(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, eventTriggers []EventTrigger, eventTriggerMetadata MetadataMap) {
	for _, eventTrigger := range eventTriggers {
		start := metadataFile.ByteCount
		section, entry := eventTrigger.GetMetadataEntry()

		metadataFile.MustPrintf(eventTrigger.GetCreateStatements())
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, eventTriggerMetadata[eventTrigger.GetUniqueID()], eventTrigger, "")
	}
}
