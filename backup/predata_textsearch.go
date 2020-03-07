package backup

/*
 * This file contains structs and functions related to backing up metadata on the
 * master for objects relating to built-in text search that needs to be restored
 * before data is restored.
 *
 * Text search is not supported in GPDB 4.3, so none of these structs or functions
 * are used in a 4.3 backup.
 */

import (
	"sort"
	"strings"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateTextSearchParserStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, parser TextSearchParser, parserMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(parser.GetCreateStatement())

	section, entry := parser.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, parserMetadata, parser, "")
}

func PrintCreateTextSearchTemplateStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, template TextSearchTemplate, templateMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(template.GetCreateStatement())

	section, entry := template.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, templateMetadata, template, "")
}

func PrintCreateTextSearchDictionaryStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, dictionary TextSearchDictionary, dictionaryMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(dictionary.GetCreateStatement())

	section, entry := dictionary.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	PrintObjectMetadata(metadataFile, toc, dictionaryMetadata, dictionary, "")
}

func PrintCreateTextSearchConfigurationStatement(metadataFile *utils.FileWithByteCount,
	toc *toc.TOC, configuration TextSearchConfiguration, configurationMetadata ObjectMetadata) {
	start := metadataFile.ByteCount
	metadataFile.MustPrintf(configuration.GetCreateStatement())

	section, entry := configuration.GetMetadataEntry()
	toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)

	tokens := make([]string, 0)
	for token := range configuration.TokenToDicts {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	for _, token := range tokens {
		start := metadataFile.ByteCount
		dicts := configuration.TokenToDicts[token]
		metadataFile.MustPrintf("\n\nALTER TEXT SEARCH CONFIGURATION %s", configuration.FQN())
		metadataFile.MustPrintf("\n\tADD MAPPING FOR \"%s\" WITH %s;", token, strings.Join(dicts, ", "))
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
	}
	PrintObjectMetadata(metadataFile, toc, configurationMetadata, configuration, "")
}
