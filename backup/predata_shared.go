package backup

/*
 * This file contains structs and functions related to backing up metadata shared
 * among many or all object types (privileges, owners, and comments) on the
 * master that needs to be restored before data is restored.
 */

import (
	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * There's no built-in function to generate constraint definitions like there is for
 * other types of metadata, so this function constructs them.
 */
func PrintConstraintStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC,
	constraints []Constraint, conMetadata MetadataMap) {
	allConstraints := make([]Constraint, 0)
	allFkConstraints := make([]Constraint, 0)
	/*
	 * Because FOREIGN KEY constraints must be backed up after PRIMARY KEY
	 * constraints, we separate the two types then concatenate the lists,
	 * so FOREIGN KEY are guaranteed to be printed last.
	 */
	for _, constraint := range constraints {
		if constraint.ConType == "f" {
			allFkConstraints = append(allFkConstraints, constraint)
		} else {
			allConstraints = append(allConstraints, constraint)
		}
	}
	constraints = append(allConstraints, allFkConstraints...)

	for _, constraint := range constraints {
		start := metadataFile.ByteCount
		if constraint.IsDomainConstraint {
			continue
		}
		metadataFile.MustPrintf(constraint.GetCreateStatements())

		section, entry := constraint.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, conMetadata[constraint.GetUniqueID()], constraint, constraint.OwningObject)
	}
}

func PrintCreateSchemaStatements(metadataFile *utils.FileWithByteCount, toc *toc.TOC,
	schemas []Schema, schemaMetadata MetadataMap) {
	for _, schema := range schemas {
		start := metadataFile.ByteCount
		metadataFile.MustPrintf(schema.GetCreateStatement())

		section, entry := schema.GetMetadataEntry()
		toc.AddMetadataEntry(section, entry, start, metadataFile.ByteCount)
		PrintObjectMetadata(metadataFile, toc, schemaMetadata[schema.GetUniqueID()], schema, "")
	}
}
