package utils

import "strings"

/*
 * Filter structure to filter schemas and relations
 */

type Filters struct {
	IncludeSchemas   []string
	ExcludeSchemas   []string
	IncludeRelations []string
	ExcludeRelations []string
}

func NewFilters(inSchema []string, exSchemas []string, inRelations []string, exRelations []string) Filters {
	f := Filters{}
	f.IncludeSchemas = inSchema
	f.ExcludeSchemas = exSchemas
	f.IncludeRelations = inRelations
	f.ExcludeRelations = exRelations
return f
}

func FiltersEmpty(filters Filters) bool {
	return len(filters.IncludeSchemas) == 0 && len(filters.ExcludeSchemas) == 0 && len(filters.IncludeRelations) == 0 && len(filters.ExcludeRelations) == 0
}

func SchemaIsExcluded(inSchemas []string, exSchemas []string, schemaName string) bool{
	included := Exists(inSchemas, schemaName) || len(inSchemas) == 0
	excluded := Exists(exSchemas, schemaName)
	return excluded || !included
}

func RelationIsExcluded(inRelations []string, exRelations []string, tableFQN string) bool{
	included := Exists(inRelations, tableFQN) || len(inRelations) == 0
	excluded := Exists(exRelations, tableFQN)
	return excluded || !included
}

func FilterRelations(relations []string, filters Filters) []string {
	var relationsFiltered []string

	for _, relation := range relations {
		schemaName := strings.Split(relation,".")[0]
		if RelationIsExcluded(filters.IncludeRelations, filters.ExcludeRelations, relation) ||
		   SchemaIsExcluded(filters.IncludeSchemas, filters.IncludeSchemas, schemaName){
			continue
		}
		relationsFiltered = append(relationsFiltered, relation)
	}

	return relationsFiltered
}
