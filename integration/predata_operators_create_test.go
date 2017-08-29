package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration create statement tests", func() {
	var toc utils.TOC
	var backupfile *utils.FileWithByteCount
	BeforeEach(func() {
		backupfile = utils.NewFileWithByteCount(buffer)
	})
	Describe("PrintCreateOperatorStatements", func() {
		It("creates operator", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION testschema.\"testFunc\" (path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION testschema.\"testFunc\" (path,path)")

			operator := backup.Operator{Oid: 0, SchemaName: "testschema", Name: "##", ProcedureName: "testschema.\"testFunc\"", LeftArgType: "path", RightArgType: "path", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}
			operators := []backup.Operator{operator}

			backup.PrintCreateOperatorStatements(backupfile, &toc, operators, backup.MetadataMap{})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.##(path, path)")

			resultOperators := backup.GetOperators(connection)
			Expect(len(resultOperators)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operator, &resultOperators[0], "Oid")
		})
		It("creates operator with owner and comment", func() {
			testutils.AssertQueryRuns(connection, "CREATE SCHEMA testschema")
			defer testutils.AssertQueryRuns(connection, "DROP SCHEMA testschema")

			testutils.AssertQueryRuns(connection, "CREATE FUNCTION testschema.\"testFunc\" (path,path) RETURNS path AS 'SELECT $1' LANGUAGE SQL IMMUTABLE")
			defer testutils.AssertQueryRuns(connection, "DROP FUNCTION testschema.\"testFunc\" (path,path)")

			operatorMetadataMap := testutils.DefaultMetadataMap("OPERATOR", false, false, true)
			operatorMetadata := operatorMetadataMap[1]
			operator := backup.Operator{Oid: 1, SchemaName: "testschema", Name: "##", ProcedureName: "testschema.\"testFunc\"", LeftArgType: "path", RightArgType: "path", CommutatorOp: "0", NegatorOp: "0", RestrictFunction: "-", JoinFunction: "-", CanHash: false, CanMerge: false}
			operators := []backup.Operator{operator}

			backup.PrintCreateOperatorStatements(backupfile, &toc, operators, operatorMetadataMap)
			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR testschema.##(path, path)")

			resultOperators := backup.GetOperators(connection)
			Expect(len(resultOperators)).To(Equal(1))
			resultMetadataMap := backup.GetCommentsForObjectType(connection, backup.TYPE_OPERATOR)
			resultMetadata := resultMetadataMap[resultOperators[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&operator, &resultOperators[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &operatorMetadata, "Oid")
		})
	})
	Describe("PrintCreateOperatorFamilyStatements", func() {
		It("creates operator family", func() {
			operatorFamily := backup.OperatorFamily{Oid: 1, SchemaName: "public", Name: "testfam", IndexMethod: "hash"}
			operatorFamilies := []backup.OperatorFamily{operatorFamily}

			backup.PrintCreateOperatorFamilyStatements(backupfile, &toc, operatorFamilies, backup.MetadataMap{})

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testfam USING hash")

			resultOperatorFamilies := backup.GetOperatorFamilies(connection)
			Expect(len(resultOperatorFamilies)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operatorFamily, &resultOperatorFamilies[0], "Oid")
		})
		It("creates operator family with owner and comment", func() {
			operatorFamilyMetadataMap := testutils.DefaultMetadataMap("OPERATOR FAMILY", false, true, true)
			operatorFamilyMetadata := operatorFamilyMetadataMap[1]
			operatorFamily := backup.OperatorFamily{Oid: 1, SchemaName: "public", Name: "testfam", IndexMethod: "hash"}
			operatorFamilies := []backup.OperatorFamily{operatorFamily}

			backup.PrintCreateOperatorFamilyStatements(backupfile, &toc, operatorFamilies, operatorFamilyMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testfam USING hash")

			resultOperatorFamilies := backup.GetOperatorFamilies(connection)
			Expect(len(resultOperatorFamilies)).To(Equal(1))
			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATORFAMILY)
			resultMetadata := resultMetadataMap[resultOperatorFamilies[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&operatorFamily, &resultOperatorFamilies[0], "Oid")
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &operatorFamilyMetadata, "Oid")
		})
	})
	Describe("PrintCreateOperatorClassStatements", func() {
		It("creates basic operator class", func() {
			operatorClass := backup.OperatorClass{Oid: 0, ClassSchema: "public", ClassName: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			backup.PrintCreateOperatorClassStatements(backupfile, &toc, []backup.OperatorClass{operatorClass}, nil)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")

			resultOperatorClasses := backup.GetOperatorClasses(connection)
			Expect(len(resultOperatorClasses)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid")
		})
		It("creates complex operator class", func() {
			operatorClass := backup.OperatorClass{Oid: 0, ClassSchema: "public", ClassName: "testclass", FamilySchema: "public", FamilyName: "testfam", IndexMethod: "gist", Type: "uuid", Default: true, StorageType: "integer", Operators: nil, Functions: nil}
			operatorClass.Operators = []backup.OperatorClassOperator{{ClassOid: 0, StrategyNumber: 1, Operator: "=(uuid,uuid)", Recheck: true}}
			operatorClass.Functions = []backup.OperatorClassFunction{{ClassOid: 0, SupportNumber: 1, FunctionName: "abs(integer)"}}

			testutils.AssertQueryRuns(connection, "CREATE OPERATOR FAMILY public.testfam USING gist")
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testfam USING gist CASCADE")
			backup.PrintCreateOperatorClassStatements(backupfile, &toc, []backup.OperatorClass{operatorClass}, nil)

			testutils.AssertQueryRuns(connection, buffer.String())

			resultOperatorClasses := backup.GetOperatorClasses(connection)
			Expect(len(resultOperatorClasses)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid", "Operators.ClassOid", "Functions.ClassOid")
		})
		It("creates basic operator class with a comment and owner", func() {
			operatorClassMetadataMap := testutils.DefaultMetadataMap("OPERATOR CLASS", false, true, true)
			operatorClassMetadata := operatorClassMetadataMap[1]

			operatorClass := backup.OperatorClass{Oid: 1, ClassSchema: "public", ClassName: "testclass", FamilySchema: "public", FamilyName: "testclass", IndexMethod: "hash", Type: "uuid", Default: false, StorageType: "-", Operators: nil, Functions: nil}

			backup.PrintCreateOperatorClassStatements(backupfile, &toc, []backup.OperatorClass{operatorClass}, operatorClassMetadataMap)

			testutils.AssertQueryRuns(connection, buffer.String())
			defer testutils.AssertQueryRuns(connection, "DROP OPERATOR FAMILY public.testclass USING hash CASCADE")

			resultOperatorClasses := backup.GetOperatorClasses(connection)
			Expect(len(resultOperatorClasses)).To(Equal(1))
			testutils.ExpectStructsToMatchExcluding(&operatorClass, &resultOperatorClasses[0], "Oid")

			resultMetadataMap := backup.GetMetadataForObjectType(connection, backup.TYPE_OPERATORCLASS)
			resultMetadata := resultMetadataMap[resultOperatorClasses[0].Oid]
			testutils.ExpectStructsToMatchExcluding(&resultMetadata, &operatorClassMetadata, "Oid")

		})
	})
})