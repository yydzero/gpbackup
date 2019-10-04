// +build tools

package tools

import (
	_ "github.com/onsi/ginkgo/ginkgo"
	_ "golang.org/x/tools/cmd/goimports"
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
)
