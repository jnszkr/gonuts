// Command gonuts generates Go code and Terraform from a nuts.yaml file.
package main

import (
	"os"

	"github.com/jnszkr/gonuts/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
