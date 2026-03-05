package main

import (
	"os"

	"github.com/thetherington/countbeat/cmd"

	_ "github.com/thetherington/countbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
