package main

import (
	"os"

	"github.com/thetherington/routelogbeat/cmd"

	_ "github.com/thetherington/routelogbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
