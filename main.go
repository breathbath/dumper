package main

import (
	"github.com/breathbath/dumper/cmd"
	"github.com/breathbath/go_utils/v3/pkg/errs"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	errs.FailOnError(err)

	err = cmd.Execute()
	errs.FailOnError(err)
}
