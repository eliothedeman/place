package main

import (
	"io"
	"os"

	"github.com/eliothedeman/check"
	"github.com/eliothedeman/place"
	"github.com/eliothedeman/quack"
)

type stack struct{}

func (s *stack) Run(args []string) {
	fs := check.Must(place.NewFS("db.db", args...))
	o := check.Must(fs.Open("yolo", os.O_CREATE, 0666))
	o.Write([]byte("hello world"))
	o.Close()
	f := check.Must(fs.Open("beast", os.O_CREATE, os.ModeDir|0666))
	defer f.Close()
	io.Copy(os.Stderr, f)
}

func main() {
	check.Must(quack.BindCobra("place", quack.Map{
		"stack": new(stack),
	})).Execute()
}
