package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

var databaseNames = []string{"chainlink_test1", "chainlink_test2", "chainlink_test3"}

//packagearg="${1:-./...}"
//PKGS=$(go list $packagearg | grep -v /vendor/)
//echo ${PKGS}
//
//declare -a DATABASES=("chainlink_test1" "chainlink_test2" "chainlink_test3")
//
//DATABASE_URL=postgres://localhost:5432/chainlink_test?sslmode=disable go test -cpu 1 -parallel 1 -p 1 github.com/smartcontractkit/chainlink/core/utils github.com/smartcontractkit/chainlink/core/web

func main() {
	for _, name := range databaseNames {
		checkOutput(exec.Command("dropdb", "--if-exists", name).CombinedOutput())
	}

	for _, name := range databaseNames {
		check(exec.Command("createdb", name).Run())
	}

	golist, err := exec.Command("go", "list", "./...").CombinedOutput()
	checkOutput(golist, err)

	var packages []string
	for _, entry := range strings.Split(string(golist), "\n") {
		if entry != "" && !strings.Contains(entry, "/vendor/") {
			packages = append(packages, strings.TrimSpace(entry))
		}
	}

	//outputs := createOutputs()
	var b bytes.Buffer
	chunks := chunkify(packages)
	running := []*exec.Cmd{}
	for i, chunk := range chunks {
		fmt.Println("Chunk ", i, " is ", chunk)
		args := []string{"test", "-cpu", "1", "-parallel", "1", "-p", "1"}
		cmd := exec.Command("go", append(args, chunk...)...)
		cmd.Dir = "/Users/dimroc/go/src/github.com/smartcontractkit/chainlink"

		additionalEnv := fmt.Sprintf("DATABASE_URL=postgres://localhost:5432/%s?sslmode=disable", databaseNames[i])
		newEnv := append(os.Environ(), additionalEnv)
		cmd.Env = newEnv
		cmd.Stdout = &b
		cmd.Stderr = &b
		check(cmd.Start())
		running = append(running, cmd)
	}

	for _, cmd := range running {
		if err := cmd.Wait(); err != nil {
			fmt.Print(string(b.Bytes()))
			panic(err)
		}
	}

	fmt.Println("=== DONE!")
}

//func createOutputs() {
//outputs := []*os.File{}
////dsts := "
//to, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE, 0666)
//check(err)

//}

func chunkify(pkgs []string) [][]string {
	var arr0, arr1, arr2 []string
	for i, entry := range pkgs {
		placement := i % 3
		switch placement {
		case 0:
			arr0 = append(arr0, entry)
		case 1:
			arr1 = append(arr1, entry)
		case 2:
			arr2 = append(arr2, entry)
		}
	}
	return [][]string{arr0, arr1, arr2}
}

func checkOutput(output []byte, err error) {
	if err != nil {
		log.Panic(string(output), err)
	}
}

func check(err error) {
	if err != nil {
		log.Panic(err)
	}
}
