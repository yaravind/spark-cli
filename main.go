package main

import (
	"fmt"
	cli "gopkg.in/urfave/cli.v2"
	"io/ioutil"
	"net/http"
	"os"
)

func main() {
	const baseHistoryApiUrl = "http://localhost:18080/api/v1/"
	cliApp := &cli.App{
		Name:        "spark-cli",
		Usage:       "CLI for Apache Spark REST API",
		Version:     "0.1.0",
		Description: "Fetches data from the Spark History Server REST API.",
		Authors: []*cli.Author{
			{
				Name:  "Aravind R. Yarram",
				Email: "yaravind@gmail.com",
			},
		},
		EnableShellCompletion: true,
		Commands: []*cli.Command{
			{
				Name:  "apps",
				Usage: "Lists all Spark applications",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "completed",
						Aliases: []string{"c"},
						Usage:   "Lists all 'completed' spark applications",
					},
					&cli.BoolFlag{
						Name:    "running",
						Aliases: []string{"r"},
						Usage:   "Lists all 'running' spark applications",
					},
				},
				Action: func(c *cli.Context) error {
					fmt.Printf("Total Args = %d, Args=%s", c.NArg(), c.Args())
					fmt.Println()
					fmt.Printf("IsSet(Completed) = %t, IsSet(Running) = %t", c.IsSet("completed"), c.IsSet("running"))
					fmt.Println()

					var url string = baseHistoryApiUrl + "applications"

					if c.IsSet("completed") {
						fmt.Println("Listing all 'completed' applications")

						url = url + "?status=completed"
						respStr := get(url)
						fmt.Println(respStr)
					} else if c.IsSet("running") {
						fmt.Println("Listing all 'running' applications")

						url = url + "?status=running"
						respStr := get(url)
						fmt.Println(respStr)
					} else {
						fmt.Println("Listing all applications")

						respStr := get(url)
						fmt.Println(respStr)
					}
					return nil
				},
			},
		},
	}

	cliApp.Run(os.Args)
}

func get(url string) (respStr string) {
	fmt.Printf("GET %s\n", url)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
	} else {
		defer resp.Body.Close()
		buff, _ := ioutil.ReadAll(resp.Body)
		respStr = string(buff)
	}

	return
}
