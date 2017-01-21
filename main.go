package main

import (
	"database/sql"
	"encoding/json"
	_ "github.com/mattn/go-sqlite3"
	cli "gopkg.in/urfave/cli.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	//_ "reflect"
)

func main() {
	db, err := sql.Open("sqlite3", ":memory:")
	checkErr(err)
	defer db.Close()

	//fail-fast if can't connect to DB
	checkErr(db.Ping())

	//create table
	_, err = db.Exec("create table APPS (ID integer PRIMARY KEY, APP_ID string not null, NAME string not null, " +
		"DURATION integer, IS_COMPLETED integer, USERSTART_T string, END_T string, LAST_UPDATED_T string, START_E integer, " +
		"END_E integer, LAST_UPDATED_E integer); delete from APPS;")
	checkErr(err)

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

					log.Printf("Total Args = %d, Args=%s", c.NArg(), c.Args())

					log.Printf("IsSet(Completed) = %t, IsSet(Running) = %t", c.IsSet("completed"), c.IsSet("running"))

					var url string = baseHistoryApiUrl + "applications"

					if c.IsSet("completed") {
						log.Println("Listing all 'completed' applications")

						url = url + "?status=completed"
						respStr := getAsStr(url)
						log.Println(respStr)
					} else if c.IsSet("running") {
						log.Println("Listing all 'running' applications")

						url = url + "?status=running"
						respStr := getAsStr(url)
						log.Println(respStr)
					} else {
						log.Println("Listing all applications")
						if apps, err := GetApps(url); err == nil {
							//log.Println(len(*apps))
							insert(db, apps)

							cntTot, cntCompleted, cntIncomplete := GetAppsSummary(db)

							log.Printf("Total Applications: %d (Completed: %d, Incomplete: %d)", cntTot, cntCompleted, cntIncomplete)
						} else {
							checkErr(err)
						}

					}
					return nil
				},
			},
		},
	}

	cliApp.Run(os.Args)
}

//----------------------------------------------------------------------------------------------------------------------
// Core domain types.
//----------------------------------------------------------------------------------------------------------------------

type Attempt struct {
	StartTime        string `json:"startTime"`
	EndTime          string `json:"endTime"`
	LastUpdated      string `json:"lastUpdated"`
	Duration         uint32 `json:"duration"`
	SparkUser        string `json:"sparkUser"`
	IsCompleted      bool   `json:"completed"`
	LastUpdatedEpoch int64  `json:"lastUpdatedEpoch"`
	StartTimeEpoch   int64  `json:"startTimeEpoch"`
	EndTimeEpoch     int64  `json:"EndTimeEpoch"`
}

type Apps struct {
	Id       string    `json:"id"`
	Name     string    `json:"name"`
	Attempts []Attempt `json:"attempts"`
}

func checkErr(err error, args ...string) {
	if err != nil {
		log.Println("Error")
		log.Fatalf("%q: %s", err, args)
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Functions working with Spark REST API.
//----------------------------------------------------------------------------------------------------------------------

func GetApps(url string) (*[]Apps, error) {
	var apps []Apps
	if respBuff, err := get(url); err == nil {
		if jsonErr := json.Unmarshal(respBuff, &apps); jsonErr == nil {
			log.Println(apps)
			return &apps, nil
		} else {
			log.Printf("Response: %s", string(respBuff))
			return nil, err
		}
	} else {
		return nil, err
	}
}

func getAsStr(url string) string {
	if respBuff, err := get(url); err != nil {
		return string(respBuff)
	}
	return "{}"
}

func get(url string) ([]byte, error) {
	log.Printf("GET %s\n", url)
	resp, err := http.Get(url)
	if err != nil {
		log.Println(err)
		return nil, err
	} else {
		defer resp.Body.Close()
		respBuff, _ := ioutil.ReadAll(resp.Body)
		return respBuff, nil
	}
}

//----------------------------------------------------------------------------------------------------------------------
// Functions working with in-memory DB.
//----------------------------------------------------------------------------------------------------------------------

func GetAppsSummary(db *sql.DB) (cntTot, cntCompleted, cntIncomplete int) {
	cntTot, cntCompleted, cntIncomplete = GetAppsTotalCount(db), GetAppsCompleted(db), GetAppsIncomplete(db)
	return
}

func GetAppsTotalCount(db *sql.DB) int {
	var cntTot int
	qryTotal := "select count(distinct APP_ID) from APPS"
	err := db.QueryRow(qryTotal).Scan(&cntTot)
	checkErr(err, qryTotal)

	return cntTot
}

func GetAppsCompleted(db *sql.DB) int {
	var cntCompleted int
	qryIsCompleted := "select count(ID) from APPS where IS_COMPLETED=?"
	err := db.QueryRow(qryIsCompleted, 1).Scan(&cntCompleted)
	checkErr(err, qryIsCompleted)

	return cntCompleted
}

func GetAppsIncomplete(db *sql.DB) int {
	var cntIncomplete int
	qryIsCompleted := "select count(ID) from APPS where IS_COMPLETED=?"
	err := db.QueryRow(qryIsCompleted, 0).Scan(&cntIncomplete)
	checkErr(err, qryIsCompleted)

	return cntIncomplete
}

func insert(db *sql.DB, apps *[]Apps) {
	tx, err := db.Begin()
	checkErr(err)

	stmt, err := tx.Prepare("insert into APPS(APP_ID, NAME, DURATION, IS_COMPLETED, USERSTART_T, END_T, LAST_UPDATED_T, START_E, END_E, LAST_UPDATED_E) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	checkErr(err)

	defer stmt.Close()

	for _, app := range *apps {
		//log.Printf("Inserting App: %s",app.Id)
		for _, attempt := range app.Attempts {
			//log.Printf("\tInserting Attempt: ", attempt.IsCompleted)
			_, err = stmt.Exec(app.Id, app.Name, attempt.Duration, attempt.IsCompleted, attempt.StartTime, attempt.EndTime, attempt.LastUpdated, attempt.StartTimeEpoch, attempt.EndTimeEpoch, attempt.LastUpdatedEpoch)
			checkErr(err)
		}
	}
	tx.Commit()
}

//----------------------------------------------------------------------------------------------------------------------
// Functions working on Apps struct.
//----------------------------------------------------------------------------------------------------------------------

func Summary(apps *[]Apps) (cntTot, cntCompleted, cntIncomplete int) {
	cntTot = len(*apps)
	for _, app := range *apps {
		if app.Attempts[0].IsCompleted {
			cntCompleted++
		} else {
			cntIncomplete++
		}
	}
	return
}
