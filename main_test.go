package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestUnmarshalAttempt(t *testing.T) {
	attemptJson := `{
		"startTime": "2016-12-30T03:42:26.828GMT",
		"endTime": "2016-12-30T03:50:05.696GMT",
		"lastUpdated": "2016-12-30T03:50:05.719GMT",
		"duration": 458868,
		"sparkUser": "esha",
		"completed": true,
		"endTimeEpoch": 1483069805696,
		"lastUpdatedEpoch": 1483069805719,
		"startTimeEpoch": 1483069346828
	}`
	attempt := &Attempt{}
	err := json.Unmarshal([]byte(attemptJson), attempt)
	if err != nil {
		t.Fatal(err)
	}

	if attempt.StartTime != "2016-12-30T03:42:26.828GMT" {
		t.Errorf("Expected %s, but got %s. May be unmarshal issue!", "2016-12-30T03:42:26.828GMT", attempt.StartTime)
	}
	if attempt.EndTime != "2016-12-30T03:50:05.696GMT" {
		t.Errorf("Expected %s, but got %s. May be unmarshal issue!", "2016-12-30T03:50:05.696GMT", attempt.EndTime)
	}
	if attempt.LastUpdated != "2016-12-30T03:50:05.719GMT" {
		t.Errorf("Expected %s, but got %s. May be unmarshal issue!", "2016-12-30T03:50:05.719GMT", attempt.LastUpdated)
	}
	if attempt.Duration != 458868 {
		t.Errorf("Expected %s, but got %s. May be unmarshal issue!", 458868, attempt.Duration)
	}
	if attempt.SparkUser != "esha" {
		t.Errorf("Expected %s, but got %s. May be unmarshal issue!", "esha", attempt.SparkUser)
	}
	if attempt.IsCompleted != true {
		t.Errorf("Expected %t, but got %t. May be unmarshal issue!", true, attempt.IsCompleted)
	}
	if attempt.StartTimeEpoch != 1483069346828 {
		t.Errorf("Expected %s, but got %s. May be unmarshal issue!", 1483069346828, attempt.StartTimeEpoch)
	}
	if attempt.EndTimeEpoch != 1483069805696 {
		t.Errorf("Expected %s, but got %s. May be unmarshal issue!", 1483069805696, attempt.EndTimeEpoch)
	}
	if attempt.LastUpdatedEpoch != 1483069805719 {
		t.Errorf("Expected %s, but got %s. May be unmarshal issue!", 1483069805719, attempt.LastUpdatedEpoch)
	}
}

func TestUnmarshalAttempts(t *testing.T) {
	attemptsJson := `[
		{
			"startTime": "2016-12-30T03:42:26.828GMT",
			"endTime": "2016-12-30T03:50:05.696GMT",
			"lastUpdated": "2016-12-30T03:50:05.719GMT",
			"duration": 458868,
			"sparkUser": "esha",
			"completed": true,
			"endTimeEpoch": 1483069805696,
			"lastUpdatedEpoch": 1483069805719,
			"startTimeEpoch": 1483069346828
		},
		{
			"startTime": "2016-12-30T03:42:26.828GMT",
			"endTime": "2016-12-30T03:50:05.696GMT",
			"lastUpdated": "2016-12-30T03:50:05.719GMT",
			"duration": 458868,
			"sparkUser": "esha",
			"completed": true,
			"endTimeEpoch": 1483069805696,
			"lastUpdatedEpoch": 1483069805719,
			"startTimeEpoch": 1483069346828
		}
	]`
	attempts := &[]Attempt{}
	err := json.Unmarshal([]byte(attemptsJson), attempts)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(attempts)
}

func TestUnmarshalApps(t *testing.T) {
	appsJson := `[
	  {
	    "id": "app-20161229224238-0001",
	    "name": "Spark shell",
	    "attempts": [
	      {
		"startTime": "2016-12-30T03:42:26.828GMT",
		"endTime": "2016-12-30T03:50:05.696GMT",
		"lastUpdated": "2016-12-30T03:50:05.719GMT",
		"duration": 458868,
		"sparkUser": "esha",
		"completed": true,
		"endTimeEpoch": 1483069805696,
		"lastUpdatedEpoch": 1483069805719,
		"startTimeEpoch": 1483069346828
	      },
	      {
		"startTime": "2016-12-30T03:42:26.828GMT",
		"endTime": "2016-12-30T03:50:05.696GMT",
		"lastUpdated": "2016-12-30T03:50:05.719GMT",
		"duration": 458868,
		"sparkUser": "esha",
		"completed": true,
		"endTimeEpoch": 1483069805696,
		"lastUpdatedEpoch": 1483069805719,
		"startTimeEpoch": 1483069346828
	      }
	    ]
	  },
	  {
	    "id": "app-20161229222707-0000",
	    "name": "Spark shell",
	    "attempts": [
	      {
		"startTime": "2016-12-30T03:26:50.679GMT",
		"endTime": "2016-12-30T03:38:35.882GMT",
		"lastUpdated": "2016-12-30T03:38:36.013GMT",
		"duration": 705203,
		"sparkUser": "esha",
		"completed": true,
		"endTimeEpoch": 1483069115882,
		"lastUpdatedEpoch": 1483069116013,
		"startTimeEpoch": 1483068410679
	      }
	    ]
	  },
	  {
	    "id": "app-20161218161022-0001",
	    "name": "WordCountApp",
	    "attempts": [
	      {
		"startTime": "2016-12-18T21:09:39.795GMT",
		"endTime": "2016-12-18T21:11:11.718GMT",
		"lastUpdated": "2016-12-18T21:11:11.988GMT",
		"duration": 91923,
		"sparkUser": "esha",
		"completed": true,
		"endTimeEpoch": 1482095471718,
		"lastUpdatedEpoch": 1482095471988,
		"startTimeEpoch": 1482095379795
	      }
	    ]
	  }
	]`
	apps := []Apps{}
	err := json.Unmarshal([]byte(appsJson), &apps)
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 3 {
		t.Errorf("Expected %d, but got %d. May be unmarshal issue!", 3, len(apps))
	}
	if apps[0].Id != "app-20161229224238-0001" {
		t.Errorf("Expected '%s', but got '%s'. May be unmarshal issue!", "app-20161229224238-0001", apps[0].Id)
	}
	if apps[0].Name != "Spark shell" {
		t.Errorf("Expected '%s', but got '%s'. May be unmarshal issue!", "Spark shell", apps[0].Name)
	}
	if len(apps[0].Attempts) != 2 {
		t.Errorf("Expected %d, but got %d. May be unmarshal issue!", 2, len(apps[0].Attempts))
	}
}
