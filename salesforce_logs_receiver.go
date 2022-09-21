package salesforce_logs_receiver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/avast/retry-go"
	"github.com/simpleforce/simpleforce"
)

const (
	EventLogFileSObjectName = "eventlogfile"
	defaultApiVersion       = "55.0"
)

var (
	debugLogger = log.New(os.Stderr, "DEBUG: ", log.Ldate|log.Ltime)
)

type SalesforceLogsReceiver struct {
	SObjects      []*SObjectToCollect
	username      string
	password      string
	securityToken string
	customFields  map[string]string
	client        *simpleforce.Client
}

type SObjectToCollect struct {
	SObjectType     string
	LatestTimestamp string
}

func NewSalesforceLogsReceiver(
	url string,
	clientID string,
	apiVersion string,
	username string,
	password string,
	securityToken string,
	sObjects []*SObjectToCollect,
	customFields map[string]string) (*SalesforceLogsReceiver, error) {
	if clientID == "" {
		return nil, fmt.Errorf("client ID must have a value")
	}

	if username == "" {
		return nil, fmt.Errorf("username must have a value")
	}

	if password == "" {
		return nil, fmt.Errorf("password must have a value")
	}

	if securityToken == "" {
		return nil, fmt.Errorf("security token must have a value")
	}

	if len(sObjects) == 0 {
		return nil, fmt.Errorf("sObjects must have a value")
	}

	currentTimeMinusOneHour := time.Now().Add(-time.Hour * 1).Format("2006-01-02T15:04:05.000Z")

	for _, sObject := range sObjects {
		if sObject.SObjectType == "" {
			return nil, fmt.Errorf("sObject type must have a value")
		}
		if sObject.LatestTimestamp == "" {
			sObject.LatestTimestamp = currentTimeMinusOneHour
			continue
		}
		if _, err := time.Parse("2006-01-02T15:04:05.000Z", sObject.LatestTimestamp); err != nil {
			return nil, fmt.Errorf("sObject latest timestamp must be in 2006-01-02T15:04:05.000Z format")
		}
	}

	if url == "" {
		url = simpleforce.DefaultURL
	}

	if apiVersion == "" {
		apiVersion = defaultApiVersion
	}

	client := simpleforce.NewClient(url, clientID, apiVersion)
	if client == nil {
		return nil, fmt.Errorf("error creating Salesforce client")
	}

	return &SalesforceLogsReceiver{
		SObjects:      sObjects,
		username:      username,
		password:      password,
		securityToken: securityToken,
		customFields:  customFields,
		client:        client,
	}, nil
}

func (slr *SalesforceLogsReceiver) LoginSalesforce() error {
	if err := slr.client.LoginPassword(slr.username, slr.password, slr.securityToken); err != nil {
		return fmt.Errorf("error login Salesforce API: %w", err)
	}

	debugLogger.Println("Logged in to Salesforce. Got new access token")
	return nil
}

func (slr *SalesforceLogsReceiver) GetSObjectRecords(sObject *SObjectToCollect) ([]simpleforce.SObject, error) {
	query := fmt.Sprintf("SELECT Id,CreatedDate FROM %s WHERE CreatedDate > %s", sObject.SObjectType, sObject.LatestTimestamp)
	result, err := slr.client.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying Salesforce API: %w", err)
	}

	debugLogger.Println("Got", len(result.Records), "records of sObject", sObject.SObjectType)
	return result.Records, nil
}

func (slr *SalesforceLogsReceiver) CollectSObjectRecord(record *simpleforce.SObject) ([]byte, *string, error) {
	id := record.ID()
	data := record.Get(id)

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, nil, fmt.Errorf("error marshaling data from Salesforce API: %w", err)
	}

	jsonData, err = slr.addCustomFields(jsonData)
	if err != nil {
		return nil, nil, fmt.Errorf("error adding custom fields to data: %w", err)
	}

	createdDate := record.StringField("CreatedDate")
	createdDate = strings.Replace(createdDate, "+0000", "Z", 1)

	debugLogger.Println("Collected data of sObject", record.Type(), "record ID", id)
	return jsonData, &createdDate, nil
}

func (slr *SalesforceLogsReceiver) addCustomFields(jsonData []byte) ([]byte, error) {
	if len(slr.customFields) == 0 {
		return jsonData, nil
	}

	var jsonMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &jsonMap); err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON data: %w", err)
	}

	for fieldKey, fieldValue := range jsonMap {
		jsonMap[fieldKey] = fieldValue
	}

	newJsonData, err := json.Marshal(jsonMap)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON data: %w", err)
	}

	return newJsonData, nil
}

func (slr *SalesforceLogsReceiver) EnrichEventLogFileSObjectData(data *simpleforce.SObject, jsonData []byte) ([][]byte, error) {
	eventLogRows, err := slr.getEventLogFileContent(data)
	if err != nil {
		return nil, fmt.Errorf("error getting EventLogFile sObject log file content: %w", err)
	}

	debugLogger.Println("Got", len(eventLogRows), "events from EventLogFile sObject ID", data.ID())

	var jsonsData [][]byte
	for _, eventLogRow := range eventLogRows {
		newJsonData, err := addEventLogToJsonData(eventLogRow, jsonData)
		if err != nil {
			return nil, fmt.Errorf("error adding event log content to JSON data: %w", err)
		}

		jsonsData = append(jsonsData, newJsonData)
	}

	debugLogger.Println("Enriched JSON data with", len(jsonsData), "events from EventLogFile sObject ID", data.ID())
	return jsonsData, nil
}

func (slr *SalesforceLogsReceiver) getEventLogFileContent(data *simpleforce.SObject) ([]map[string]interface{}, error) {
	apiPath := data.StringField("LogFile")
	logFileContent, err := slr.getFileContent(apiPath)
	if err != nil {
		return nil, fmt.Errorf("error getting event log file content: %w", err)
	}

	trimmedLogFileContent := strings.Replace(string(logFileContent), "\n\n", "\n", -1)
	debugLogger.Println("Got EventLogFile sObject log file content ID", data.ID())

	reader := strings.NewReader(trimmedLogFileContent)
	csvReader := csv.NewReader(reader)

	csvData, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV data: %w", err)
	}

	var logEvents []map[string]interface{}
	for rowIndex, row := range csvData {
		if rowIndex == 0 {
			continue
		}

		logEvent := make(map[string]interface{})
		for fieldIndex, field := range row {
			key := csvData[0][fieldIndex]
			logEvent[key] = field
		}

		logEvents = append(logEvents, logEvent)
	}

	return logEvents, nil
}

func (slr *SalesforceLogsReceiver) getFileContent(apiPath string) ([]byte, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s", strings.TrimRight(slr.client.GetLoc(), "/"), apiPath), nil)
	req.Header.Add("Content-Type", "application/json; charset=UTF-8")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", "Bearer "+slr.client.GetSid())

	var resp *http.Response
	err = retry.Do(
		func() error {
			resp, err = httpClient.Do(req)
			if err != nil {
				return err
			}

			if resp.StatusCode < 200 || resp.StatusCode > 299 {
				buf := new(bytes.Buffer)
				buf.ReadFrom(resp.Body)
				return fmt.Errorf("ERROR: statuscode: %d, body: %s", resp.StatusCode, buf.String())
			}

			return nil
		},
		retry.RetryIf(
			func(err error) bool {
				result, matchErr := regexp.MatchString("statuscode: 5[0-9]{2}", err.Error())
				if matchErr != nil {
					return false
				}
				if result {
					return true
				}

				return false
			}),
		retry.DelayType(retry.BackOffDelay),
		retry.Attempts(3),
	)
	if err != nil {
		return nil, err
	}

	var content []byte
	content, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return content, nil
}

func addEventLogToJsonData(eventLog map[string]interface{}, jsonData []byte) ([]byte, error) {
	var jsonMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &jsonMap); err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON data: %w", err)
	}

	jsonMap["LogFileContent"] = eventLog

	newJsonData, err := json.Marshal(jsonMap)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON data: %w", err)
	}

	return newJsonData, nil
}
