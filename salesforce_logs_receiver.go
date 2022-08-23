package salesforce_logs_receiver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/simpleforce/simpleforce"
)

const (
	defaultApiVersion = "55.0"
	eventLogFileSObjectName = "eventlogfile"
)

type SalesforceLogsReceiver struct {
	SObjectNames []string

	username      string
	password      string
	securityToken string
	client        *simpleforce.Client
}

func NewSalesLogsReceiver(url string, clientID string, apiVersion string, username string, password string, securityToken string, sObjectNames []string) (*SalesforceLogsReceiver, error) {
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

	if len(sObjectNames) == 0 {
		return nil, fmt.Errorf("sObjects must have a value")
	}

	if url == "" {
		url = simpleforce.DefaultURL
	}

	if apiVersion == "" {
		apiVersion = defaultApiVersion
	}

	client := simpleforce.NewClient(url, clientID, apiVersion)
	//sObjectsList := strings.Split(sObjects, ",")

	return &SalesforceLogsReceiver{
		SObjectNames:  sObjectNames,
		username:      username,
		password:      password,
		securityToken: securityToken,
		client:        client,
	}, nil
}

func (slr *SalesforceLogsReceiver) LoginSalesforce() error {
	if err := slr.client.LoginPassword(slr.username, slr.password, slr.securityToken); err != nil {
		return fmt.Errorf("error login Salesforce API: %w", err)
	}

	return nil
}

func (slr *SalesforceLogsReceiver) CollectSObject(sObjectName string) error {
	query := fmt.Sprintf("SELECT Id,CreatedDate FROM %s", sObjectName)
	result, err := slr.client.Query(query)
	if err != nil {
		return fmt.Errorf("error querying Salesforce API: %w", err)
	}

	for _, record := range result.Records {
		id := record.StringField("Id")
		data := record.Get(id)

		var jsonData []byte
		jsonData, err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("error marshaling data from Salesforce API: %w", err)
		}

		if strings.ToLower(sObjectName) == eventLogFileSObjectName {
			jsonData, err = slr.handleEventLogFileSObject(data, jsonData)
			if err != nil {
				return fmt.Errorf("error handling event log file sObject: %w", err)
			}
		}


	}

	return nil
}

func (slr *SalesforceLogsReceiver) handleEventLogFileSObject(data *simpleforce.SObject, jsonData []byte) ([]byte, error) {
	eventLog, err := slr.getEventLogFileContent(data)
	if err != nil {
		return nil, fmt.Errorf("error getting event log file content: %w", err)
	}

	jsonData, err = addEventLogToJsonData(eventLog, jsonData)
	if err != nil {
		return nil, fmt.Errorf("error adding event log to JSON data: %w", err)
	}

	return jsonData, nil
}

func (slr *SalesforceLogsReceiver) getEventLogFileContent(data *simpleforce.SObject) (map[string]interface{}, error){
	apiPath := data.StringField("LogFile")
	logFileContent, err := slr.getFileContent(apiPath)
	if err != nil {
		return nil, fmt.Errorf("error getting event log file content: %w", err)
	}

	reader := strings.NewReader(string(logFileContent))
	csvReader := csv.NewReader(reader)

	csvData, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV data: %w", err)
	}

	csvMap := make(map[string]interface{}, 0)
	for index, value := range csvData {
		if index == 0 {
			continue
		}

		key := csvData[0][index-1]
		csvMap[key] = value[index-1]
	}

	return csvMap, nil
}

func (slr *SalesforceLogsReceiver) getFileContent(apiPath string) ([]byte, error) {
	httpClient := &http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s", strings.TrimRight(slr.client.GetLoc(), "/"), apiPath), nil)
	req.Header.Add("Content-Type", "application/json; charset=UTF-8")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", "Bearer "+slr.client.GetSid())

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		return nil, fmt.Errorf("ERROR: statuscode: %d, body: %s", resp.StatusCode, buf.String())
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
	err := json.Unmarshal(jsonData, &jsonMap)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON data: %w", err)
	}

	jsonMap["LogFileContent"] = eventLog

	var newJsonData []byte
	newJsonData, err = json.Marshal(jsonMap)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON data: %w", err)
	}

	return newJsonData, nil
}
