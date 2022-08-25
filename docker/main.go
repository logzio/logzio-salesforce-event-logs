package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/logzio/logzio-go"
	receiver "github.com/logzio/salesforce-logs-receiver"
)

const (
	salesforceURLEnvName     = "SALESFORCE_URL"
	clientIDEnvName          = "CLIENT_ID"
	apiVersionEnvName        = "API_VERSION"
	usernameEnvName          = "USERNAME"
	passwordEnvName          = "PASSWORD"
	securityTokenEnvName     = "SECURITY_TOKEN"
	sObjectTypesEnvName      = "SOBJECT_TYPES"
	fromTimestampEnvName     = "FROM_TIMESTAMP"
	intervalEnvName          = "INTERVAL"
	logzioListenerURLEnvName = "LOGZIO_LISTENER_URL"
	logzioTokenEnvName       = "LOGZIO_TOKEN"

	defaultLogzioListenerURL = "https://listener.logz.io:8071"
)

var (
	errorLogger = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime)
)

type salesforceCollector struct {
	receiver *receiver.SalesforceLogsReceiver
	shipper  *logzio.LogzioSender
	interval int
}

func newSalesforceCollector() (*salesforceCollector, error) {
	rec, err := createSalesforceReceiver()
	if err != nil {
		return nil, fmt.Errorf("error creating Salesforce receiver: %w", err)
	}

	shipper, err := createLogzioSender()
	if err != nil {
		return nil, fmt.Errorf("error creating Logz.io sender: %w", err)
	}

	intervalStr := os.Getenv(intervalEnvName)
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		return nil, fmt.Errorf("interval must be a positive number")
	}

	if interval <= 0 {
		return nil, fmt.Errorf("interval must be a positive number")
	}

	return &salesforceCollector{
		receiver: rec,
		shipper:  shipper,
		interval: interval,
	}, nil
}

func createSalesforceReceiver() (*receiver.SalesforceLogsReceiver, error) {
	sObjectTypesStr := os.Getenv(sObjectTypesEnvName)
	sObjectTypes := strings.Split(strings.TrimSpace(sObjectTypesStr), ",")
	latestTimestamp := os.Getenv(fromTimestampEnvName)

	var sObjects []*receiver.SObjectToCollect
	for _, sObjectType := range sObjectTypes {
		sObjects = append(sObjects, &receiver.SObjectToCollect{
			SObjectType:     sObjectType,
			LatestTimestamp: latestTimestamp,
		})
	}

	rec, err := receiver.NewSalesforceLogsReceiver(
		os.Getenv(salesforceURLEnvName),
		os.Getenv(clientIDEnvName),
		os.Getenv(apiVersionEnvName),
		os.Getenv(usernameEnvName),
		os.Getenv(passwordEnvName),
		os.Getenv(securityTokenEnvName),
		sObjects)
	if err != nil {
		return nil, fmt.Errorf("error creating Salesforce logs receiver object: %w", err)
	}

	if err = rec.LoginSalesforce(); err != nil {
		return nil, err
	}

	return rec, nil
}

func createLogzioSender() (*logzio.LogzioSender, error) {
	logzioListenerURL := os.Getenv(logzioListenerURLEnvName)
	if logzioListenerURL == "" {
		logzioListenerURL = defaultLogzioListenerURL
	}

	logzioToken := os.Getenv(logzioTokenEnvName)
	if logzioToken == "" {
		return nil, fmt.Errorf("Logz.io token must have a value")
	}

	shipper, err := logzio.New(
		fmt.Sprintf("%s&type=salesforce", logzioToken),
		logzio.SetDebug(os.Stderr),
		logzio.SetUrl(logzioListenerURL),
		logzio.SetDrainDuration(time.Second*5),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating Logz.io sender object: %w", err)
	}

	return shipper, nil
}

func (sfc *salesforceCollector) collect() {
	var waitGroup sync.WaitGroup

	for _, sObject := range sfc.receiver.SObjects {
		waitGroup.Add(1)

		go func(sObject *receiver.SObjectToCollect) {
			defer waitGroup.Done()

			records, err := sfc.receiver.GetSObjectRecords(sObject)
			if err != nil {
				errorLogger.Println("error getting sObject ", sObject.SObjectType, " records: ", err)
				return
			}

			for _, record := range records {
				data, createdDate, err := sfc.receiver.CollectSObjectRecord(&record)
				if err != nil {
					errorLogger.Println("error collecting sObject ", sObject.SObjectType, " record ID ", record.ID(), ": ", err)
					return
				}

				if strings.ToLower(sObject.SObjectType) == receiver.EventLogFileSObjectName {
					enrichedData, err := sfc.receiver.EnrichEventLogFileSObjectData(&record, data)
					if err != nil {
						errorLogger.Println("error enriching EventLogFile sObject ", " record ID ", record.ID(), ": ", err)
						return
					}

					for _, data = range enrichedData {
						if !sfc.sendDataToLogzio(data, sObject.SObjectType, record.ID()) {
							return
						}
					}
				} else {
					if !sfc.sendDataToLogzio(data, sObject.SObjectType, record.ID()) {
						return
					}
				}

				sObject.LatestTimestamp = *createdDate
			}
		}(sObject)

		waitGroup.Wait()
	}

	sfc.shipper.Stop()
}

func (sfc *salesforceCollector) sendDataToLogzio(data []byte, sObjectName string, sObjectRecordID string) bool {
	if err := sfc.shipper.Send(data); err != nil {
		errorLogger.Println("error sending sObject ", sObjectName, " record ID ", sObjectRecordID, " to Logz.io: ", err)
		return false
	}

	return true
}

func main() {
	collector, err := newSalesforceCollector()
	if err != nil {
		panic(err)
	}

	for {
		collector.collect()
		time.Sleep(time.Duration(collector.interval) * time.Second)

		if err = collector.receiver.LoginSalesforce(); err != nil {
			panic(fmt.Errorf("error creating new access token: %w", err))
		}
	}
}
