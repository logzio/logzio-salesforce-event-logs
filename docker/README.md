# Logz.io Salesforce Collector

Collect sObjects data from Salesforce and ship them to Logz.io.

## Pull Docker Image

Download the logzio-salesforce-collector image:

```shell
docker pull logzio/logzio-salesforce-collector
```

## Run The Docker Container

```shell
 docker run --name logzio-salesforce-collector \
 --env SALESFORCE_URL="<<SALESFORCE_URL>>" \
 --env CLIENT_ID="<<CLIENT_ID>>" \
 --env USERNAME="<<USERNAME>>" \
 --env PASSWORD="<<PASSWORD>>" \
 --env SECURITY_TOKEN="<<SECURITY_TOKEN>>" \
 --env SOBJECT_TYPES="<<SOBJECT_TYPES>>" \
 --env FROM_TIMESTAMP="<<FROM_TIMESTAMP>>" \
 --env INTERVAL="<<INTERVAL>>" \
 --env LOGZIO_LISTENER_URL="<<LOGZIO_LISTENER_URL>>" \
 --env LOGZIO_TOKEN="<<LOGZIO_TOKEN>>" \
logzio/logzio-salesforce-collector
```

### Environment Variables

| Name | Description | Required? | Default |
| --- | --- | ---| ---|
| CLIENT_ID | Salesforce App Client ID. | Yes | - |
| USERNAME | Salesforce account username (your email) | Yes | - |
| PASSWORD | Salesforce account password | Yes | - |
| SECURITY_TOKEN | Salesforce account security token | Yes | - |
| SOBJECT_TYPES | List of sObject types to collect. Each type must be separated by comma, for example: "TYPE1,TYPE2,TYPE3". | Yes | - |
| FROM_TIMESTAMP | Timestamp from when to collect data. Must be in the following format: 2006-01-02T15:04:05.000Z . | Yes | - |
| LOGZIO_TOKEN | Logz.io logs token. | Yes | - |
| SALESFORCE_URL | Salesforce URL. | No | https://login.salesforce.com |
| INTERVAL | The time interval to collect Salesforce data (in seconds). | No | 5 (seconds) |
| LOGZIO_LISTENER_URL | Logz.io listener logs URL. | No | https://listener.logz.io:8071

## Searching in Logz.io

All logs will have the type `salesforce`.
