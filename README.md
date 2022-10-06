# How to use

* Clone conductor repository

      $ git clone https://github.com/Netflix/conductor.git

* Copy contents of `conductor-kafka-event-queue` to new folder `kafka-event-queue` created in the root of netflix
  conductor repository
* Add project to `settings.gradle` like this:

      include 'kafka-event-queue'

* Add project dependency to `server/build.gradle` like this:

      implementation project(':conductor-kafka-event-queue')

* Add server properties:

      conductor.event-queues.kafka.enabled=true
      conductor.event-queues.kafka.bootstrap-servers=localtest:9092
      conductor.event-queues.kafka.default-group-id=test-conductor
      conductor.event-queues.kafka.offset=earliest
      conductor.event-queues.kafka.client-id=conductor

* Build or run `conductor-server` project

---

# Usage examples

* Producer

To produce messages to kafka, you need to create an EVENT type task:

    {
        "name": "sendToKafka",
        "taskReferenceName": "sendToKafka",
        "inputParameters": {
            "key": "123",
            "headers": {
                "myHeader1": "testHeaderValue1",
                "myHeader2": "testHeaderValue2"
            },
            "payload": {
                "myBusinessData": "${workflow.input.myInputBusinessData}"
            }
        },
        "sink": "kafka:topic1",
        "type": "EVENT"
    }

Input parameters:

| Name | Description | Type | Required |
|---------|-------------|-------|----------|
| key | Key of message | String | false    |
| headers | Map of message headers | Map<String, String> | false    |
| payload | Payload of message | JSON | true |

---

* Consumer

To consume messages from kafka, you need to create an event handler:

    {
        "name": "complete_task_demo_handler",
        "event": "kafka:id=1bd18b58-98b8-4964-bd47-7c0b618df31a;name=complete_task_demo_handler;topics=topic1,topic2;dlq=demo-dlq1",
        "actions": [
            {
                "action": "complete_task",
                "complete_task": {
                    "workflowId": "${eventHeaders.workflowInstanceId}",
                    "taskRefName": "${eventHeaders.taskRef}",
                    "output": {
                        "messageKey": "${eventKey}",
                        "messageHeaders": "${eventHeaders}",
                        "messagePayload": "${eventPayload}"
                    }
                }
            }
        ],
        "active": true
    }

Queue URI parameters:

| Name  | Description                                                                                                                 | Type          | Required                       |
|-------|-----------------------------------------------------------------------------------------------------------------------------|---------------|--------------------------------|
| id | Unique identifier. Should be generated each time on handler creation/update/removal API call                                | UUID          | true                           |
| name | Handler name. Reuse the same value as handler name                                                                          | String        | true                           |
| topics | Comma separated topics list to consume                                                                                      | List\<String> | true                           |
| group | Consumer group id. If not set, then the default group id from properties will be used                                       | String        | false                          |
| dlq | DLQ(dead letter queue) topic name. Messages which were not processed by conductor for some reason will be sent to this topic | String        | false                          |
| filteringHeader | Header name which will be used to filter out unnecessary message                                                            | String        | false                          |
| filteringValue | Value of `filteringHeader`                                                                                                  | String        | true if filteringHeader is set |

Event parameters:

| Name         | Description |
|--------------|-------------|
| eventKey     | Message key |
| eventPayload | Message payload |
| eventHeaders | Message headers |
