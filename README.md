# How to use
* Clone conductor repository

      $ git clone https://github.com/Netflix/conductor.git

* Copy contents of `conductor-kafka-event-queue` to new folder `kafka-event-queue` created in the root of netflix conductor repository
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
            "headers": {
                "myHeader1": "testHeaderValue1",
                "myHeader2": "testHeaderValue2"
            },
            "value": {
                "myBusinessData": "${workflow.input.myInputBusinessData}"
            },
            "key": "123"
        },
        "sink": "kafka:topic1",
        "type": "EVENT"
    }
  Input parameters:
  
| Name  | Description | Required |
|-------|-------------|----------|
| value | Payload of message | true |
| headers | Map of message headers | false |
| key | Key of message | false |

---

  * Consumer

To consume messages from kafka, you need to create an event handler:

    {
        "name": "complete_task_demo_handler",
        "event": "kafka:topics=topic1,topic2;dlq=demo-dlq1;name=complete_task_demo_handler;id=1bd18b58-98b8-4964-bd47-7c0b618df31a",
        "actions": [
            {
                "action": "complete_task",
                "complete_task": {
                    "workflowId": "${eventHeaders.workflowInstanceId}",
                    "taskRefName": "${eventHeaders.taskRef}",
                    "output": {
                        "messageData": "${eventData}",
                        "messageHeaders": "${eventHeaders}",
                        "messageId": "${eventId}"
                    }
                }
            }
        ],
        "active": true
    }
  Queue URI parameters:

| Name  | Description | Required |
|-------|-------------|----------|
| topics | Comma separated topics list to consume | true |
| group | Consumers group id. If not set, then the default group id from properties used | false |
| filteringHeader | Header name to get value for filtering consumed messages | false |
| filteringValue | Value for matching to value of header `filteringHeader`  | false |
| dlq | Topic to send failed messages. Failed messages is defined by conductor internal mechanism | false |
| name | Handler name | true |
| id | Unique UUID. Must regenerated when the handler is changed | true |

  Event parameters:

| Name  | Description |
|-------|-------------|
| eventData | Message payload |
| eventHeaders | Message headers |
| eventKey | Message key |