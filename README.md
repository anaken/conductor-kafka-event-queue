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
      conductor.event-queues.kafka.group-id=test-conductor
      conductor.event-queues.kafka.offset=earliest
      conductor.event-queues.kafka.auto-commit=false

* Build or run `conductor-server` project
