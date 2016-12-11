# About this project

This project is a Message Queue written in Crystal, very similar to NSQ.

## Usage  



To run this project you will need three components.

* A Message Router, running with either debug set to true or false
	
	`crystal router.cr false`
	
* A Message Consumer, with a topic and channel set
	
	`crystal tools/consumer.cr my_topic my_channel`
	
* A Message Producer, with a topic and message
	
	`crystal tools/producer.cr my_topic "Hello World"`
	
* If you would like to run the benchmarks, feel free to use tools/meth

```
	./meth --consumer # Will benchmark received messags (used in conjuction with producer)
	./meth --producer # Will benchmark sent messages (used in conjunction with consumer)
	./meth --latency  # Will benchmark request latency
```
