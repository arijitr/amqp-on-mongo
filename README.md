# amqp-on-mongo

 This project provides a way to build an amqp protocol based messaging service backed by mongo . The nodejs based client library utilizes mongo db's tailable cursor feature to create a messaging service. 
 
 The messaging features supported by the library are the following:
 
- A direct exchange protocol: The library provides a routing key based direct exchnage protocol of amqp.
- message acknowledgement: Subscriber has option to have auto acknowkledgement or explicit acknowledgement. The messages are ensured to have at-most-once delivery to a single customer. 
- Multiple Worker Support: Messages are ensured to get delivered only to one worker of specific group of consumers using the same subscription name.
- Message requeue: Messages are auto requeued if it fails to get delivered successfully.
- Message durability: All messages are durable messages and are never lost.

 The motivation for the project can be found [here](https://dzone.com/articles/using-mongodb-capped-collection-as-messaging-frame)
 
 # Setting it up 
 
 - Download this project in your local 
 - Import the broker.js in your target source file and start using it . 
 - Sample publisher and subscriber are given for references.
 
The broker.js provides a Broker class which exposes mainly following 3 function.
## Create
The caller of the module must create a broker instance by calling the static function create defined in the Broker class. The create method takes an object argument, option. Three attributes of the argument is mandatory .i.e. url, dbname, and name. The create method initializes the db connection. and creates the capped collection with the same name as the exchange name and with the maximum size defaulted to 1000. The caller can override this default size by providing a qsize attribute in the option passed to create the broker.

## Publish
The publish method takes two mandatory arguments. routingkey, and message. The messages are published using the routingkey. Internally, it only inserts a record to the created capped collection. The message and routingkey is grouped together inside a wrapper javascript object to define the payload of the inserted record. Thus it leaves an option to filter out the messages based on the routingkey during consumption.
### Sample reference for a publisher


## Subscribe
The subscribe method takes two mandatory arguments, routingkey and a callback function. It sets up a filter using the routingkey and then creates a tailable cursor using the filter ( line #71). At the end, it establishes a stream on the tailable cursor to create the continuous flow of data as and when inserted in the collection.

### Sample reference for a subscriber

