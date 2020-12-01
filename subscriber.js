// A sample subscriber to show how to use the broker to listen messages from publisher.
const Broker=require('./broker');
const MONGO_URL='mongodb://localhost:27017?authSource=admin';
let options={
    url:MONGO_URL,
    dbname: "broker",
    name: "StockMarket"
}
//create a broker passing the option parameter which have the url for the Mongo .
Broker.create(options).then(async (broker)=>{
    broker.subscribe('bse',{routingkey:"BSE",autoAck:false},(data)=>{
        let datetime=new Date();
        console.log(datetime, " data received from Stockmarket for BSE----->",data.message)
        broker.ack(data.deliveryTag)
    })
   
}).catch(e=>{
    console.log('broker creation failed', e)
});
