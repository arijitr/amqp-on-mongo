
const Broker=require('./broker');
const MONGO_URL='mongodb://localhost:27017?authSource=admin';
let options={
    url:MONGO_URL,
    dbname: "broker",
    name: "StockMarket"
}
Broker.create(options).then(async (broker)=>{
    await broker.publish("BSE","Index gone up by 5 %");
    broker.destroy();
}).catch(e=>{
    console.log('broker creation failed', e)
});