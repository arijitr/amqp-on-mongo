//broker.js
const SIZE=1000000;
const MAX_QSIZE=1000
const {MongoClient}=require('mongodb')


class Broker{
    
    constructor(client,option){
        this.option=option;
        this.client=client;
        
        
    }
    /*
    * The factory function to create a Broker instance . The option takes following attributes.
    * url : connection string to mongodb instance
    * dbname: database name
    * name:  exchange name
    */

    static async create(option) {
        let client=null;
        try{
            client = await MongoClient.connect(option.url,{useUnifiedTopology: true });
            const db = client.db(option.dbname);
            option.qsize=option.qsize||MAX_QSIZE;
            //creating capped collection if it does not exist
            let exist=await db.listCollections({ name: option.name }).hasNext();
            if(!exist){
                let collection=await db.createCollection(option.name, {capped: true, size: SIZE,max:option.qsize})
                console.log(" Broker  got created with max queue size ",option.qsize);
                await collection.insertOne({ init: true ,_id: new Date().getTime()});
            
                
            }
            //creating the Broker instance
            let broker=new Broker(client,option);
            return broker;
        }catch(e){
            console.log('broker creation failed ',e)
            if(!!client){ 
                //close the connection if creation failed but connection exist
                client.close()
            }
            throw e
        }
        
    }

    /*
    * subscribe to a specific routingkey
    * suboption : declares the subscription options . i.e. routingkey , autoAck
    */
    async subscribe(subscriptionName, suboption,next){

       
        var filter = {routingkey:suboption.routingkey};
        //initializes autoAck default to true if not passed.
        suboption.autoAck=suboption.autoAck||true;
        if('function' !== typeof next) throw('Callback function not defined');

        let db=this.client.db(this.option.dbname)
    
        let collection=db.collection(this.option.name)  
        let deliveryCol=db.collection("delivery");
        let seekCursor=deliveryCol.find({subscriptionName:subscriptionName, exchange:this.option.name}).sort({msgid:-1}).limit(1);
        let lastdelivery=await seekCursor.next();
        if(lastdelivery!=null){
            //Pick up the id of the last message and add it as filter.
            console.log('lastdelivery ',lastdelivery.msgid)
            filter['_id']={ $gt: lastdelivery.msgid};
        }
        var cursorOptions = {
                    tailable: true,
                    awaitdata: true,
                    numberOfRetries: -1
        };
        const tailableCursor = collection.find(filter, cursorOptions);
        //create stream from tailable cursor
        var stream =tailableCursor.stream();
        console.log('queue is waiting for message ...')
        stream.on('data', async (data)=>{
            
            // build delivery record
            data.deliveryTag={};
            data.deliveryTag.msgid=data._id; 
            //build the id of the record using exchange name, subscription name and _id of the message.
            //This is to ensure that one message is delivered to one consumer of the subscription.
            data.deliveryTag._id=this.option.name+"#"+subscriptionName+"#"+data._id; 
            data.deliveryTag.exchange=this.option.name
            data.deliveryTag.subscriptionName=subscriptionName;
            data.deliveryTag.deliverytimestamp=new Date().getTime();
            data.deliveryTag.requeue=false;
            data.deliveryTag.acktimestamp=null;
           
            if(suboption.autoAck){
                    
                //set the auto-ack
                data.deliveryTag.acktimestamp=new Date().getTime();
                data.deliveryTag.state='acknowledged';
            }else{
                data.deliveryTag.state='delivered'
            }
            //check if the msg is already delivered for the same subscription.
           
            let delivered=await deliveryCol.find(
                {
                _id: data.deliveryTag._id
                }).hasNext();
            if(!delivered){
                //insert the delivery record.
                deliveryCol.insertOne(data.deliveryTag,(err,result)=>{
                    if(!!err){
                        console.log("already delivered to other worker")
                    }else if(result.result.ok==1){
                        //only in case of successful insertion
                        next(data);
                        
                    }
                });
            }
            
        });

    }
    /* 
    * publish a message i.e. insert a message to the capped collection.
    * routingkey : the routingkey of the message 
    * message : message payload. This could be string or any data type or any vald javascript object.
    */
    async publish(routingkey,message){
        let data={};
        data.routingkey=routingkey;
        data.message=message;
        data._id=new Date().getTime();
        data.processed=false;
        let db= this.client.db(this.option.dbname);
        let result=await db.collection(this.option.name).insertOne(data);
        if(result.result.ok==1){
            console.log('message published to exchange ',this.option.name," with routing  key ",routingkey );
        }
        return result;
    }
    /*
    * acknowledge a message delivery 
    */
   async ack(deliveryTag){
    let db=this.client.db(this.option.dbname);
    let collection=db.collection('delivery');

    let result=await collection.updateOne(
        {
            msgid: deliveryTag.msgid,
            exchange: this.option.name
        },
        {
            $set: {
                acktimestamp: new Date().getTime(),
                state: 'acknowledged'
            }
        }
    );
    if(result.result.ok==1){
        console.log('message acknowledged  ',deliveryTag.msgid,deliveryTag.subscriptionName)
    }
    
   }
   /*
    * reject a message delivery 
    */
   async nack(deliveryTag){
    let db=this.client.db(this.option.dbname);
    let collection=db.collection('delivery')

    let result=await collection.updateOne(
        { 
            msgid: deliveryTag.msgid, 
            exchange: this.option.name 
        },
        {
            $set: {
                acktimestamp: new Date().getTime(),
                state: 'rejected'
            }
        }
    );
    if(result.result.ok==1){
        console.log('message rejected  ',deliveryTag.msgid,deliveryTag.subscriptionName)
        if(deliveryTag.requeue){
            let exchange=db.collection(this.option.name);
            let msg=await exchange.find({_id:deliveryTag.msgid}).next();
            msg._id=new Date().getTime();
            exchange.insertOne(msg);
        }
    }
    
   }

//     /*
//     * acknowledge a message delivery 
//     */
//    async ack(deliveryTag){
//     let db=this.client.db(this.option.dbname);
//     let collection=await db.collection(deliveryTag.subscriptionName)

//     let result=await collection.updateOne(
//         { _id:  deliveryTag.id },
//         { $set: { processed:true  } },
//     );
//     if(result.result.ok==1){
//         console.log('message acknowledged  ',deliveryTag.id,deliveryTag.subscriptionName)
//     }
    
//    }
   async destroy(){
       if(this.client)
       this.client.close();
   }

}
module.exports=Broker
