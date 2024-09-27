import { Server } from "socket.io";
import Redis from 'ioredis'
import prismaClient from "./prisma";
import { produceMessage } from "./kafka";


const pub = new Redis({
    host: 'caching-294e8e6b-sajidazhar222-64c3.d.aivencloud.com',
    port: 16823,
    username: 'default',
    password: 'AVNS_XqxfPCy0QXK16D_gm7J'
});
const sub=new Redis({
    host: 'caching-294e8e6b-sajidazhar222-64c3.d.aivencloud.com',
    port: 16823,
    username: 'default',
    password: 'AVNS_XqxfPCy0QXK16D_gm7J'
})

class SocketService{
    private _io: Server;

    constructor(){
        console.log("Init Socket Service ...");
        this._io=new Server({
            cors:{
                allowedHeaders : ["*"],
                origin:"*",
            },
        });
        sub.subscribe("MESSAGES");
    }

   

    get io(){
        return this._io;
    }

    public initListners(){
        const io= this.io;
        console.log('Init socket listener..');
        io.on("connect", (socket) =>{
            console.log(`New Socket Connected`,socket.id);
            socket.on('event:message',async ({message}:{message:string}) =>{
                console.log('New Message Rec', message);
                //publis this message to redis
                await pub.publish('MESSAGES',JSON.stringify({message}))
            });
        });
        sub.on('message',async (channel, message) => {
            if(channel === 'MESSAGES'){
                console.log("new message from redis", message);
                io.emit("message",message);
               await produceMessage(message);
               console.log("Message Produced to Kafka Broker");
            }
        });
    }
}

export default SocketService;