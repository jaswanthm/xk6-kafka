/*

This is a k6 test script that imports the xk6-kafka and
tests Kafka by sending 200 Avro messages per iteration
without any associated key.
*/

import {Connection, Reader, SCHEMA_TYPE_AVRO, SCHEMA_TYPE_JSON, SchemaRegistry, Writer,} from "k6/x/kafka"; // import kafka extension

const brokers = ["localhost:9092"];
const topic = "products";

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    autoCreateTopic: true,
});
const reader = new Reader({
    brokers: brokers,
    topic: topic,
});
const connection = new Connection({
    address: brokers[0],
});
const schemaRegistry = new SchemaRegistry();

if (__VU == 0) {
    connection.createTopic({topic: topic});
}

const valueSchema = JSON.stringify({
    type: "record",
    name: "Value",
    namespace: "dev.mostafa.xk6.kafka",
    fields: [
        {
            name: "name",
            type: "string",
        },
        {
            name: "id",
            type: "string",
        },
        {
            name: "type",
            type: "string",
        },
        {
            name: "version",
            type: "string",
        },
        {
            name: "event",
            type: "string",
        },
        {
            name: "price",
            type: "float",
        },

    ],
});

// declare all characters
const characters ='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

function generateString(length) {
    let result = ' ';
    const charactersLength = characters.length;
    for ( let i = 0; i < length; i++ ) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }

    return result;
}

function create_UUID(){
    var dt = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = (dt + Math.random()*16)%16 | 0;
        dt = Math.floor(dt/16);
        return (c=='x' ? r :(r&0x3|0x8)).toString(16);
    });
    return uuid;
}

function getRandomFloat(min, max, decimals) {
    const str = (Math.random() * (max - min) + min).toFixed(decimals);

    return parseFloat(str);
}

export default function () {
    for (let index = 0; index < 100; index++) {
        let messages = [
            {
                value: schemaRegistry.serialize({
                    data: {
                        name: generateString(10),
                        id:  create_UUID(),
                        type: generateString(5),
                        version: "v1",
                        price: getRandomFloat(1, 200, 2),
                        event: "UPDATED"
                    },
                    // schema: {schema: valueSchema},
                    schemaType: SCHEMA_TYPE_JSON,
                }),
            },
        ];

        writer.produce({messages: messages});
    }

    // // Read 10 messages only
    // let messages = reader.consume({limit: 10});
    // check(messages, {
    //     "10 messages returned": (msgs) => msgs.length == 10,
    //     "value is correct": (msgs) =>
    //         schemaRegistry.deserialize({
    //             data: msgs[0].value,
    //             schema: {schema: valueSchema},
    //             schemaType: SCHEMA_TYPE_AVRO,
    //         }).name == "xk6-kafka",
    // });
}

export function teardown(data) {
    if (__VU == 0) {
        // Delete the topic
        // connection.deleteTopic(topic);
    }
    writer.close();
    reader.close();
    connection.close();
}
