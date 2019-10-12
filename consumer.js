require('dotenv').config();
var async = require('async');
var fs = require('fs');
var kafka = require('kafka-node');

var protoPath = process.env.JS_PROTO_PATH;
var topicName = process.env.TOPIC_NAME;
var messageUniqueIdentifier = process.env.MESSAGE_UNIQUE_IDENTIFIER;
var eachMessageIdentifier = process.env.EACH_MESSAGE_IDENTIFIER;

var log = require('./' + protoPath);
var protoObjectKeys = Object.keys(log);
var keyClass = log[protoObjectKeys[0]];
var msgClass = log[protoObjectKeys[1]];

// File Output Setup

var folderName = topicName;
var now = new Date();
var outputDir = 'output/'+ folderName + '/' + getTimestampForFilename(now);

// Kafka Topics Setup

var topics = [topicName];

// -------------

// Kafka Consumer Setup
var options = {
    kafkaHost: process.env.KAFKA_HOST,
    groupId: process.env.CONSUMER_GROUP_ID,
    sessionTimeout: process.env.SESSION_TIMEOUT,
    fromOffset: 'earliest',
    commitOffsetsOnFirstJoin: true,
    outOfRangeOffset: 'earliest',
    encoding: 'buffer'
};

var consumerGroup = new kafka.ConsumerGroup(options, topics);

// -------------

console.log("Consuming " + topicName + " from " + options['kafkaHost'])

consumerGroup.on('message', function(message) {
    deserializedKey = keyClass.deserializeBinary(message.key).toObject();
    prettyPrintedKey = JSON.stringify(prettyPrint(deserializedKey), null, 2);

    deserializedMessage = msgClass.deserializeBinary(message.value).toObject();
    prettyPrintedMessage = JSON.stringify(prettyPrint(deserializedMessage), null, 2);

    toBePrinted = 'key: \n' + prettyPrintedKey
    toBePrinted = toBePrinted + '\n\n--------------------------------\n'
    toBePrinted = toBePrinted + 'message: \n' + prettyPrintedMessage
    toBePrinted = toBePrinted + '\n\n--------------------------------';

    fileDir = outputDir + '/' + deserializedMessage[messageUniqueIdentifier];
    fs.mkdirSync(fileDir, { recursive: true })

    eventTimestamp = convertProtoTimestampToTime(deserializedMessage[eachMessageIdentifier]);
    filename = getTimestampForFilename(eventTimestamp);
    filePath = fileDir + '/' + filename + '.json';

    fd = fs.openSync(filePath, 'a+');
    fs.writeSync(fd, toBePrinted);
    fs.closeSync(fd);
});

process.once('SIGINT', function() {
    async.each([consumerGroup], function(consumer, callback) {
        consumer.close(true, callback);
    });
});

function prettyPrint(something, output = null) {
    if (something) {
        if (Array.isArray(something)) {
            output = something.map(function(item, index) {
                return prettyPrint(item);
            });
        } else if (typeof something === 'object') {
            if (something['seconds'] || something['nanos']) {
                output = convertProtoTimestampToTime(something).toUTCString();
            } else {
                output = {}
                for (var k in something) {
                    output[k] = prettyPrint(something[k]);
                }
            }
        } else {
            output = something;
        }
    } else {
        output = something;
    }
    return output;
}

function getTimestampForFilename(time) {
    return time.getFullYear() + '_' + time.getMonth() + '_' + time.getDate() + '_' + time.getHours() + '_' + time.getMinutes() + '_' + time.getSeconds()
}

function convertProtoTimestampToTime(protoTimestamp) {
    var seconds = protoTimestamp['seconds'];
    var nanos = protoTimestamp['nanos'];
    var result = new Date();
    result.setTime(seconds * 1000 + nanos / 1000000);
    return result;
}
