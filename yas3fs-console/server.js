var AWS = require('aws-sdk');
var util = require('util');
var uuid = require('node-uuid'); // For unique queue name
var express = require('express'); // For static files

var sns;
var sqs;
var io;
var queues;

var sqsWaitTimeSeconds;
var exiting;

function hadError(err) {
    util.log(util.inspect(err));
}

function init() {
    process.on('exit', doExit);
    process.on('SIGINT', doExit);
    process.on('SIGTERM', doExit);

    sns = new AWS.SNS().client;
    sqs = new AWS.SQS().client;

    port = process.env.PORT || 3000;
    sqsWaitTimeSeconds = 20;

    app = express();
    app.use(express.favicon());
    app.use(express.logger('dev'));
    app.use(express.static(__dirname + '/public'));
    server = app.listen(port);

    io = require('socket.io').listen(server, {'log level': 1});

    queues = {};
    clients = {};
    
    setEventHandlers();
};

var setEventHandlers = function() {
    io.sockets.on('connection', onSocketConnection);
}

function onSocketConnection(client) {
    util.log('New user has connected: '+client.id);
    if (!clients[client.id]) {
	clients[client.id] = {}
	clients[client.id].socket = client;
    }
    client.on('disconnect', onClientDisconnect);
    client.on('update', onUpdate);
    client.on('list', onList);
    client.on('log', onLog);
}

function onLog(data) {
    util.log('Log: '+data);
}

function onClientDisconnect() {
    util.log('User has disconnected: '+this.id);
    delete clients[this.id];
}

function onUpdate(topicArn) {
    util.log('Update for Topic ARN: '+topicArn+' from: '+this.id);
    clients[this.id].topicArn = topicArn;
    if (topicArn) {
	if (!queues[topicArn]) {
	    initTopic(topicArn);
	} else {
	    if (queues[topicArn].ready)
		publishPingToTopic(topicArn);
	}
    }
}

function onList() {
    util.log('Get Topic List for: '+this.id);
    getSomeTopics();
}

function getSomeTopics(nextToken) {
    var topics = [];
    var params;
    if (nextToken) {
	params = {
	'NextToken': nextToken
	}
    } else {
	params = {}
    }
    sns.listTopics(params, function (err, data) {
	if (err) return hadError(err);
	data.Topics.forEach(function(item) {
	    topics.push(item.TopicArn);
	});
	util.log('topics: '+topics);
	io.sockets.emit('topic', topics);
	if (data.NextToken) {
	    getSomeTopics(data.NextToken);
	}
    });
}

function initTopic(topicArn) {
    util.log('Init Topic ARN: '+topicArn);
    queues[topicArn] = {}
    queues[topicArn].queueName = 'yas3fs-console-' + uuid.v4();
    createQueueForTopic(topicArn);    
}

function createQueueForTopic(topicArn) {
    util.log('Create queue: ' + queues[topicArn].queueName);
    sqs.createQueue({
	'QueueName': queues[topicArn].queueName
    }, function (err, data) {
	if (err) return hadError(err);
	queues[topicArn].queueUrl = data.QueueUrl
	util.log('Queue URL: ' + queues[topicArn].queueUrl);
	getQueueArnForTopic(topicArn)
    });
}

function getQueueArnForTopic(topicArn) {
    sqs.getQueueAttributes({
	QueueUrl: queues[topicArn].queueUrl,
	AttributeNames: ['QueueArn']
    }, function (err, data) {
	if (err) return hadError(err);
	queues[topicArn].queueArn = data.Attributes['QueueArn'];
	util.log('Queue ARN: ' + queues[topicArn].queueArn);
	subscribeQueueToTopic(topicArn)
    });
}

function subscribeQueueToTopic(topicArn) {
    util.log('subscribing ' + queues[topicArn].queueArn + ' to ' + topicArn);
    sns.subscribe({
	'TopicArn': topicArn,
	'Protocol': 'sqs',
	'Endpoint': queues[topicArn].queueArn
    }, function (err, data) {
	if (err) return hadError(err);
	queues[topicArn].subscriptionArn = data.SubscriptionArn
	util.log('subscribed to topic');
	addPermissionToWrite(topicArn)
    });
}

function addPermissionToWrite(topicArn) {
    util.log('set queue permission');
    var attributes = {
	'Version': '2008-10-17',
	'Id': queues[topicArn].queueArn + '/SQSDefaultPolicy',
	'Statement': [{
            'Sid': 'Sid' + new Date().getTime(),
            'Effect': 'Allow',
            'Principal': {
                'AWS': '*'
            },
            'Action': 'SQS:SendMessage',
            'Resource': queues[topicArn].queueArn,
            'Condition': {
                'ArnEquals': {
                    'aws:SourceArn': topicArn
                }
            }
        }]
    };
    sqs.setQueueAttributes({
	QueueUrl: queues[topicArn].queueUrl,
	Attributes: {
            'Policy': JSON.stringify(attributes)
	}
    }, function (err, data) {
	queues[topicArn].ready = true;
	if (err) return hadError(err);
	util.log('starting queue polling interval');
	publishPingToTopic(topicArn);
	getMessagesFromQueueForTopic(topicArn);
    });
}

function publishPingToTopic(topicArn) {
    util.log('publishing ping on topic: '+topicArn);
    var message = JSON.stringify( [ 'all', 'ping' ] );
    sns.publish({
	TopicArn: topicArn,
	Message: message
    }, function (err, data) {
	if (err) return hadError(err);
    });
}

function getMessagesFromQueueForTopic(topicArn) {
    util.log('getting messages from queue');
    if (!queues[topicArn]) return;
    sqs.receiveMessage({
	QueueUrl: queues[topicArn].queueUrl,
	MaxNumberOfMessages: 10, // 10 is the maximum according to API specifications
	WaitTimeSeconds: sqsWaitTimeSeconds
    }, function (err, data) {
	if (err) return hadError(err);
	if (data.Messages) { 
	    util.log('got messages from queue');
	    data.Messages.forEach(function(message) {
		var body = JSON.parse(message.Body);
		var receiptHandle = message.ReceiptHandle;
		var status = JSON.parse(body.Message);
		util.log('status: '+status);
		for (var key in clients) {
		    var client = clients[key];
		    if (client.topicArn == topicArn) {
			client.socket.emit('add', status);
		    }
		}
		deleteMessageFromQueueForTopic(topicArn, receiptHandle);
	    });
	} else {
	    util.log('no messages from queue');
	}
	setTimeout(getMessagesFromQueueForTopic, 50, topicArn); // 50ms should be ok between calls
    });
}

function deleteMessageFromQueueForTopic(topicArn, receiptHandle) {
    util.log('deleting message');
    sqs.deleteMessage({
	QueueUrl: queues[topicArn].queueUrl,
	ReceiptHandle: receiptHandle
    }, function (err, data) {
	if (err) return hadError(err);
	util.log('message deleted: '+data);
    });
}

function deleteQueueForTopic(topicArn) {
    sns.unsubscribe({
	SubscriptionArn: queues[topicArn].subscriptionArn
    }, function (err, data) {
	if (err) return hadError(err);
	util.log('queue unsubscribed');
	sqs.deleteQueue({
	    QueueUrl: queues[topicArn].queueUrl
	}, function (err, data) {
	    if (err) return hadError(err);
	    util.log('queue deleted');
	    delete queues[topicArn];
	});
    });
}

function doExit() {
    if (exiting) return;
    exiting = true;
    util.log('Removing queues before exit...');
    for (var topicArn in queues) {
	deleteQueueForTopic(topicArn);
    }
    setInterval( function() {
	var ready = true;
	for (var t in queues) {
	    ready = false;
	}
	if (ready) {
	    util.log('Exiting...');
	    process.exit();
	}
    }, 100); // A check evey 100ms should be ok
}

init();
