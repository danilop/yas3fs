var socket;
var nodes;
var timer;

var secondsForForget;
var secondsForOld;

function init() {
    console.log('init begin...');
    socket = io.connect();
    setEventHandlers();
    nodes = {};
    $('#topicArnList').val('-').trigger('change');
    $('#topicArnList').change(function() {
	if ($('#topicArnList').val() == '-') {
	    $('#topicArn').removeAttr("disabled");
	    $('#topicArn').focus();
	} else {
	    $('#topicArn').attr("disabled", true);
	}
    });
    $('#timer-interval').val(10).trigger('change');
    $('#timer-interval').change(function() {
	if (timer) {
	    var interval = $('#timer-interval').val();
	    console.log('interval: '+ interval);
	    clearInterval(timer);
	    timer = setInterval(update, interval * 1000);
	    update(); // For the first one
	}
    });
    $('#old-elapsed').change(function() {
	if (timer) {
	    secondsForOld = $('#old-elapsed').val();
	    console.log('secondsForOld: '+ secondsForOld);
	    updateTable();
	}
    });
    $('#old-elapsed').val(15).trigger('change');
    $('#forget-elapsed').change(function() {
	if (timer) {
	    secondsForForget = $('#forget-elapsed').val();
	    console.log('secondsForForget: '+ secondsForForget);
	    updateTable();
	}
    });
    $('#forget-elapsed').val(60).trigger('change');
    $('#start-button').click(function() {
	$('#start-button').attr("disabled", true);
	$('#topicArnList').attr("disabled", true);
	$('#topicArn').attr("disabled", true);
	$('#stop-button').removeAttr("disabled");
	topicArn = $('#topicArnList').val();
	if (topicArn == '-') {
	    topicArn = $('#topicArn').val();
	}
	nodes = {};
	updateTable();
	if (!timer) {
	    var interval = $('#timer-interval').val();
	    console.log('interval: '+ interval);
	    timer = setInterval(update, interval * 1000);
	    update(); // For the first one
	}
    });
    $('#stop-button').attr("disabled", true);
    $('#stop-button').click(function() {
	$('#stop-button').attr("disabled", true);
	$('#start-button').removeAttr("disabled");
	$('#topicArnList').removeAttr("disabled");
	if ($('#topicArnList').val() == '-') {
	    $('#topicArn').removeAttr("disabled");
	}
	if (timer) {
	    clearInterval(timer);
	    timer = null;
	}
    });
    $("#topicArn").keyup(function(event){
	if(event.keyCode == 13){
            $("#start-button").click();
	}
    });
    $('#topicArn').focus();

    socket.emit('list');
    
    console.log('init done!');
}

function start() {
}

function setEventHandlers() {
    socket.on('add', onAdd);
    socket.on('topic', onTopic);
}

function update() {
    console.log('update!');
    socket.emit('update', topicArn);
    updateTable();
}

function onAdd(data) {
    console.log('add');
    if (data[1] == 'status') {
	var n = {};
	n.hostname = data[2];
	n.num_entries = data[3];
	n.mem_size = data[4];
	n.disk_size = data[5];
	n.dq = data[6];
	n.pq = data[7];
	n.updateTime = new Date();
	nodes[data[0]] = n;
	updateTable();
    }
}

function onTopic(data) {
    console.log('topic');
    data.forEach(function(topic) {
	$('#topicArnList').append($('<option>', { value: topic }).text(topic));
    });
}

function getKeys(obj) {
    var r = []
    for (var k in obj) {
        if (!obj.hasOwnProperty(k)) 
            continue
	console.log('k='+k);
        r.push(k)
    }
    return r
}

function updateTable() {
    var nodeTable = '<table><tr>' +
	'<th>ID</th>' +
	'<th>Hostname</th>' +
	'<th>Entries in the Cache</th>' +
	'<th>Memory Cache Size (in bytes)</th>' +
	'<th>Disk Cache Size (in bytes)</th>' +
	'<th>Download Queue Length</th>' +
	'<th>Prefetch Queue Length</th>' +
	'</tr>';
    console.log('nodes '+nodes);
    var ids = getKeys(nodes);
    console.log('ids '+ids);
    ids.sort();
    console.log('sorted ids '+ids);
    for (var i in ids) {
	var id = ids[i]
	console.log('adding node ' + id);
	var node = nodes[id];
	elapsedFromUpdate = (new Date() - node.updateTime) / 1000; // To have seconds
	if (elapsedFromUpdate > secondsForForget) {
	    delete node[id];
	    continue;
	} else if (elapsedFromUpdate > secondsForOld) {
	    nodeTable += '<tr class="old">';
	} else {
	    nodeTable += '<tr>';
	}
	nodeTable += '<td>' + id + '</td>' +
	    '<td>' + node.hostname + '</td>' +
	    '<td>' + node.num_entries + '</td>' +
	    '<td>' + node.mem_size + '</td>' +
	    '<td>' + node.disk_size + '</td>' +
	    '<td>' + node.dq + '</td>' +
	    '<td>' + node.pq + '</td>' +
	    '</tr>';
    }
    nodeTable.concat('</table>');
    $('#table').html(nodeTable);
}

init();
