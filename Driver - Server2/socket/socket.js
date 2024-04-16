const log = require('../log/winston').logger('Socket Service');

const conf = require('../conf/conf');

const SOCKET_TOPIC = {
    COMMON: 'COMMON',
    HELLO: 'hello',
}
module.exports.SOCKET_TOPIC = SOCKET_TOPIC;

let sockets = new Set();

module.exports.initSocket = function (socket) {
    sockets.add(socket)
    log.info('***** Current socket size: ', sockets.size)
    log.info('***** One socket connection: ', socket.id);

    socket.on('disconnection', function () {
        log.warn('***** One socket disconnection: ', socket.id);
        if (sockets) {
            sockets.delete(socket);
            log.info('***** One socket disconnection. ');
            log.info('***** Current sockets size: ', sockets.size);
        }
    })

    socket.on(SOCKET_TOPIC.COMMON, function (body) {
        log.info(`***** Receive socket msg: Topic => `, SOCKET_TOPIC.COMMON);
        log.info(`***** Receive socket msg: Msg   => `, JSON.stringify(body));
    })

    socket.on(SOCKET_TOPIC.HELLO, function (body) {
        log.info(`***** Receive socket msg: Topic => `, SOCKET_TOPIC.HELLO);
        log.info(`***** Receive socket msg: Msg   => `, JSON.stringify(body));
    })
}

module.exports.publicCommonSocketMsg = function (body) {
    log.info('***** Send msg to socket => Topic: %s, Body: %s', SOCKET_TOPIC.COMMON, body)
    for (let socket of sockets) {
        if (socket.connected) socket.emit(SOCKET_TOPIC.COMMON, body)
    }
}

module.exports.publicSocketMsg = function (topic, body) {
    log.info('***** Send msg to socket => Topic: %s, Body: %s', topic, body)
    for (let socket of sockets) {
        if (socket.connected) socket.emit(topic, body)
    }
}
