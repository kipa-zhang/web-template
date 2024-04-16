const mqtt = require('mqtt');
const moment = require('moment');

const conf = require('../conf/conf.js');
const log = require('../log/winston').logger('ActiveMQ Server');

const SOCKET = require('../socket/socket');

const MQService = require('../services/mqService');

/**
 * Topics
 */
const MobileNotification = 'mobileNotification';

const AskRoute = 'AskRoute';
const ReRoute = 'Re-Route';
const SendRoute = 'SendRoute';
const RouteAgain = 'RouteAgain';

const AddIncident = 'addIncident';
const AffectRoutes = 'reRouteAllResult';

const NoGoZone = 'create_nogozone';
const NoGoZoneReturnResult = 'NoGoZoneReturnResult';

const CreateCAZone = 'CreateCAZone';
const ResultCAZone = 'ResultCAZone';
const UserZoneReturnResult = 'UserZoneReturnResult';

const UPDATE_WAY_POINT = 'UpdateWaypointCAUser';
module.exports.UPDATE_WAY_POINT = UPDATE_WAY_POINT;

/**
 * https://www.npmjs.com/package/mqtt
 */
let client = null;
let clientId = "mqtt_" + Math.random().toString(16).substring(2, 8).toUpperCase();

client = mqtt.connect(conf.activeMQConf, {
    clientId: clientId,
    clean: false, // set to false to receive QoS 1 and 2 messages while offline
    keepalive: 10, // seconds
    reconnectPeriod: 20 * 1000, // milliseconds
    connectTimeout: 20 * 1000, // milliseconds
    // no need re-subscribe here, or will receive nothing here!
    resubscribe: false, // if connection is broken and reconnects, subscribed topics are automatically subscribed again
    properties: {
        sessionExpiryInterval: 1 * 3600
    }
})

client.on('connect', function () {
    client.subscribe(MobileNotification, { qos: 1 }, function (error) {
        if (error) log.error(error);
        else log.info('Subscribe Topic => ' + MobileNotification);
    })
    client.subscribe(SendRoute, { qos: 1 }, function (error) {
        if (error) log.error(error);
        else log.info('Subscribe Topic => ' + SendRoute);
    })
    client.subscribe(RouteAgain, { qos: 1 }, function (error) {
        if (error) log.error(error);
        else log.info('Subscribe Topic => ' + RouteAgain);
    })
    client.subscribe(NoGoZoneReturnResult, { qos: 1 }, function (error) {
        if (error) log.error(error);
        else log.info('Subscribe Topic => ' + NoGoZoneReturnResult);
    })
    client.subscribe(ResultCAZone, { qos: 1 }, function (error) {
        if (error) log.error(error);
        else log.info('Subscribe Topic => ' + ResultCAZone);
    })
    client.subscribe(AffectRoutes, { qos: 1 }, function (error) {
        if (error) log.error(error);
        else log.info('Subscribe Topic => ' + AffectRoutes);
    })

    log.info('ActiveMQ Started!')
});

client.on('message', function (topic, payload) {
    log.info('Topic => ' + topic)
    log.info('Payload => ' + payload.toString())

    switch (topic) {
        case SendRoute:
			SOCKET.publicSocketMsg(SendRoute, payload);
            MQService.updateTaskRoute(payload.toString())
            break;
        case RouteAgain:
            SOCKET.publicSocketMsg(RouteAgain, payload);
            break;
        case NoGoZoneReturnResult:
            SOCKET.publicSocketMsg(NoGoZoneReturnResult, payload);
            break;
        case ResultCAZone:
            // save affected routes, userZoneId info into incident
            MQService.resultCAZone(payload).then(flag => {
                SOCKET.publicSocketMsg(UserZoneReturnResult, flag);
            });
            break;
        // case SendRoute:
        //     // save affected routes info into incident
        //     MQService.affectedRouteHandler(payload);
        //     break;
    }
})

module.exports.publicMobileNotification = function (body) {
    try {
        if (typeof body === 'object') body = JSON.stringify(body);
        client.publish(MobileNotification, body, { qos: 0 });
        log.info('Active MQ Send Topic : ', MobileNotification);
        log.info('Active MQ Send Body : ', body);
    } catch (error) {
        log.error(error);
    }
}

module.exports.publicMQMsg = function (topic, body) {
    try {
        client.publish(topic, body);
        log.info('Active MQ Send Topic : ', topic);
        log.info('Active MQ Send Body : ', body);
    } catch(error) {
        log.error(error);
    }
}

module.exports.publicAskRoute = function (body) {
    try {
        client.publish(AskRoute, body);
        log.info('Active MQ Send Topic : ', AskRoute);
        log.info('Active MQ Send Body : ', body);
    } catch (error) {
        log.error(error);
    }
}
module.exports.publicReRoute = function (body) {
    try {
        client.publish(ReRoute, body);
        log.info('Active MQ Send Topic : ', ReRoute);
        log.info('Active MQ Send Body : ', body);
    } catch(error) {
        log.error(error);
    }
}

module.exports.publicCreateUserZone = function (body) {
    try {
        client.publish(CreateCAZone, body);
        log.info('Active MQ Send Topic : ', CreateCAZone);
        log.info('Active MQ Send Body : ', body);
    } catch (error) {
        log.error(error);
    }
}
module.exports.publicNoGoZone = function (body) {
    try{
        client.publish(NoGoZone, body);
        log.info('Active MQ Send Topic : ', NoGoZone);
        log.info('Active MQ Send Body : ', body);
    } catch (error) {
        log.error(error);
    }
}

module.exports.publicIncident = function (body) {
    try{
        client.publish(AddIncident, body);
        log.info('Active MQ Send Topic : ', AddIncident);
        log.info('Active MQ Send Body : ', body);
    }catch(e){
        log.error(e);
    }
}