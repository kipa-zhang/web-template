const log = require('../winston/logger').logger('Firebase Service');
const { FirebaseNotification } = require('../model/firebaseNotification');

const conf = require('../conf/conf');
const { HttpsProxyAgent } = require('https-proxy-agent')
// const agent = new HttpsProxyAgent(`${ conf.proxy.protocol }://${ conf.proxy.host }:${ conf.proxy.port }'`)

var admin = require("firebase-admin");
var serviceAccount = require("./serviceAccountKey.json");

let firebaseMessage = null;
const getMessaging = function () {
    try {
        if (!firebaseMessage) {
            admin.initializeApp({
                credential: admin.credential.cert(serviceAccount),
                // httpAgent: agent,
            });
            firebaseMessage = admin.messaging();
            if (firebaseMessage) {
                log.info(`Success get firebaseMessage object.`)
            } else {
                log.info(`Failed get firebaseMessage object.`)
            }
        }
        return firebaseMessage;
    } catch (error) {
        log.error(`getMessaging =>`)
        log.error(error)
        return null;
    }
    
}
module.exports.getMessaging = getMessaging

module.exports.sendNotification = async function (messageList, notificationList) {
    try {
        log.info(`Send Firebase Notification =>`)
        getMessaging().sendAll(messageList)
            .then(resp => {
                // TODO: check notification result
                log.info(JSON.stringify(resp, null, 4));
                let index = 0, successNotificationId = [], invalidTokenDriverId = [];
                for (let response of resp.responses) {
                    const error = response.error;
                    if (error) {
                        log.error(`Failure sending notification to`, messageList[index].token);
                        log.error(error);
                        // Cleanup the tokens who are not registered anymore.
                        if (error.code === 'messaging/invalid-registration-token' || error.code === 'messaging/registration-token-not-registered') {
                            invalidTokenDriverId.push(notificationList[index].driverId);
                        }
                    } else {
                        successNotificationId.push(notificationList[index].id)
                    }
                    index++;
                }
                if (successNotificationId.length) {
                    log.info(`Update Already Send Firebase Notification ID => ${ JSON.stringify(successNotificationId) } `)
                    FirebaseNotification.update({ success: 1 }, { where: { id: successNotificationId } });
                }
                if (invalidTokenDriverId.length) {
                    log.info(`Clear Invalid Token of Firebase Notification DriverId => ${ JSON.stringify(invalidTokenDriverId) } `)
                }
                log.info(`Finish Send Firebase Notification ...`)
            }).catch(error => {
                log.error(`Send Firebase Failed => `);
                log.error(error);
            });
    } catch (error) {
        log.error(error);
    }
}
