const log = require('../winston/logger').logger('Index Service');
const utils = require('../util/utils');

const { FirebaseNotification } = require('../model/firebaseNotification');
const { sendNotification } = require('../firebase/firebase');

module.exports = {
    publicFirebaseNotification: async function (req, res) {
        try {
            const checkParams = function (targetList, title, content) {
                if (!targetList.length) {
                    log.warn(`TargetList is empty`)
                    throw `TargetList is empty`
                }
            }

            let { targetList, title, content } = req.body;

            // TODO: check params
            log.info(`Check Firebase Notification params...`);
            checkParams(targetList, title, content);    
            log.info(`Check Firebase Notification params finished.`);

            // TODO: generate payload
            log.info(`Prepare Send Firebase Notification ...`);
            let messageList = [], notificationList = [];
            for (let target of targetList) {
                messageList.push({
                    topic: target.driverId.toString(),
                    notification: {
                        title: title,
                        body: content,
                    },
                    data: {}
                });
                notificationList.push({
                    taskId: target.taskId,
                    driverId: target.driverId,
                    vehicleNo: target.vehicleNo,
                    type: target.type,
                    title,
                    content,
                })
            }

            log.info(`Store record into db ...`);
            notificationList = await FirebaseNotification.bulkCreate(notificationList);
            log.info(`messageList: `);
            log.info(JSON.stringify(messageList, null, 4))
            log.info(`notificationList: `);
            log.info(JSON.stringify(notificationList, null, 4))
            log.info(`Prepare Send Firebase Notification finished`);
            sendNotification(messageList, notificationList)
            return res.json(utils.response(1, 'Success'));
        } catch (error) {
            log.error(error)
            return res.json(utils.response(0, error));
        }
    },
    publicFirebaseNotificationBySystem: async function (req, res) {
        try {
            let { notificationList } = req.body;
            if (!notificationList.length) {
                log.warn(`NotificationList is empty`)
                throw `NotificationList is empty`
            }

            // TODO: generate payload
            log.info(`Prepare Send Firebase Notification ...`);
            let messageList = [], noticeList = [];
            for (let target of notificationList) {
                messageList.push({
                    topic: target.driverId.toString(),
                    notification: {
                        title: target.title,
                        body: target.content,
                    },
                    data: {
                        type: target.type,
                    }
                });
                noticeList.push({
                    taskId: target.taskId,
                    driverId: target.driverId,
                    vehicleNo: target.vehicleNo,
                    type: target.type,
                    title: target.title,
                    content: target.content,
                })
            }

            log.info(`Store record into db ...`);
            noticeList = await FirebaseNotification.bulkCreate(noticeList);
            log.info(`messageList: `);
            log.info(JSON.stringify(messageList, null, 4))
            log.info(`noticeList: `);
            log.info(JSON.stringify(noticeList, null, 4))
            log.info(`Prepare Send Firebase Notification finished`);
            sendNotification(messageList, noticeList)
            return res.json(utils.response(1, 'Success'));
        } catch (error) {
            log.error(error)
        }
    }
}
