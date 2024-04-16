const log = require('../log/winston').logger('Firebase Service');
const conf = require('../conf/conf');

const axios = require('axios');
const { User } = require('../model/user.js');
const { Task } = require('../model/task');
const sendNotification = async function (targetList, title, content) {
    try {
        log.info(`Start Send Firebase Notification ...`)
        log.info(JSON.stringify({ targetList, title, content }, null, 4))
        return await axios.post(`${ conf.firebaseServer }/publicFirebaseNotification`, { targetList, title, content })
            .then(result => {
                log.info(JSON.stringify(result.data))
                log.info(`Finish Send Firebase Notification ...`)

                if (result.data.resp_code == 1) {
                    log.info(`Send Firebase Notification success.`)
                    return true
                } else {
                    log.warn(`Send Firebase Notification failed.`)
                    return false
                }
            })
    } catch (error) {
        log.error(error)
    }
}

module.exports.createFirebaseNotification = async function (taskIdList, title, content) {
    try {
        if (!taskIdList.length) {
            log.warn(`TaskIdList is empty!`)
            return;
        }
        let notificationList = [];
        for (let taskId of taskIdList) {
            let task = await Task.findByPk(taskId);
            if (task) {
                if (!task.driverId) {
                    log.info(`Task ${ taskId } has no driver, no need send firebase notification`)
                } else {
                    let user = await User.findOne({ where: { driverId: task.driverId } })
                    if (user && user.firebaseToken) {
                        notificationList.push({
                            taskId,
                            token: user.firebaseToken,
                            driverId: task.driverId,
                            vehicleNo: task.vehicleNumber
                        })
                    } else {
                        log.warn(` DriverId ${ task.driverId } does not exist in user table or has no firebase token now ! `)
                    }
                    
                }
            } else {
                log.warn(` TaskId ${ taskId } does not exist ! `)
            }
        }
        if (notificationList.length) {
            // sendNotification(notificationList, title, content);
            log.info(`Current Notification length => ${ notificationList.length }`)
            let count = Math.ceil(notificationList.length / 100);
            log.info(`Notification will be cut into parts => ${ count }`)
            for (let index = 0; index < count; index++) {
                log.info(` ******************************************* `)
                log.info(`Will Send Firebase Notification part ${ index + 1 }`)
                if (index == (count - 1)) {
                    // last part
                    sendNotification(notificationList.slice( 100 * index ), title, content);
                } else {
                    sendNotification(notificationList.slice( 100 * index, 100 * index + 100 ), title, content);
                }
            }
        } else {
            log.warn(`There are no notification need to send.`)
        }
    } catch (error) {
        log.error(error)
    }
}

/**
 * 
 * @param {*} [{
 *      taskId,
        token: user.firebaseToken,
        driverId: task.driverId,
        vehicleNo: task.vehicleNumber
 * }]
 * @param {*} title 
 * @param {*} content 
 * @returns 
 */
module.exports.createFirebaseNotification2 = async function (notificationList, title, content) {
    try {
        if (notificationList.length) {
            // sendNotification(notificationList, title, content);
            log.info(`Current Notification length => ${ notificationList.length }`)
            let count = Math.ceil(notificationList.length / 100);
            log.info(`Notification will be cut into parts => ${ count }`)
            for (let index = 0; index < count; index++) {
                log.info(` ******************************************* `)
                log.info(`Will Send Firebase Notification part ${ index + 1 }`)
                if (index == (count - 1)) {
                    // last part
                    sendNotification(notificationList.slice( 100 * index ), title, content);
                } else {
                    sendNotification(notificationList.slice( 100 * index, 100 * index + 100 ), title, content);
                }
            }
        } else {
            log.warn(`There are no notification need to send.`)
        }
    } catch (error) {
        log.error(error)
    }
}