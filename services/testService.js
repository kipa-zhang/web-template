// const { sequelizeObj } = require('../db/dbConf')
// const log = require('../log/winston').logger('Test Service');
// const moment = require('moment');
// const _ = require('lodash');

// const { Sequelize, Op, QueryTypes } = require('sequelize');
// const { DriverPosition } = require('../model/driverPosition');
// const { Device } = require('../model/device');
// const { User } = require('../model/user');

// /********************************************* */
// /*****   GPS By Backup Test   ******/
// const createDriverGPSRecord2 = async function ({ driverId, vehicleNo, startTime, hours }) {
//     try {
//         let recordList = [];
//         let time = moment(startTime).valueOf();
        
//         for (let index = 0; index < hours * 3600; index++) {
//             let newTime = moment(time + index * 1000).format('YYYY-MM-DD HH:mm:ss');
    
//             let amplitude  = 2
//             let speed = 56 + Math.floor(amplitude * Math.sin(index));
            
//             if (index > 100 && index < 110) speed += 15
//             if (index > 300 && index < 310) speed += 15
//             if (index > 500 && index < 510) speed += 15
//             if (index > 700 && index < 710) speed += 15
//             if (index > 900 && index < 910) speed += 15
    
//             let lat = `1.39${ index }`
//             let lng = `103.84${ index }`
//             recordList.push([ driverId, vehicleNo, lat, lng , speed, newTime, newTime, newTime, 1, 1, 1 ])
            
//             if (index == hours * 3600 - 1) {
//                 let user = await User.findOne({ where: { driverId } })
//                 if (!user) continue
//                 let exist = await DriverPosition.findOne({ where: { driverId, vehicleNo } })
//                 if (exist) {
//                     if (moment(exist.updatedAt).isBefore(moment(newTime))) {
//                         await DriverPosition.upsert({ driverId, vehicleNo, unitId: user.unitId, speed, lat, lng, updatedAt: newTime, creator: user.userId })
//                     }
//                 } else {
//                     await DriverPosition.upsert({ driverId, vehicleNo, unitId: user.unitId, speed, lat, lng, updatedAt: newTime, creator: user.userId })
//                 }
//             }
//         }

//         let sql = ` INSERT INTO driver_position_history(driverId, vehicleNo, lat, lng, speed, createdAt, gpsTime, receiveTime, gpsPermission, gpsService, network) VALUE ?; `;
//         await sequelizeObj.query(sql, { replacements: [ recordList ], type: QueryTypes.INSERT })
//     } catch (error) {
//         throw error
//     }
// }
// const createDeviceGPSRecord2 = async function ({ deviceId, startTime, hours }) {
//     try {
//         let recordList = [];
//         let time = moment(startTime).valueOf();
        
//         for (let index = 0; index < hours * 3600; index++) {
//             let newTime = moment(time + index * 1000).format('YYYY-MM-DD HH:mm:ss');
    
//             let amplitude = 2
            
//             let speed = 56 + Math.floor(amplitude * Math.sin(index));

//             if (index > 100 && index < 110) speed += 15
//             if (index > 300 && index < 310) speed += 15
//             if (index > 500 && index < 510) speed += 15
//             if (index > 700 && index < 710) speed += 15
//             if (index > 900 && index < 910) speed += 15
    
//             let lat = `1.39${ index }`
//             let lng = `103.84${ index }`
//             recordList.push([ deviceId, lat, lng, speed, 0, newTime, newTime])
    
//             if (index == hours * 3600 - 1) {
//                 let exist = await Device.findByPk(deviceId)
//                 if (exist) {
//                     if (!exist.updatedAt || moment(exist.updatedAt).isBefore(moment(newTime))) {
//                         await Device.upsert({ deviceId, speed, lat, lng, updatedAt: newTime })
//                     }
//                 } else {
//                     await Device.upsert({ deviceId, speed, lat, lng, updatedAt: newTime })
//                 }
//             }
//         }

//         let sql = ` INSERT INTO device_position_history(deviceId, lat, lng, speed, rpm, createdAt, deviceTime) VALUE ?; `;
        
//         await sequelizeObj.query(sql, { replacements: [ recordList ], type: QueryTypes.INSERT });
//     } catch (error) {
//         throw error
//     }
// }

// const createGPSMain = async function () {
//     try {
//         // createDriverGPSRecord2({ 
//         //     driverId: 69,
//         //     vehicleNo: 'KIPA0623',
//         //     startTime: '2023-12-27 10:10:00',
//         //     hours: 1
//         // })
//         createDriverGPSRecord2({ 
//             driverId: 69,
//             vehicleNo: 'pK100-10',
//             startTime: '2024-04-28 01:10:00',
//             hours: 22
//         })
//         createDriverGPSRecord2({ 
//             driverId: 171,
//             vehicleNo: 'VK100-142',
//             startTime: '2024-04-28 01:10:00',
//             hours: 22
//         })
//         // createDriverGPSRecord2({ 
//         //     driverId: 1150,
//         //     vehicleNo: 'pK100-10',
//         //     startTime: '2024-01-08 10:50:00',
//         //     hours: 3
//         // })
//         // createDriverGPSRecord({ 
//         //     driverId: 170,
//         //     vehicleNo: 'VK100-123',
//         //     startTime: '2023-09-26 17:50:00',
//         //     hours: 0.1
//         // })

//         // createDeviceGPSRecord2({
//         //     deviceId: 'AHNJ8888D',
//         //     startTime: '2023-12-27 11:10:00',
//         //     hours: 1
//         // })
//         createDeviceGPSRecord2({
//             deviceId: 'VK100-142',
//             startTime: '2024-04-28 01:10:00',
//             hours: 22
//         })
//         createDeviceGPSRecord2({
//             deviceId: 'pK100-10',
//             startTime: '2024-04-28 01:10:00',
//             hours: 22
//         })
//         // createDeviceGPSRecord2({
//         //     deviceId: 'AHNJ9999D',
//         //     startTime: '2024-01-08 10:10:00',
//         //     hours: 3
//         // })
//         // createDeviceGPSRecord2({
//         //     deviceId: 'Device-pK100-10',
//         //     startTime: '2024-01-08 10:50:00',
//         //     hours: 3
//         // })
//         // createDeviceGPSRecord({
//         //     deviceId: 'Device-VK100-123',
//         //     startTime: '2023-09-26 18:40:00',
//         //     hours: 0.1
//         // })
//     } catch (error) {
//         log.error(error)
//     }
// }
// createGPSMain();




// /********************************************* */
// /*****   GPS By Hour Test   ******/

// const createDriverGPSRecord = async function ({ driverId, vehicleNo, startTime, hours, tableName }) {
//     try {
//         let recordList = [];
//         let time = moment(startTime).valueOf();
        
//         for (let index = 0; index < hours * 3600; index++) {
//             let newTime = moment(time + index * 1000).format('YYYY-MM-DD HH:mm:ss');
    
//             let amplitude = 2
            
//             let speed = 56 + Math.floor(amplitude * Math.sin(index));

//             if (index > 100 && index < 110) speed += 15
//             if (index > 300 && index < 310) speed += 15
//             if (index > 500 && index < 510) speed += 15
//             if (index > 700 && index < 710) speed += 15
//             if (index > 900 && index < 910) speed += 15
    
//             let lat = `1.39${ index }`
//             let lng = `103.84${ index }`
//             recordList.push([ driverId, vehicleNo, lat, lng , speed, newTime, newTime, newTime, 1, 1, 1 ])
            
//             if (index == hours * 3600 - 1) {
//                 let user = await User.findOne({ where: { driverId } })
//                 if (!user) continue
//                 let exist = await DriverPosition.findOne({ where: { driverId, vehicleNo } })
//                 if (exist) {
//                     if (moment(exist.updatedAt).isBefore(moment(newTime))) {
//                         await DriverPosition.upsert({ driverId, vehicleNo, unitId: user.unitId, speed, lat, lng, updatedAt: newTime, creator: user.userId })
//                     }
//                 } else {
//                     await DriverPosition.upsert({ driverId, vehicleNo, unitId: user.unitId, speed, lat, lng, updatedAt: newTime, creator: user.userId })
//                 }
//             }
//         }
//         let sql = ` INSERT INTO ${ tableName }(driverId, vehicleNo, lat, lng, speed, createdAt, gpsTime, receiveTime, gpsPermission, gpsService, network) VALUE ?; `;
        
//         await sequelizeObj.query(sql, { replacements: [ recordList ], type: QueryTypes.INSERT })
//     } catch (error) {
//         throw error
//     }
// }
// const createDeviceGPSRecord = async function ({ deviceId, startTime, hours, tableName }) {
//     try {
//         let recordList = [];
//         let time = moment(startTime).valueOf();
        
//         for (let index = 0; index < hours * 3600; index++) {
//             let newTime = moment(time + index * 1000).format('YYYY-MM-DD HH:mm:ss');
    
//             let amplitude = 2
            
//             let speed = 56 + Math.floor(amplitude * Math.sin(index));

//             if (index > 100 && index < 110) speed += 15
//             if (index > 300 && index < 310) speed += 15
//             if (index > 500 && index < 510) speed += 15
//             if (index > 700 && index < 710) speed += 15
//             if (index > 900 && index < 910) speed += 15
    
//             let lat = `1.39${ index }`
//             let lng = `103.84${ index }`
//             recordList.push([ deviceId, lat, lng, speed, 0, newTime, newTime])
    
//             if (index == hours * 3600 - 1) {
//                 let exist = await Device.findByPk(deviceId)
//                 if (exist) {
//                     if (moment(exist.updatedAt).isBefore(moment(newTime))) {

//                         await Device.upsert({ deviceId, speed, lat, lng, updatedAt: newTime })
//                     }
//                 } else {
//                     await Device.upsert({ deviceId, speed, lat, lng, updatedAt: newTime })
//                 }
//             }
//         }
//         let sql = ` INSERT INTO ${ tableName }(deviceId, lat, lng, speed, rpm, createdAt, deviceTime) VALUE ?; `;
        
//         await sequelizeObj.query(sql, { replacements: [ recordList ], type: QueryTypes.INSERT });
//     } catch (error) {
//         throw error
//     }
// }
// const createGPSMainByHour = async function () {
//     // Driver
//     for (let index = 0; index < 1; index++) {
//         // Date
//         for (let index2 = 7; index2 < 10; index2++) {
//             let date = index2;
//             if (date < 10) {
//                 date = _.padStart(date, 2, '0');
//             }
//             // Hour
//             for (let hour = 10; hour < 13; hour++) {
//                 if (hour < 10) hour = `0${ hour }`
//                 await checkTable(`2311${ date }${ hour }`, 'MOBILE')
//                 await createDriverGPSRecord({ 
//                     driverId: 97 + index,
//                     vehicleNo: 'VK100-' + (130 + index),
//                     startTime: moment(`2023-11-${ date } 00:00:00`).add(hour, 'h').format('YYYY-MM-DD HH:mm:ss'),
//                     hours: 1,
//                     tableName: `driver_position_history_2311${ date }${ hour }`
//                 })
//             }
//         }
//     }

//     // Device
//     for (let index = 0; index < 1; index++) {
//         // Date
//         for (let index2 = 7; index2 < 10; index2++) {
//             let date = index2;
//             if (date < 10) date = _.padStart(date, 2, '0');
//             // Hour
//             for (let hour = 10; hour < 13; hour++) {
//                 if (hour < 10) hour = `0${ hour }`
//                 await checkTable(`2311${ date }${ hour }`, 'OBD')
//                 await createDeviceGPSRecord({ 
//                     deviceId: 'Device-VK100-' + (130 + index),
//                     startTime: moment(`2023-11-${ date } 00:00:00`).add(hour, 'h').format('YYYY-MM-DD HH:mm:ss'),
//                     hours: 1,
//                     tableName: `device_position_history_2311${ date }${ hour }`
//                 })
//             }
//         }
//     }
// }
// // createGPSMainByHour()