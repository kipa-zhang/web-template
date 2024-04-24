const log = require('../log/winston').logger('Upload Service');
const utils = require('../util/utils');
const CONTENT = require('../util/content');
const conf = require('../conf/conf');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf')

const path = require('path');
const fs = require('graceful-fs');
const moment = require('moment');
const xlsx = require('node-xlsx');
const formidable = require('formidable');
const _ = require('lodash');

const { Driver } = require('../model/driver');
const { Device } = require('../model/device');
const { Vehicle } = require('../model/vehicle');
const { VehicleRelation } = require('../model/vehicleRelation');
const { PermitType } = require('../model/permitType');
const { User } = require('../model/user');	
const { Unit } = require('../model/unit');
const { Friend } = require('../model/friend.js');
const { Waypoint } = require('../model/waypoint.js');
const { OperationRecord } = require('../model/operationRecord.js');

/**
 * https://github.com/node-formidable/formidable
 * https://github.com/mgcrea/node-xlsx#readme
 */

module.exports.uploadVehicle = async function (req, res) {
	let dirPath = conf.uploadFilePath + "\\vehicle"
	if (!fs.existsSync(dirPath)) {
		fs.mkdirSync(dirPath, {recursive: true});
	}
	const form = formidable({ 
		multiples: true, 
		maxFileSize: 10 * 1024 * 1024, 
		keepExtensions: false, 
		uploadDir: dirPath,
		fileExt: /\.xlsx$|\.xls$/i
	 });
	form.parse(req, async (error, fields, files) => {
		if (error) {
			log.error(error)
			return res.json(utils.response(0, 'Upload failed!'));
		}
		log.info('fields: ', JSON.stringify(fields))
		log.info('files: ', JSON.stringify(files))
		// change to array object(allow multi files upload)
		if (files.file.constructor !== Array) files.file = [files.file];

		let uploadVehicleList = [], uploadVehicleRelationList = [], uploadDeviceList = [], uploadUnitList = [], permitTypeList = [];
		let indexOfVehicleNo = 0, indexOfHardWareID = 1, indexOfUnit = 2, indexOfSubUnit = 3, indexOfLimitSpeed = 4;
		let indexOfVehicleType = 5, indexOfPermitType = 6, indexOfTotalMileage = 7, indexOfDimensions = 8, indesOfAvidate = 9;
		try {
			let warnMsgHtml = ``;
			for (let file of files.file) {
				if (file.name.endsWith('.xlsx') || file.name.endsWith('.xls')) {
					let list = xlsx.parse(file.path, { cellDates: true });
					// First sheet's data
					log.info(JSON.stringify(list));
					list = list[0].data; 
					let rowIndex = -1;
					for (let data of list) {
						rowIndex++;

						// First row is title!
						if (rowIndex === 0) {
							if (!checkVehicleFile(data, { indexOfVehicleNo, indexOfHardWareID, indexOfUnit, indexOfSubUnit, indexOfLimitSpeed, indexOfVehicleType, indexOfPermitType, indexOfTotalMileage, indexOfDimensions, indesOfAvidate})) {
								log.warn('Please Select Vehicle Excel file!')
								return res.json(utils.response(0, 'Please Select Vehicle Excel file!'));
							}
							continue;
						}
						let vehicleNo = data[indexOfVehicleNo] ? data[indexOfVehicleNo].trim() : '';
						let unit = data[indexOfUnit] ? data[indexOfUnit].trim() : null;
						let subUnit = data[indexOfSubUnit] ? data[indexOfSubUnit].trim() : null;

						if (!vehicleNo) {
							log.info('Vehicle No is empty, jump this row!')
							warnMsgHtml += `Row ${rowIndex + 1}: Vehicle No is empty, jump this row!<br/>`
						} else if (!unit) {
							warnMsgHtml += `Row ${rowIndex + 1}: Vehicle unit is empty, jump this row!<br/>`
							continue;
						} else {
							let hardWareID = data[indexOfHardWareID] ? data[indexOfHardWareID].trim() : null;
							let totalMileage = data[indexOfTotalMileage];
							if (totalMileage) {
								totalMileage += '';
							} else {
								totalMileage = '0';
							}

							let limitSpeed = data[indexOfLimitSpeed];
							if (limitSpeed) {
								limitSpeed += '';
							} else {
								limitSpeed = '60';
							}

							let existVehicle = uploadVehicleList.find(item => item.vehicleNo == vehicleNo);
							if (!existVehicle) {
								uploadVehicleList.push({ 
									vehicleNo: vehicleNo, 
									unit: unit, 
									subUnit: subUnit, 
									deviceId: hardWareID || null, 
									vehicleType: data[indexOfVehicleType].trim(), 
									limitSpeed: limitSpeed.trim(), // While xlsx is null here
									permitType: data[indexOfPermitType].trim(), 
									totalMileage: totalMileage.trim(), 
									dimensions: data[indexOfDimensions].trim(), 
									nextAviTime: data[indesOfAvidate] ? data[indesOfAvidate].trim() : null, 
									creator: req.cookies.userId 
								})
								uploadVehicleRelationList.push({ 
									vehicleNo: vehicleNo, 
									deviceId: hardWareID || null, 
									limitSpeed: limitSpeed.trim(),  // While xlsx is null here
									unit: unit, 
									subUnit: subUnit
								})
								permitTypeList.push({
									vehicleType: data[indexOfVehicleType].trim(), 
									permitType: data[indexOfPermitType].trim()
								});
							}

							if (data[indexOfUnit]) {
								let existUnit = uploadUnitList.find(item => (item.unit == unit && item.subUnit == subUnit));
								if (!existUnit) {
									uploadUnitList.push(data[indexOfUnit].trim() + ',' + (data[indexOfSubUnit] ? data[indexOfSubUnit].trim() : ''))
								}
							}
							if (hardWareID) {
								let existDevice = uploadDeviceList.find(item => (item.deviceId == hardWareID));
								if (!existDevice) {
									uploadDeviceList.push({  
										deviceId: hardWareID,
										creator: req.cookies.userId
									})
								}
							}
						}
					}
				} else {
					return res.json(utils.response(0, 'Please Use Excel file!'));
				}
			}
			await sequelizeObj.transaction(async transaction => {
				const updateUnit = async function (uploadUnitList) {
					if (!uploadUnitList.length) return;

					uploadUnitList = Array.from(new Set(uploadUnitList));
					let newUploadUnitList = []
					for (let uploadUnit of uploadUnitList) {
						newUploadUnitList.push({ 
							unit: uploadUnit.split(',')[0],
							subUnit: uploadUnit.split(',')[1] ? uploadUnit.split(',')[1] : null,
						})
					}
					uploadUnitList = newUploadUnitList;
					
					let unitList = await Unit.findAll();
					for (let uploadUnit of uploadUnitList) {
						for (let unit of unitList) {
							if (
								(unit.unit.toLowerCase() === uploadUnit.unit?.toLowerCase()) 
								&& 
								(unit.subUnit?.toLowerCase() === uploadUnit.subUnit?.toLowerCase())
							) {
								uploadUnit.id = unit.id;
							}
						}
					}
					await Unit.bulkCreate(uploadUnitList, { updateOnDuplicate: ['unit', 'subUnit'] })
				}
				const updateVehicle = async function (uploadVehicleList, unitList) {
					if (!uploadVehicleList.length) return;

					for (let vehicle of uploadVehicleList) {
						for (let unit of unitList) {
							if (
								(unit.unit.toLowerCase() === vehicle.unit?.toLowerCase()) 
								&& 
								(unit.subUnit?.toLowerCase() === vehicle.subUnit?.toLowerCase())
							) {
								vehicle.unitId = unit.id;
								break;
							}
						}
					}
					await Vehicle.bulkCreate(uploadVehicleList, { updateOnDuplicate: ['unitId', 'deviceId', 'limitSpeed', 'vehicleType', 'permitType', 'dimensions', 'totalMileage', 'nextAviTime'] });
				}
				const updateDevice = async function (uploadDeviceList) {
					if (!uploadDeviceList.length) return;

					await Device.bulkCreate(uploadDeviceList, { updateOnDuplicate: ['deviceId'] });
				}
				const updateVehicleRelation = async function (uploadVehicleRelationList) {
					if (!uploadVehicleRelationList.length) return;

					let vehicleRelationList = await VehicleRelation.findAll();
					for (let uploadVehicleRelation of uploadVehicleRelationList) { 
						for (let vehicleRelation of vehicleRelationList) {
							if (uploadVehicleRelation.vehicleNo === vehicleRelation.vehicleNo) {
								// once find same vehicleNo, just overwrite the db
								uploadVehicleRelation.id = vehicleRelation.id;
								break;
							} 
						}
					}
					await VehicleRelation.bulkCreate(uploadVehicleRelationList, { updateOnDuplicate: ['limitSpeed', 'deviceId'] })
				}

				const updatePermittype = async function (permitTypeList) {
					if (permitTypeList.length == 0) return

					let newPermitList = [];
					for (let temp of permitTypeList) { 
						let permit = temp.permitType;
						let vehicleType = temp.vehicleType;
						
						let vehicleTaskList = await sequelizeObj.query(`
							SELECT * FROM permittype p
							WHERE p.permitType = ? AND FIND_IN_SET(?, p.vehicleType)
						`, { 
							type: QueryTypes.SELECT, 
							replacements: [
								permit, vehicleType
						]})
						if (vehicleTaskList && vehicleTaskList.length > 0) {
							continue;
						} else {
							let oldPermitType = await PermitType.findOne({where: {permitType: permit}});
							let permitType = {};
							if (oldPermitType) {
								permitType = {permitType: permit, vehicleType: oldPermitType.vehicleType + ',' + vehicleType};
							} else {
								permitType = {permitType: permit, vehicleType: vehicleType};
							}

							newPermitList.push(permitType);
						}
					}

					await PermitType.bulkCreate(newPermitList, { updateOnDuplicate: ['vehicleType'] })
				}

				// update unit
				await updateUnit(uploadUnitList);
				let unitList = await Unit.findAll();
				// update vehicle
				await updateVehicle(uploadVehicleList, unitList);
				// update device
				await updateDevice(uploadDeviceList);
				// update vehicleRelation
				await updateVehicleRelation(uploadVehicleRelationList);

				await updatePermittype(permitTypeList);

				await OperationRecord.create({
					operatorId: req.cookies.userId,
					businessType: 'Upload Vehicle',
					businessId: null,
					optType: 'Upload',
					afterData: `${ JSON.stringify(uploadDeviceList) }`, 
					optTime: moment().format('YYYY-MM-DD HH:mm:ss'),
					remarks: null
				})

				return res.json(utils.response(1, warnMsgHtml || 'Success'));
			}).catch(error => {
				log.error('(uploadVehicle) : ', error);
				return res.json(utils.response(0, error));
			});
		} catch (error) {
			log.error('(uploadVehicle) : ', error);
			return res.json(utils.response(0, error));
		}			
	});
}

module.exports.uploadWaypoint = async function (req, res) {
    try {
		let dirPath = conf.uploadFilePath + "\\waypoint"
		if (!fs.existsSync(dirPath)) {
			fs.mkdirSync(dirPath, {recursive: true});
		}
		const form = formidable({ 
			multiples: true, 
			maxFileSize: 10 * 1024 * 1024, 
			keepExtensions: false, 
			uploadDir: dirPath
		});
		form.parse(req, async (error, fields, files) => {
			if (error) {
				log.error(error)
				return res.json(utils.response(0, 'Upload failed!'));
			}
			log.info('fields: ', JSON.stringify(fields))
			log.info('files: ', JSON.stringify(files))
			// change to array object(allow multi files upload)
			if (files.file.constructor !== Array) files.file = [files.file];

			let uploadWaypointList = [];
			let indexOfWaypointName = 0, indexOfLat = 1, indexOfLng = 2;
			for (let file of files.file) {
				if (file.name.endsWith('.xlsx') || file.name.endsWith('.xls')) {
					let list = xlsx.parse(file.path, { cellDates: true });
					// First sheet's data
					log.info(JSON.stringify(list));
					list = list[0].data; 
					let rowIndex = -1;
					for (let data of list) {
						rowIndex++;
						// First row is title!
						if (rowIndex === 0) {
							if (!checkWaypointFile(data, { indexOfWaypointName, indexOfLat, indexOfLng })) {
								log.warn('Please Select Waypoint Excel file!')
								return res.json(utils.response(0, 'Please Select Waypoint Excel file!'));
							}
							continue;
						}

						if (!data[indexOfWaypointName]) {
							log.info('Waypoint is empty, jump this row!')
						} else {
							uploadWaypointList.push({ 
								waypointName: data[indexOfWaypointName].trim(),
								lat: data[indexOfLat],
								lng: data[indexOfLng],
								creator: req.cookies.userId,
							});
						}
					}
				} else {
					return res.json(utils.response(0, 'Please Use Excel file!'));
				}
			}
			await sequelizeObj.transaction(async transaction => {
				const updateWaypointList = async function (uploadWaypointList) {
					if (!uploadWaypointList.length) return;

					let waypointList = await Waypoint.findAll();
					for (let uploadWaypoint of uploadWaypointList) {
						for (let waypoint of waypointList) {
							if (uploadWaypoint.waypointName.toLowerCase() === waypoint.waypointName.toLowerCase()) {
								uploadWaypoint.id = waypoint.id;
								break
							}
						}
					}
					await Waypoint.bulkCreate(uploadWaypointList, { updateOnDuplicate: ['lat', 'lng', 'creator'] });
				}

				// update waypoint
				await updateWaypointList(uploadWaypointList);
			}).catch(error => {
				throw error
			});
			return res.json(utils.response(1, 'Success'));			
		});
    } catch (err) {
        log.error('(uploadVehicle) : ', err);
        return res.json(utils.response(0, 'Server error!'));
    }
}

const checkFilePath = function (path) {
    try {
        if (!fs.existsSync(path)) fs.mkdirSync(path);
    } catch (error) {
		log.error(error);
        throw error
    }
}
const checkFileExist = function (filePath) {
    try {
        return fs.existsSync(filePath);
    } catch (error) {
		log.error(error);
        throw error
    }
}

module.exports.uploadImage = async function (req, res) {
	try {
		let folderPath = './public/resources/upload/notification';
		checkFilePath(folderPath);

		const form = formidable({ multiples: false, maxFileSize: 10 * 1024 * 1024, keepExtensions: false, uploadDir: folderPath });
		form.parse(req, async (error, fields, files) => {
			if (error) {
				log.error(error)
				return res.json(utils.response(0, 'Upload failed!'));
			}
			log.info('fields: ', JSON.stringify(fields))
			log.info('files: ', JSON.stringify(files))

			try {
				let newFileName = `${ utils.generateUniqueKey() }.${ files.file.type.split('/')[1] }`
				let filePath = `${ folderPath }/${ newFileName }`
				let result = checkFileExist(filePath)
				if (result) {
					return res.json(utils.response(0, 'File name already exist.'));
				} else {
					fs.copyFileSync(files.file.path, filePath)
					fs.unlinkSync(files.file.path)
					return res.json(utils.response(1, newFileName));
				}
			} catch (error) {
				log.error('(uploadVehicle) : ', error);
				return res.json(utils.response(0, error));
			}			
		});
	} catch (error) {
		log.error(error)
		return res.json(utils.response(0, 'Server error!')); 
	}
}

const checkDriverFile = function (titleData, indexObj) {
	if (!titleData[indexObj.indexOfDriver]?.trim().toLowerCase().includes('driver')) return false;
	if (!titleData[indexObj.indexOfVehicleNo]?.trim().toLowerCase().includes('vehicle')) return false;
	if (!titleData[indexObj.indexOfUnit]?.trim().toLowerCase().includes('unit')) return false;
	if (!titleData[indexObj.indexOfSubUnit]?.trim().toLowerCase().includes('sub-unit')) return false;
	if (!titleData[indexObj.indexOfLimitSpeed]?.trim().toLowerCase().includes('speed')) return false;
	if (!titleData[indexObj.indexOfDriverName]?.trim().toLowerCase().includes('name')) return false;
	if (!titleData[indexObj.indexOfPassword]?.trim().toLowerCase().includes('password')) return false;
	if (!titleData[indexObj.indexOfNric]?.trim().toLowerCase().includes('nric')) return false;
	return true;
}

const checkVehicleFile = function (titleData, indexObj) {
	if (!titleData[indexObj.indexOfVehicleNo]?.trim().toLowerCase().includes('vehicle')) return false;
	if (!titleData[indexObj.indexOfHardWareID]?.trim().toLowerCase().includes('hardware')) return false;
	if (!titleData[indexObj.indexOfUnit]?.trim().toLowerCase().includes('unit')) return false;
	if (!titleData[indexObj.indexOfSubUnit]?.trim().toLowerCase().includes('sub-unit')) return false;
	if (!titleData[indexObj.indexOfLimitSpeed]?.trim().toLowerCase().includes('speed')) return false;

	if (!titleData[indexObj.indexOfVehicleType]?.trim().toLowerCase().includes('vehicletype')) return false;
	if (!titleData[indexObj.indexOfPermitType]?.trim().toLowerCase().includes('permittype')) return false;
	if (!titleData[indexObj.indexOfTotalMileage]?.trim().toLowerCase().includes('totalmileage')) return false;
	if (!titleData[indexObj.indexOfDimensions]?.trim().toLowerCase().includes('dimensions')) return false;
	if (!titleData[indexObj.indesOfAvidate]?.trim().toLowerCase().includes('avi')) return false;
	return true;
}

const checkWaypointFile = function (titleData, indexObj) {
	if (!titleData[indexObj.indexOfWaypointName]?.trim().toLowerCase().includes('name')) return false;
	if (!titleData[indexObj.indexOfLat]?.trim().toLowerCase().includes('latitude')) return false;
	if (!titleData[indexObj.indexOfLng]?.trim().toLowerCase().includes('longitude')) return false;
	return true;
}