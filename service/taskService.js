const log = require('../log/winston').logger('Task Service');

const moment = require('moment');
const axios = require('axios');
const fs = require('graceful-fs');
const readline = require('readline')
const { openSync, closeSync, appendFileSync } = require('fs');
const utils = require('../util/utils');
const conf = require('../conf/conf');

const { Sequelize, Op, QueryTypes } = require('sequelize');
const { sequelizeObj } = require('../db/dbConf');
const { sequelizeSystemObj } = require('../db/dbConf_system');

const { User } = require('../model/user.js');
const { Driver } = require('../model/driver.js');
const _SystemJob = require('../model/system/job.js');

const { UrgentDuty } = require('../model/urgent/urgentDuty');

const outputService = require('../service/outputService');
const distanceService = require('../service/distanceService');
const mobileTOService = require('../service/mobileTOService');
const urgentService = require('../service/urgentService');

const { Mileage } = require('../model/mileage.js');
const { Vehicle } = require('../model/vehicle.js');
const { MileageHistory } = require('../model/mileageHistory.js');
const { Task } = require('../model/task.js');
const { ODD } = require('../model/odd.js');
const { MT_RAC } = require('../model/mtRAC');
const { MtAdmin } = require('../model/mtAdmin');
const { PermitType } = require('../model/permitType.js');

const _SystemDriver = require('../model/system/driver');
const _SystemTask = require('../model/system/task');

const { DriverPlatformConf } = require('../model/driverPlatformConf.js');

const { DriverMileage } = require('../model/driverMileage.js');

const { CheckList } = require('../model/checkList');

const vehicleMaintenanceInfoCalcUtils = require('../service/VehicleMaintenanceInfoCalcUtils.js');
const { UrgentIndent } = require('../model/urgent/urgentIndent.js');

const CHECKLIST = {
    "1": "Route Familiarisation",
    "2": "Force Preparation",
    "3": "Vehicle Check",
    "4": "Just-In-Time Training",
    "5": "MT-RAC",
}

const stroreRecordIntoFile = async function (updateTaskOptTimeObj) {
	try {
		let folderPath = `./mileageBackup`
		let filePath = `${ folderPath }/mileageBackup-${ moment().format('YYYY-MM-DD') }.txt`
		outputService.checkFilePath(folderPath)
		outputService.checkFileExist(filePath)

		let fd = openSync(filePath, 'a'); // 'a': write and append file

		let str = JSON.stringify(updateTaskOptTimeObj) + '\n';
		appendFileSync(fd, str, 'utf8');
        closeSync(fd);

	} catch (error) {
		log.error(`stroreRecordIntoFile => ` + error.message)
		log.error(error)
	}
}

let latestReturnResult = {
	"area":"Toa Payoh",
	"timestamp":moment().format('YYYY-MM-DD HH:mm:ss'),
	"forecast":"Cloudy",
	"icon":"iVBORw0KGgoAAAANSUhEUgAAADAAAAAwCAYAAABXAvmHAAAAAXNSR0IArs4c6QAABExJREFUaEPtmH3onWMYxz/fP4gwEdG8ZkjzspZMXiMzbGYRs6Ww0kILZfyx5CUxEZp3I69/GP7A2iwyRhJJIZIyamjNyx8yL0V9df26n5/7PL/nnOec7ZzzO786V51Oz3Pdz3Vf3/u67utNTHDSBNefIYDxtuDQAkMLbOcJDIwL2d4dmF7Cs0nSt60wjjsA23OBC4DLmyj6MrBS0ltV/HEDYPsY4F5gZpte9B7wkKQANEo9BWD7OOBY4GhgKvAu8AEwBbgD2AP4AlgNvC1pfa6c7QB3SfrtkHjzcxA9AWA73OEiYHbN6X4FnFvn57YPAh4AziuD6CqAdBHvBxaljX4CPgTi/0dgBnAiEBe2oNWS5tW5UZIdljo1rZ0j6fWuAbB9GvBOpsgKYIWk78rK2T4BuBk4O+NNk/R5KyC2DwFeA44CnpG0qG0Ats8HpiVfDn/8PvxX0krbJwOvAHslBWaW/blKMdvXAWGxoC2S9m3DEjcBtwN/APvUAqhwi/IenwBxyhcmxiRJv9cpUvBt7wn8mp6vl3RfjRUOTvvFssUtAdiOyPFlJjBO+QfgX2D/5AK7ZfyFkla1q3wGYgHwQno+RdL7NSDWpgCxoQ7AJuCAJGy6pE9zwbbjUt4JnAHcJunWTpXPQDyVLv9jkq6qARD73AI0B2D7QWBJEjRZ0uZmQiMp1V3AOmC2FwOPR56QdFI3AHwNHA40JI46RbaVn0WxrZJytxwj0nZrC9iOaPJz+nJKXaLZVqVL7rh3yhfxeqqkSHKVZHsNMAd4pPIO5DFdUm2k6gaAkGHbSdbpkjZUyU1RK9x5R+CcWgDAYZK+6ZaSNb5dAHgSuEvSxvJ62zcAdycPObABgO2IODdmlze+HxN9egXGdhR6kaULaohItvdLYTz4D0taMgogxfyXgCMzASPpulcKV5xu5J35Kb8cn/gjiiYX+wyIMnyzpMnxbgRAhfLXAOv65TpNfL2INMF+HoiCb1JaO1vSuhxAnHyUv0F9CZvtWDULl/nyyyQ9V7xQqVxYKim6pIEg2xFpIpzHyUelOkvSlly5AFCYar2kdtu7vgG0Ha7zagCQFNVwAwWAwn2ulRRdz0CR7SOAIqntKinK6FEKAFHXR2U5T1J0PANFtncC/kpKzZD0cRlAUXEukPTiQGn/f4QsSvoxRWVYIOrwqMeXS1o2gABiQPA0sFHSoVV3YClwD/CmpLMGEMATwBXAKkkLqwDMAt5IjEclXT0oIGxHo1RM5C6VFAmtMQrFk+1oAy9OnDObjfH6CayUn5oebF4LFZVg6LlM0vJ+KlzslUaO0VJeme2/s6S/q/RpVcz9mbLgL0DbU4btAL1LGtnEf0Ex8Zjbqp0d0w+ksWA0zDG+GA/6B4hY/2zMnOoUaNbQRPKIKVjxi9lNr+k34KNUMmxtd7O+tYvtKtTpuiGATk+s2+uHFuj2iXYqb2iBTk+s2+snvAX+A4v7kXZII+M2AAAAAElFTkSuQmCC",
	"label":"Today is a goog day!"
}

module.exports = {
	getTaskSummary: async function (req, res) {
		let { taskId } = req.body
		let task = null;
		let indentId = null;
		if (taskId.startsWith('DUTY')) {
			let idArray = taskId.split('-');
			if (idArray.length < 2) {
				log.warn(`TaskId ${taskId} format error.`)
				return res.json(utils.response(0, `TaskId ${taskId} format error.`));
			}
			taskId = `DUTY-${idArray[1]}`;
			if (idArray.length == 3) {
				indentId = idArray[2];
				let taskList = await sequelizeObj.query(`
					SELECT
						ud.dutyId as taskId,
						ui.startTime as indentStartTime,
						ui.endTime as indentEndTime,
						uc.purpose,
						ui.vehicleNo as vehicleNumber,
						ui.driverId,
						ui.indentId,
						ui.status as driverStatus
					FROM urgent_indent ui
					LEFT JOIN urgent_duty ud on ui.dutyId = ud.id
					LEFT JOIN urgent_config uc ON ud.configId = uc.id
					WHERE ui.id = '${indentId}'
				`, { 
					type: QueryTypes.SELECT, replacements: []
				});
				if (taskList.length) {
					task = taskList[0];
				}
			} else {
				let taskList = await sequelizeObj.query(` 
					SELECT
						ud.dutyId as taskId,
						ud.indentStartDate as indentStartTime,
						ud.indentEndDate as indentEndTime,
						uc.purpose,
						uc.vehicleNo as vehicleNumber,
						uc.driverId,
						'' as indentId,
						ud.status as driverStatus
					FROM urgent_duty ud
					LEFT JOIN urgent_config uc ON ud.configId = uc.id
					LEFT JOIN urgent_indent ui on ui.dutyId = ud.id
					WHERE ud.dutyId = '${taskId}'
				`, { 
					type: QueryTypes.SELECT, replacements: []
				});
				if (taskList.length) {
					task = taskList[0];
				}
			}
		} else {
			task = await Task.findOne({ where: { taskId } })
		}

		if (!task) {
			log.warn(`TaskId ${taskId} do not exist.`)
			return res.json(utils.response(0, `TaskId ${taskId} do not exist.`));
		}
		task = task.dataValues ? task.dataValues : task;
		let vehicle = await Vehicle.findByPk(task.vehicleNumber);
		if (vehicle?.keyTagId) {
			task.vehicleKeyTagId = vehicle.keyTagId;
		}

		return res.json(utils.response(1, task));
	},
	reportTaskODD: async function(req, res) {
		try {
            let { taskId, oddContent, type} = req.body
			
            let task = null;
			if (taskId.startsWith('DUTY')) {
				let idArray = taskId.split('-');
				if (idArray.length < 2) {
					log.warn(`TaskId ${taskId} format error.`)
					return res.json(utils.response(0, `TaskId ${taskId} format error.`));
				}
				taskId = `DUTY-${idArray[1]}`;
				let taskList = await sequelizeObj.query(` 
					SELECT
						ud.dutyId as taskId,
						ud.driverId
					FROM urgent_duty ud
					WHERE ud.dutyId = '${taskId}'
				`, { 
					type: QueryTypes.SELECT, replacements: []
				});
				if (taskList && taskList.length > 0) {
					task = taskList[0];
				}
			} else {
				task = await Task.findOne({ where: { taskId } })
			}

            if (!task) {
                log.warn(`TaskId ${taskId} do not exist.`)
                return res.json(utils.response(0, `TaskId ${taskId} do not exist.`));
            }

			let user = await User.findOne({ where: { driverId: task.driverId } });
			let userId = null;
			if (user) {
				userId = user.userId;
			}

			await ODD.create({
                taskId: task.taskId,
                contentFrom: type,
                content: oddContent,
				creator: userId
            })

            return res.json(utils.response(1, 'Success.'));
        } catch (ex) {
            log.error(ex)
            return res.json(utils.response(0, "Failed"));
        }
	},
	getWeatherForecast: async function (req, res) {
		const getNearestArea = function (currentPosition, areaList) {
			let nearestArea = null, nearestDistance = null;
			for (let area of areaList) {
				let distance = distanceService.getPointDistance({
					lat: area.label_location.latitude,
					lng: area.label_location.longitude,
				}, currentPosition);
				if (!nearestDistance || (distance < nearestDistance)) {
					nearestArea = area.name;
					nearestDistance = distance;
				}
			}
			return { nearestArea, nearestDistance }
		}
		const requestWeatherData = async function () {
			return await axios.get(conf.Weather_Forecast_API).then(result => {
				return result.data
			}).catch(error => {
				log.error(error)
				return null
			})
		}
		const getWeatherData = async function () {
			if (!fs.existsSync('./data/Data_Weather.json')) {
				fs.writeFileSync('./data/Data_Weather.json', '')
			}

			let result = fs.readFileSync('./data/Data_Weather.json', 'utf-8')
			if (result) {
				result = JSON.parse(result)
				let validPeriodTime = result.items[0].valid_period
				if (moment().isBetween(moment(validPeriodTime.start), moment(validPeriodTime.end))) {
					// valid
					log.info(`Data_Weather.json is valid, get data from here.`)
					return result
				} else {
					log.info(`Data_Weather.json is invalid, will get data from API and update Data_Weather.json.`)
				}
			} 
			// no data or in-valid, need request
			let data = await requestWeatherData();
			if (data && data.api_info.status === 'healthy' && JSON.stringify(data.items[0]) !== '{}') {
				fs.writeFileSync('./data/Data_Weather.json', JSON.stringify(data, null, 4))
				return data
			} else {
				throw new Error('Weather_Forecast_API is not working now.')
			}
		}
		try {
			let { lat, lng } = req.body;
			log.info(`getWeatherForecast API => ${conf.Weather_Forecast_API}`)
			let result = await getWeatherData();
			log.info(`getWeatherForecast API Data => ${JSON.stringify(result)}`)

			let { nearestArea, nearestDistance } = getNearestArea({ lat, lng }, result.area_metadata);
			log.info(`getNearestArea => `, nearestArea)
			let returnResult = { area: nearestArea, timestamp: moment(result.items[0].timestamp).format('YYYY-MM-DD HH:mm:ss') }
			result.items[0].forecasts.some(forecast => {
				if (forecast.area === nearestArea) {
					returnResult.forecast = forecast.forecast;
					// Weather icon
					if (forecast.forecast.toLowerCase().indexOf('cloudy') > -1) {
						// returnResult.icon = fs.readFileSync(path.join(__dirname, '../public/weather/weather_cloudy.png'), 'base64')
						// returnResult.icon = "iVBORw0KGgoAAAANSUhEUgAAADAAAAAwCAYAAABXAvmHAAAAAXNSR0IArs4c6QAABExJREFUaEPtmH3onWMYxz/fP4gwEdG8ZkjzspZMXiMzbGYRs6Ww0kILZfyx5CUxEZp3I69/GP7A2iwyRhJJIZIyamjNyx8yL0V9df26n5/7PL/nnOec7ZzzO786V51Oz3Pdz3Vf3/u67utNTHDSBNefIYDxtuDQAkMLbOcJDIwL2d4dmF7Cs0nSt60wjjsA23OBC4DLmyj6MrBS0ltV/HEDYPsY4F5gZpte9B7wkKQANEo9BWD7OOBY4GhgKvAu8AEwBbgD2AP4AlgNvC1pfa6c7QB3SfrtkHjzcxA9AWA73OEiYHbN6X4FnFvn57YPAh4AziuD6CqAdBHvBxaljX4CPgTi/0dgBnAiEBe2oNWS5tW5UZIdljo1rZ0j6fWuAbB9GvBOpsgKYIWk78rK2T4BuBk4O+NNk/R5KyC2DwFeA44CnpG0qG0Ats8HpiVfDn/8PvxX0krbJwOvAHslBWaW/blKMdvXAWGxoC2S9m3DEjcBtwN/APvUAqhwi/IenwBxyhcmxiRJv9cpUvBt7wn8mp6vl3RfjRUOTvvFssUtAdiOyPFlJjBO+QfgX2D/5AK7ZfyFkla1q3wGYgHwQno+RdL7NSDWpgCxoQ7AJuCAJGy6pE9zwbbjUt4JnAHcJunWTpXPQDyVLv9jkq6qARD73AI0B2D7QWBJEjRZ0uZmQiMp1V3AOmC2FwOPR56QdFI3AHwNHA40JI46RbaVn0WxrZJytxwj0nZrC9iOaPJz+nJKXaLZVqVL7rh3yhfxeqqkSHKVZHsNMAd4pPIO5DFdUm2k6gaAkGHbSdbpkjZUyU1RK9x5R+CcWgDAYZK+6ZaSNb5dAHgSuEvSxvJ62zcAdycPObABgO2IODdmlze+HxN9egXGdhR6kaULaohItvdLYTz4D0taMgogxfyXgCMzASPpulcKV5xu5J35Kb8cn/gjiiYX+wyIMnyzpMnxbgRAhfLXAOv65TpNfL2INMF+HoiCb1JaO1vSuhxAnHyUv0F9CZvtWDULl/nyyyQ9V7xQqVxYKim6pIEg2xFpIpzHyUelOkvSlly5AFCYar2kdtu7vgG0Ha7zagCQFNVwAwWAwn2ulRRdz0CR7SOAIqntKinK6FEKAFHXR2U5T1J0PANFtncC/kpKzZD0cRlAUXEukPTiQGn/f4QsSvoxRWVYIOrwqMeXS1o2gABiQPA0sFHSoVV3YClwD/CmpLMGEMATwBXAKkkLqwDMAt5IjEclXT0oIGxHo1RM5C6VFAmtMQrFk+1oAy9OnDObjfH6CayUn5oebF4LFZVg6LlM0vJ+KlzslUaO0VJeme2/s6S/q/RpVcz9mbLgL0DbU4btAL1LGtnEf0Ex8Zjbqp0d0w+ksWA0zDG+GA/6B4hY/2zMnOoUaNbQRPKIKVjxi9lNr+k34KNUMmxtd7O+tYvtKtTpuiGATk+s2+uHFuj2iXYqb2iBTk+s2+snvAX+A4v7kXZII+M2AAAAAElFTkSuQmCC"
						returnResult.label = conf.Weather_Forecast_Label.W1
					} else if (forecast.forecast.toLowerCase().indexOf('showers') > -1) {
						// returnResult.icon = fs.readFileSync(path.join(__dirname, '../public/weather/weather_shower.png'), 'base64')
						// returnResult.icon = "iVBORw0KGgoAAAANSUhEUgAAADAAAAAwCAYAAABXAvmHAAAAAXNSR0IArs4c6QAABSxJREFUaEPtmmmsXVMUx3//D4ZEiJhCiUQQU8QQUytNVAhaxdPwAZUaiphiHhIJDUGNjaEfOkQr+oFWW1MkGtoIKqmhoUSNTYiSBjFFaOQv63WfZ7/rnnvPee++966wkpv74ey91/+/9zprr+GIYRDbxwPxOwDYHxgFfA2sB54FXpL0zkCgaCCTqs6xfRtwCnBIhTnPAbMkPV9hbN+QISFgeztgLnBaBmYZsBZ4P+3+wcBBQPzvlY2bLemiqiQ6TsD2McDyDEDbnbV9KfAAsEWat0LSuCokahGwvStwZPodAbyZdnS5pPUJ/DPANkn5dZLurwLE9s7AC8Chafw0SWGCLaUyAdtXAA+VrBYv5GzgKOCENGaypCfaAWh8bjt0hK6QKyU93GqNSgRsh0mEaRTyC/BW8ia7AFs3KKm0e2XAbH+S3otvgLGSPi0b25aA7Y+AfdICs4D5kt7IF7R9J3BzXfttQeBY4OX0/EFJ1wyIgO3pwA1p8nhJL7ZQejJwLnCtpC/rmk4TU1qSvNhKSWNqE7AdphG2HRL++eLBgqoz3/btwC0xR1KppZQ+sD0+eYWfJRVepQ6GQY21fTrwdFpkVHi5Zgu2IhAu7FbgY0nFOzAoUHUmJ5f9VZpzmKS36xIoPE/lS6UOwHZjbW8L/JDGjZO0oi6B8OsXAusk7dFOYaef244Q49207nmS5lUmkIKwG4Et271EnQZerGc7otYPsvXXATMl3Zvr7PcO2D4DmAbslw1aKOnMoQLaal3bk4FLgNyNzpUUltErfQQS+Kdy4ECAXzgS4HOdtnvSfRShSsh0STf1EbAdl1BEjSHfAhdIisCqa8R2RKozgfMTqEmSFveegO3Y+TCfcFsTJL3XNcgbgNheBEwCer2jbI8Gitjmekn3dSv4tNljgVcTxqlBoIh3IicdLemPbiaQSERMdiIwLwgsBuIleURSEYd3NQfb81PguCoIRKy9Z1xakiKP7XqxfRcQXujXIPBjSgF7JC3tevSbnM7jQNwRq4NAVAuOA2ZIuvpfQiBy8cjNZwSB4jhWS6pSvxlxjra/A6J00xMEzgaK5PtySY+OOMIWAFKcFmH+F1HBCALBJI5kb2CjpM27lYDtCUBRuTtH0oLiJs5PYRVwalkGNFLksp0PCFFL7S3f5MFcmFEQKSSi0ii8rhmpy832ZqkgHFWJ8DqFbC/p+34E0g03BXisYZc3AnFXRJA3nLJDKucEiUJeAU7KN/QfOXGqRtyRRX3DCbqZrt9SZjZHUuPm/m1CjTNt7wTsnv0iRx1Oicg4Usow4d/LFLetzA0n4oHo+p/AQHatk3P+eydg+3BgiqTLOrmTtsPDRCW6Vjpb6wRsbwW8lvpaUyXN6QQJ21cF+MhzgaiCh+usJHUJFJFrLL4hpaCflWmyvSPwZ3FrNhtnO5KplUCMDblbUtFraEuiMgHb0buK5L9oxMXiUSkrNaXU2dnQqjBmO6LfaPIVEj5/TNW+cR0CZwELGrZkraR9S3a2qG7H49KWk+01Kd7JlymthTbqqkMgB9S3TrPmQ6prhlkUfYWfkrl92AjAtptsQOUeWx0C0SOIflkuTeumWZaXj71HUhSM+0lDG6t4VtoPGPAJxMRkr1HBixcuuidPNqudZpW+XN9SSVG+aSQQnyJELh5d0KhAL2nV1BsUgUQiMrbdJH3ezPbTmGbm1rLPZvvAOGFJEb5XlsomVHnFTSd1dLov8mkT637IUUXnkBBIpxAV74npk5plkl6vAqjumL8Akinip8Ft3/cAAAAASUVORK5CYII="
						returnResult.label = conf.Weather_Forecast_Label.W2
					} else if (forecast.forecast.toLowerCase().indexOf('rain') > -1) {
						// returnResult.icon = fs.readFileSync(path.join(__dirname, '../public/weather/weather_shower.png'), 'base64')
						// returnResult.icon = "iVBORw0KGgoAAAANSUhEUgAAADAAAAAwCAYAAABXAvmHAAAAAXNSR0IArs4c6QAABSxJREFUaEPtmmmsXVMUx3//D4ZEiJhCiUQQU8QQUytNVAhaxdPwAZUaiphiHhIJDUGNjaEfOkQr+oFWW1MkGtoIKqmhoUSNTYiSBjFFaOQv63WfZ7/rnnvPee++966wkpv74ey91/+/9zprr+GIYRDbxwPxOwDYHxgFfA2sB54FXpL0zkCgaCCTqs6xfRtwCnBIhTnPAbMkPV9hbN+QISFgeztgLnBaBmYZsBZ4P+3+wcBBQPzvlY2bLemiqiQ6TsD2McDyDEDbnbV9KfAAsEWat0LSuCokahGwvStwZPodAbyZdnS5pPUJ/DPANkn5dZLurwLE9s7AC8Chafw0SWGCLaUyAdtXAA+VrBYv5GzgKOCENGaypCfaAWh8bjt0hK6QKyU93GqNSgRsh0mEaRTyC/BW8ia7AFs3KKm0e2XAbH+S3otvgLGSPi0b25aA7Y+AfdICs4D5kt7IF7R9J3BzXfttQeBY4OX0/EFJ1wyIgO3pwA1p8nhJL7ZQejJwLnCtpC/rmk4TU1qSvNhKSWNqE7AdphG2HRL++eLBgqoz3/btwC0xR1KppZQ+sD0+eYWfJRVepQ6GQY21fTrwdFpkVHi5Zgu2IhAu7FbgY0nFOzAoUHUmJ5f9VZpzmKS36xIoPE/lS6UOwHZjbW8L/JDGjZO0oi6B8OsXAusk7dFOYaef244Q49207nmS5lUmkIKwG4Et271EnQZerGc7otYPsvXXATMl3Zvr7PcO2D4DmAbslw1aKOnMoQLaal3bk4FLgNyNzpUUltErfQQS+Kdy4ECAXzgS4HOdtnvSfRShSsh0STf1EbAdl1BEjSHfAhdIisCqa8R2RKozgfMTqEmSFveegO3Y+TCfcFsTJL3XNcgbgNheBEwCer2jbI8Gitjmekn3dSv4tNljgVcTxqlBoIh3IicdLemPbiaQSERMdiIwLwgsBuIleURSEYd3NQfb81PguCoIRKy9Z1xakiKP7XqxfRcQXujXIPBjSgF7JC3tevSbnM7jQNwRq4NAVAuOA2ZIuvpfQiBy8cjNZwSB4jhWS6pSvxlxjra/A6J00xMEzgaK5PtySY+OOMIWAFKcFmH+F1HBCALBJI5kb2CjpM27lYDtCUBRuTtH0oLiJs5PYRVwalkGNFLksp0PCFFL7S3f5MFcmFEQKSSi0ii8rhmpy832ZqkgHFWJ8DqFbC/p+34E0g03BXisYZc3AnFXRJA3nLJDKucEiUJeAU7KN/QfOXGqRtyRRX3DCbqZrt9SZjZHUuPm/m1CjTNt7wTsnv0iRx1Oicg4Usow4d/LFLetzA0n4oHo+p/AQHatk3P+eydg+3BgiqTLOrmTtsPDRCW6Vjpb6wRsbwW8lvpaUyXN6QQJ21cF+MhzgaiCh+usJHUJFJFrLL4hpaCflWmyvSPwZ3FrNhtnO5KplUCMDblbUtFraEuiMgHb0buK5L9oxMXiUSkrNaXU2dnQqjBmO6LfaPIVEj5/TNW+cR0CZwELGrZkraR9S3a2qG7H49KWk+01Kd7JlymthTbqqkMgB9S3TrPmQ6prhlkUfYWfkrl92AjAtptsQOUeWx0C0SOIflkuTeumWZaXj71HUhSM+0lDG6t4VtoPGPAJxMRkr1HBixcuuidPNqudZpW+XN9SSVG+aSQQnyJELh5d0KhAL2nV1BsUgUQiMrbdJH3ezPbTmGbm1rLPZvvAOGFJEb5XlsomVHnFTSd1dLov8mkT637IUUXnkBBIpxAV74npk5plkl6vAqjumL8Akinip8Ft3/cAAAAASUVORK5CYII="
						returnResult.label = conf.Weather_Forecast_Label.W2
					} else {
						// returnResult.icon = fs.readFileSync(path.join(__dirname, '../public/weather/weather_sunny.png'), 'base64')
						// returnResult.icon = "iVBORw0KGgoAAAANSUhEUgAAADAAAAAwCAYAAABXAvmHAAAAAXNSR0IArs4c6QAABI9JREFUaEPtmWvo3mMYxz/fckqSZA1FlEIK5VSUjCE21srIXjifyTZFctpmWeaFLUbYNJKSvbExW40Mm0M5TU0r3kgOQ0h54dRXF/ev7p49z/O772f/5/mn/tfL3++6r+v6Xvd139fhFkMk228Df0o6c1hqNCzBts8A3kjy50laNgxdEwB6eXViBwrjbVQhdKmkFwptqmLrC8D2SmC5pE+qpCZm26uAnyXdNuD644Glkqb0Wt8TgO1ngMuB34FZkl4exIhB19i+AFgN7Am8JGlmN1n9ANwFPJAWjRREh/FhwiJJ91UBCGbbDwPzRgmii/ERQj1DsPUQd4D4TdI+fa7OPYCzgVOBycCBifc7YAfwFvCupF/7yPgcOCL972t88LQCyHbiVuArSYd3Krd9CHAHMBvYvyXu/wBeBRZ2uxxsvwecADxacviLAPQzyPYtyfgAkdM24H3gb+Bk4LiO/78kELtUYuwSANuhfE5m2ELg4xQm3+cG2z4gATkJWJD9Wy/p/EFvq4EB2Ham9AvgCklbSgyxfSGwBDiq4Zc0kC0DLbK9FTg2KV8l6aoSw7ucnaXA3PR9oJ2oBmD7SeC6pHSDpPMGMb5ZY/sSoCkznpJ0fY28KgC243b4ICnYIam5Jmt07sRrezlwc/pxoqQPSwXWAsi9P2YFmu294+CnsKzahWIAtncHvgYmpesvv0lKHdaTz/aMqHmAn4CDJEW+aKUaANHXvp4kTpMUyWjMyPahwJdJ4HRJ60qE1wCIwi4KvKBJkn4sUVDDY/ub8D6wWNLdJWtrAERvcDWwVVLU6WNOtt8ETgeelnRNiQLZjnQfqT6nv4CHci/YfgWYBqyQ1FyjJTqKeWw/DtwIrJM0vVloO3LOeuDgDmGrA0Ck/m4eXSLpzkzIeALIRzQ5hk0BIGIukklO2yRtzD+k9nLcQsh2OHm/Djs/qjkD//tDfC6wIXlg2NfoTEmRE1qpZgf2BX4AoutaIClK5zGjVKGuASKBTZYU/UIrFQMISbZDQZTCQTMkrW3VUMhgezNwGrBWUmTlIqoFcBbwWpK8XdLRRVpamGxHOxr9QdBUSU3GbxVfBSDtwgqgSTLLJDVTi1Zl3RhsHwlsT/9WSrq2RlA1gAQib2iulBRDsGrK4j7WjqahaazsaCmjsLtd0mclKGyfAjwIRIL6l0baUmYgwvC8Iytp6qNsmZ8B/VRS58SixA//AS/m7MHY0WLmXG1jleCtjvlOM4oA2J4FPJfGJTtNilOrGQXeRYWDrUiIj3S7bWzHs1RMK+ZIerHNwa0AbN8QUzJgt5hUS9qrl1DbYzFa/DaNJKMinivpsX4g2t4HoqlYlEItBM6WFCPvoZHti4Hnk8Ni9jRfUtjQlUrH6yHoJklPDM3yTLDtmFDErjf29Sxd+gF4Frgsyb1HUvNWMAoMUbbcC9yflG2UdE43xf0AxMRsKvCOpMWDWG37sAgFSTF6rKYEIoYJa3q9M7ce4mqtaUFKVjEqDzqmNMnV6hsmgLwNnCJpU61xJfwTAPrkhIkdGNcQCuWpToqHwYEeuksA/AMCU/GumR9xyAAAAABJRU5ErkJggg=="
						returnResult.label = conf.Weather_Forecast_Label.W1
					}

					if (returnResult.forecast.toLowerCase().indexOf('thundery') > -1 
					|| returnResult.forecast.toLowerCase().indexOf('heavy') > -1
					) {
						returnResult.bgColor = 'red'
					} else {
						returnResult.bgColor = '#CC154536'
					}

					return true;
				}
			})
			log.info(`getWeatherForecast Result => ${JSON.stringify(returnResult)}`)
			latestReturnResult = returnResult;
			return res.json(utils.response(1, returnResult));
		} catch (error) {
			log.error(error)
			return res.json(utils.response(0, error));
		}
	},
	getDriverMileage: async function (req, res) {
		let userId = req.body.userId;
		let user = await User.findByPk(userId);
		if (!user) {
			throw new Error(`UserId ${userId} do not exist.`)
		}
		let driver = await Driver.findByPk(user.driverId);
		if (!driver) {
			throw new Error(`DriverId ${user.driverId} do not exist.`)
		}
		//  else if (!driver.permitType) {
		// 	log.warn(`Driver ${driver.driverName} do not has permitType.`)
		// }
		log.info(`DriverID ${user.driverId} has permitType => ${driver.permitType}`)
		let result = { safetyScore: 100, totalMileage: 0, permitData: [] }

		let statResult = [];
		let permitTypes = new Set();
		let driverTotalMileage = 0;
		// Real-time mileage statistics
		let driverPermitTaskMileageList = await sequelizeObj.query(` 
			SELECT veh.permitType, sum(m.mileageTraveled) as permitMileage
			FROM mileage m
			LEFT JOIN (
				select v1.vehicleNo, v1.permitType from vehicle v1
				UNION ALL
				select v2.vehicleNo, v2.permitType from vehicle_history v2
			) veh ON m.vehicleNo = veh.vehicleNo
			WHERE m.driverId = ${user.driverId} and m.endMileage IS NOT NULL 
			GROUP BY veh.permitType
		`, { 
			type: QueryTypes.SELECT, replacements: []
		});
			
		let driverMileageStatList = await sequelizeObj.query(`
			SELECT
				dm.permitType,
				dm.passDate,
				dm.baseMileage
			FROM driver_permittype_detail dm
			where dm.driverId=? AND dm.approveStatus='Approved' ORDER BY dm.permitType asc
		`, { type: QueryTypes.SELECT, replacements: [ user.driverId ] });
		for (let permitTypeMileage of driverMileageStatList) {
			permitTypes.add(permitTypeMileage.permitType);
		}
		for (let permitType of permitTypes) {
			let newPermitType = permitType
			let driverPermitTypeTaskMileage = driverPermitTaskMileageList.find(item => item.permitType == permitType);
			let driverPermitTypeBaseMileage = driverMileageStatList.find(item => item.permitType == permitType);

			let totalMileage = 0;
			if (driverPermitTypeTaskMileage) {
				totalMileage += driverPermitTypeTaskMileage.permitMileage ? driverPermitTypeTaskMileage.permitMileage : 0;
			}
			if (driverPermitTypeBaseMileage) {
				totalMileage += driverPermitTypeBaseMileage.baseMileage ? driverPermitTypeBaseMileage.baseMileage : 0;
			}

			let permitTypeConf = await PermitType.findOne({ where: { permitType : newPermitType} });
			if (permitTypeConf?.parent) {
				let parentPermitType = permitTypeConf.parent;
				let parentMileageObj = statResult.find(item => item.permitType == parentPermitType);
				if (parentMileageObj) {
					parentMileageObj.totalMileage += totalMileage;
					continue;
				} else {
					newPermitType = parentPermitType;
					permitTypeConf = await PermitType.findOne({ where: { permitType: newPermitType} });
				}
			} else {
				continue;
			}

			let eligibilityMileage = permitTypeConf?.eligibilityMileage ? permitTypeConf.eligibilityMileage : 4000;
			statResult.push({permitType: newPermitType, totalMileage, eligibilityMileage});
		}

		for (let temp of statResult) {
			let moreMileage = temp.totalMileage < temp.eligibilityMileage ? temp.eligibilityMileage - temp.totalMileage : 0;
			temp.moreMileage = moreMileage;
			driverTotalMileage += temp.totalMileage;
		}

		result.totalMileage = driverTotalMileage ? driverTotalMileage.toFixed(2) : 0;
		result.permitData = statResult
		
		log.info(JSON.stringify(result, null, 4))
		
		return res.json(utils.response(1, result));
	},
	getTOIndents: async function (req, res) {
		let userId = req.body.userId;
		let user = await User.findByPk(userId);
		if (!user) {
			throw new Error(`UserId ${userId} do not exist.`)
		}

		// log.info(`getTOIndents API => ${ conf.URL_Of_3rd_List.GetTOIndents }`)
		// let result = await axios.post(conf.URL_Of_3rd_List.GetTOIndents, {
		// 	driverId: user.driverId
		// }).then(result => result.data);
		let result = await mobileTOService.GETDriverTaskList(user.driverId)
		log.info(`getTOIndents API Data => ${ JSON.stringify(result) }`)

		if (result.code === 1) {
			for (let indent of result.data.indents) {
				log.info(`********************************************`)
				log.info(`Find Data by indent type => ${ indent.name }`)
				for (let task of indent.dataList) {
					log.info(`Start  find ODD data by taskId => ${ task.taskId }`)
					let odd = await ODD.findOne({ where: { taskId: task.taskId }, order: [ [ 'updatedAt', 'DESC' ] ]})
					task.odd = odd ? odd.content : '';
					log.info(`Finish find ODD data by taskId => ${ task.taskId }`)

					log.info(`Start  find MT RAC data by taskId => ${ task.taskId }`)
					// let assignedTask = await Task.findOne({ where: { taskId: task.taskId }});
					let mtRAC = await MT_RAC.findOne({ where: { taskId: task.taskId }, order: [ [ 'id', 'DESC' ] ] })
					task.commanderContact = (mtRAC?.needCommander) ? { username: mtRAC.commander, contact: mtRAC.commanderContactNumber, hub: task.hub, node: task.node } : null;
					log.info(`Finish find MT RAC data by taskId => ${ task.taskId }`)

					log.info(`Start  find Mileage data by taskId => ${ task.taskId }`)
					let mileage = await Mileage.findByPk(task.taskId)
					if (mileage) {
						task.startMileage = mileage.startMileage;
						task.endMileage = mileage.endMileage;
					} else {
						let vehicle = await Vehicle.findByPk(task.vehicleNumber)
						task.startMileage = (vehicle?.totalMileage) ? vehicle.totalMileage : 0;
						task.endMileage = 0;
					}
					log.info(`Finish find Mileage data by taskId => ${ task.taskId }`)

					log.info(`Start  find CheckList data by taskId => ${ task.taskId }`)
					let checkList = await CheckList.findAll({ where: { taskId: task.taskId } })
					let checkList1 = checkList.find(item => item.checkListName == CHECKLIST[1])
					let checkList2 = checkList.find(item => item.checkListName == CHECKLIST[2])
					let checkList4 = checkList.find(item => item.checkListName == CHECKLIST[4])
					let checkList5 = checkList.find(item => item.checkListName == CHECKLIST[5])

					task.taskReady = false;

					const updateStatus = async function () {
						if (checkList1 && checkList2 && checkList4) {
							if (task.purposeType && task.purposeType.toLowerCase() == 'wpt') {
								task.taskReady = true;

								await Task.update({ driverStatus: 'Ready', vehicleStatus: 'Ready' }, { where: { taskId: task.taskId, driverStatus: 'waitcheck' } })
							} else if (checkList5) {
								task.taskReady = true;

								if (task.taskId.startsWith('DUTY')) {
									await UrgentIndent.update({ status: 'Ready' }, { where: { dutyId: task.taskId.split('-')[1], status: 'waitcheck' } })
									await UrgentDuty.update({ status: 'Ready' }, { where: { dutyId: 'DUTY-' + task.taskId.split('-')[1], status: 'waitcheck' } })
								} else {
									await Task.update({ driverStatus: 'Ready', vehicleStatus: 'Ready' }, { where: { taskId: task.taskId, driverStatus: 'waitcheck' } })
								}
							} 
						}
					}
					await updateStatus()
					
					log.info(`Finish find CheckList data by taskId => ${ task.taskId }`)

					// need unique taskId while urgent task(one duty may has 0-3 indents, if > 0, generate new taskId with indentId, mobile need unique taskId)
					if (task.dataFrom == 'Urgent' && task.indentId) {
						task.taskId += '-' + task.indentId 
					}
				}
			}
			log.info(`result.data => `, JSON.stringify(result.data))
			return res.json(utils.response(1, result.data));
		} else {
			return res.json(utils.response(0, result.msg));
		}
	},
	getTOIndentByTaskId: async function (req, res) {
		let taskId = req.body.taskId;
		let task0 = null, result = null
		if (taskId.startsWith('DUTY')) {

			let temp = taskId.split('-')
			taskId = `DUTY-${ temp[1] }` 
			let indentId = temp[2]

			task0 = await urgentService.getDutyById(taskId)
			let duty = await urgentService.getDutyDetailById(taskId, indentId)
			result = {
				"code": 1,
				"message": "success",
				"data": duty,
			}
		} else {
			task0 = await Task.findOne({ where: { taskId } })
			result = await mobileTOService.GETDriverTaskByTaskId(taskId);
		}

		log.info(`GETDriverTaskByTaskId API Data => ${JSON.stringify(result)}`)

		if (result.code === 1) {
			let task = result.data;
			//let vehicleRelation = await VehicleRelation.findOne({ where: { driverId: task.driverId, vehicleNo: task.vehicleNumber } })
			task.limitSpeed = task.limitSpeed ? task.limitSpeed : 60
			
			let odd = await ODD.findOne({ where: { taskId: task.taskId }, order: [ [ 'updatedAt', 'DESC' ] ]})
			task.odd = odd ? odd.content : '';

			let assignedTask = null
			if (taskId.startsWith('DUTY')) {
				assignedTask = task0
			} else {
				assignedTask = await Task.findOne({ where: { taskId: task.taskId }});
			}
			let mtRAC = await MT_RAC.findOne({ where: { taskId: taskId }, order: [ [ 'id', 'DESC' ] ] })
			task.commanderContact = (mtRAC?.needCommander) ? { username: mtRAC.commander, contact: mtRAC.commanderContactNumber, hub: assignedTask.hub, node: assignedTask.node } : null;

			let mileage = await Mileage.findByPk(task.taskId)
			if (mileage) {
				task.startMileage = mileage.startMileage;
				task.endMileage = mileage.endMileage;
			} else {
				let vehicle = await Vehicle.findByPk(task.vehicleNumber)
				task.startMileage = vehicle ? vehicle.totalMileage : 0;
				task.endMileage = 0;
			}

			let checkList = await CheckList.findAll({ where: { taskId: taskId } })
			let checkList1 = checkList.find(item => item.checkListName == CHECKLIST[1])
			let checkList2 = checkList.find(item => item.checkListName == CHECKLIST[2])
			let checkList4 = checkList.find(item => item.checkListName == CHECKLIST[4])
			let checkList5 = checkList.find(item => item.checkListName == CHECKLIST[5])

			task.taskReady = false;
			const updateStatus = async function () {
				if (checkList1 && checkList2 && checkList4) {
					if (task.purposeType && task.purposeType.toLowerCase() == 'wpt') {
						task.taskReady = true;

						await Task.update({ driverStatus: 'Ready', vehicleStatus: 'Ready' }, { where: { taskId: task.taskId, driverStatus: 'waitcheck' } })
					} else if (checkList5) {
						task.taskReady = true;
						let a = '';
						if (task.taskId.startsWith('DUTY')) {
							await UrgentIndent.update({ status: 'Ready' }, { where: { dutyId: task.taskId.split('-')[1], status: 'waitcheck' } })
							await UrgentDuty.update({ status: 'Ready' }, { where: { dutyId: 'DUTY-' + task.taskId.split('-')[1], status: 'waitcheck' } })
						} else {
							await Task.update({ driverStatus: 'Ready', vehicleStatus: 'Ready' }, { where: { taskId: task.taskId, driverStatus: 'waitcheck' } })
						}
					} 
				}
			}
			await updateStatus()
			
			log.info(`task => `, JSON.stringify(task, null, 4))

			// need unique taskId while urgent task(one duty may has 0-3 indents, if > 0, generate new taskId with indentId, mobile need unique taskId)
			if (task.dataFrom == 'Urgent' && task.indentId) {
				task.taskId += '-' + task.indentId 
				task.startLateReason = 'urgent;'
			}

			return res.json(utils.response(1, task));
		} else {
			return res.json(utils.response(0, result.msg));
		}
	},
	startTask: async function (req, res) {
		try {
			let { taskId, userId, vehicleNo, startMileage, mobileStartTime, lateStartRemarks } = req.body;
			let user = await User.findByPk(userId);
			if (!user) {
				log.warn(`UserId ${userId} do not exist.`)
				throw new Error(`UserId ${userId} do not exist.`)
			}
			if (!startMileage) startMileage = 0;
			// let result = await axios.post(conf.URL_Of_3rd_List.StartTask, {
			// 	driverId: user.driverId,
			// 	taskId,
			// 	taskStatus: 'Arrived',
			// 	driverStatus: 'Arrived',
			// 	arrivalTime,
			// }).then(result => result.data);
			let result = await mobileTOService.startTask({
				driverId: user.driverId,
				taskId,
				taskStatus: 'Started',
				driverStatus: 'Started',
				mobileStartTime,
			})
			if (result.code === 1) {
				// Update Mileage
				let mileage = await Mileage.findByPk(taskId);
				if (!mileage) {
					// Maybe first time, there is no record
					await Mileage.create({
						taskId,
						driverId: user.driverId,
						vehicleNo,
						date: mobileStartTime,
						startTime: mobileStartTime,
						startMileage: startMileage
					})
				} else {
					await Mileage.update({
						driverId: user.driverId,
						vehicleNo,
						startTime: mobileStartTime,
						startMileage: startMileage
					}, { where: { taskId } })
				}

				if (lateStartRemarks) {
					await Task.update({ lateStartRemarks }, { where: { taskId } })
				}
				return res.json(utils.response(1, { mobileStartTime }));
			} else if (result.code === 2) {
				// already started, ignore this request.(maybe offline request)
				return res.json(utils.response(1, { mobileStartTime }));
			} else {
				return res.json(utils.response(0, result.msg));
			}
		} catch (error) {
			log.error(error);
			return res.json(utils.response(0, error));
		}
	},
	startTask2: async function (req, res) {
		let { taskId, userId, vehicleNo, startMileage, mobileStartTime, lateStartRemarks } = req.body;

		mobileStartTime = mobileStartTime.replace(/[^a-zA-Z0-9: ]/g,  ' ')
		mobileStartTime = mobileStartTime.trim()
		mobileStartTime = moment(mobileStartTime).format('YYYY-MM-DD HH:mm:ss')

		let user = await User.findByPk(userId);
		if (!user) {
			log.warn(`UserId ${userId} do not exist.`)
			throw new Error(`UserId ${userId} do not exist.`)
		}
		if (!startMileage) startMileage = 0;

		if (!taskId) {
			log.warn(`TaskId ${taskId} can not be null.`)
			throw new Error(`TaskId ${taskId} can not be null.`)
		}

		log.info(`UserId ${userId} exe startTask taskId: ${taskId}`)
		
		await sequelizeObj.transaction(async (t1) => {
			
			let checkList = await CheckList.findAll({ where: { taskId: taskId } })
			let checkList1 = checkList.find(item => item.checkListName == CHECKLIST[1])
			let checkList2 = checkList.find(item => item.checkListName == CHECKLIST[2])
			let checkList4 = checkList.find(item => item.checkListName == CHECKLIST[4])
			let checkList5 = checkList.find(item => item.checkListName == CHECKLIST[5])
			let tempTask = null, indentId = null

			if (taskId.startsWith('DUTY')) {
				let temp = taskId.split('-')
				taskId = `DUTY-${ temp[1] }` 
				indentId = temp[2]

				tempTask = await urgentService.getDutyById(taskId)

				if (checkList1 && checkList2 && checkList4 && checkList5) {
					await UrgentIndent.update({ status: 'Ready' }, { where: { dutyId: temp[1], status: 'waitcheck' } })
					await UrgentDuty.update({ status: 'Ready' }, { where: { dutyId: 'DUTY-' + temp[1], status: 'waitcheck' } })
				}

				tempTask = await urgentService.getDutyById(taskId) 
			} else {
				tempTask = await Task.findByPk(taskId);

				if (checkList1 && checkList2 && checkList4) {
					if (tempTask.purposeType && tempTask.purposeType.toLowerCase() == 'wpt') {
						await Task.update({ driverStatus: 'Ready', vehicleStatus: 'Ready' }, { where: { taskId, driverStatus: 'waitcheck' } })
					} else if (checkList5) {
						await Task.update({ driverStatus: 'Ready', vehicleStatus: 'Ready' }, { where: { taskId, driverStatus: 'waitcheck' } })
					} 
				}

				tempTask = await Task.findByPk(taskId);
			}

			if (!tempTask) {
				log.warn(`TaskId ${taskId} do not exist.`)
				throw new Error(`TaskId ${taskId} do not exist.`)
			}
	
			// Task already started
			if (tempTask.mobileStartTime) {
				log.warn(`TaskID => ${ taskId }: has already started, ignore this request.`)
				return res.json(utils.response(1, { mobileStartTime: tempTask.mobileStartTime }));
			}
			if (tempTask.driverStatus.toLowerCase() != 'ready') {
				log.warn(`TaskID => ${ taskId }: Task status is not ready, can not started.`)
				return res.json(utils.response(1, { mobileStartTime: null }));
			}

			if (tempTask.dataFrom == 'SYSTEM') {
				let systemTaskId = taskId;
				if (systemTaskId?.startsWith('AT-')) {
					systemTaskId = systemTaskId.replace('AT-', '');
				}
				await sequelizeSystemObj.transaction(async (t1) => {
					log.info(`UserId ${userId} exe startTask taskId: ${systemTaskId}, task is system, start update systemTask info.`)
					let sysTask = await _SystemTask.Task.findByPk(systemTaskId);
					if (!sysTask) {
						log.warn(`SystemTask TaskId ${systemTaskId} do not exist.`)
						throw new Error(`SystemTask TaskId ${systemTaskId} do not exist.`);
					}
					sysTask.taskStatus = 'Started';
					sysTask.mobileStartTime = mobileStartTime;
					await sysTask.save();
		
					// 2 update driver status
					let driver = await _SystemDriver.Driver.findByPk(systemTaskId);
					driver.status = 'Started';
					await driver.save();

					await _SystemJob.OperationHistory.create({
						requestId: sysTask.requestId,
						tripId: sysTask.tripId,
						taskId: sysTask.id,
						status: 'Started',
						action: 'Started',
						remark: driver.name
					});
				}).catch(error => {
					log.info(`UserId ${userId} exe startTask taskId: ${taskId}, update systemTask fail, errorMsg: ${error && error.message ? error.message : ''}`);
					throw error
				})
			}

			log.info(`UserId ${userId} exe startTask taskId: ${taskId}, start update mobiusTask info.`)

			let task = null
			if (taskId.startsWith('DUTY')) {
				task = await urgentService.getDutyById(taskId)
				let indent = await UrgentIndent.findByPk(indentId)
				await sequelizeObj.query(`
					UPDATE urgent_duty SET \`status\` = ?, mobileStartTime = ? 
					${ lateStartRemarks ? `, lateStartRemarks='${ lateStartRemarks }'` : '' } 
					WHERE dutyId = ? and status != 'cancelled'
				`, { type: QueryTypes.UPDATE, replacements: [ 'Started', mobileStartTime, taskId ] })
				await sequelizeObj.query(`
					UPDATE urgent_indent SET \`status\` = ?, mobileStartTime = ? WHERE id = ? and status not in ('cancelled', 'completed')
				`, { type: QueryTypes.UPDATE, replacements: [ 'Started', mobileStartTime, indentId ] })

				// Update system data
				await sequelizeSystemObj.transaction(async t => {
					await sequelizeSystemObj.query(`
						UPDATE job SET \`status\` = ? WHERE requestId IN (?) and status != 'cancelled'
					`, { type: QueryTypes.UPDATE, replacements: [ 'Started', [indent.requestId] ]})
					await sequelizeSystemObj.query(`
						UPDATE job_task SET \`taskStatus\` = ?, mobileStartTime = ?  WHERE requestId IN (?) and taskStatus != 'cancelled'
					`, { type: QueryTypes.UPDATE, replacements: [ 'Started', mobileStartTime, [indent.requestId] ]})
					await sequelizeSystemObj.query(`
						UPDATE driver SET \`status\` = ?  WHERE taskId IN (?)
					`, { type: QueryTypes.UPDATE, replacements: [ 'Started', indentId ]})
				})
			} else {
				task = await Task.findByPk(taskId);
				task.mobileStartTime = mobileStartTime;
				task.driverStatus = "started"
				task.vehicleStatus = "started"
				if (lateStartRemarks) {
					task.lateStartRemarks = lateStartRemarks
				}
				await task.save();
			}

			// Update Mileage
			let mileageTaskId = taskId + (indentId ? ('-' + indentId) : "");
			let mileage = await Mileage.findByPk(mileageTaskId);
			if (!mileage) {
				log.info(`UserId ${userId} exe startTask taskId: ${taskId}, start create mileageObj.`)
				// Maybe first time, there is no record
				await Mileage.create({
					taskId: mileageTaskId,
					driverId: user.driverId,
					vehicleNo,
					date: mobileStartTime,
					startTime: mobileStartTime,
					startMileage: startMileage
				})
			} else {
				await Mileage.update({
					driverId: user.driverId,
					vehicleNo,
					startTime: mobileStartTime,
					startMileage: startMileage
				}, { where: { taskId: mileageTaskId } })
			}

			log.info(`UserId ${userId} exe startTask taskId: ${taskId}, start task success.`)
			return res.json(utils.response(1, { mobileStartTime }));
		}).catch(error => {
			log.info(`UserId ${userId} exe startTask taskId: ${taskId}, update mobiusTask fail, errorMsg: ${error && error.message ? error.message : ''}`);
			throw error
		})
	},
	endTask: async function (req, res) {
		try {
			let { taskId, userId, vehicleNo, endMileage } = req.body;
			let user = await User.findByPk(userId);
			if (!user) {
				log.warn(`UserId ${userId} do not exist.`)
				throw new Error(`UserId ${userId} do not exist.`)
			}
			let endTime = moment().format('YYYY-MM-DD HH:mm:ss')
			// let result = await axios.post(conf.URL_Of_3rd_List.EndTask, {
			// 	driverId: user.driverId,
			// 	taskId,
			// 	taskStatus: 'Completed',
			// 	driverStatus: 'Completed',
			// 	endTime,
			// }).then(result => result.data);
			let result = await mobileTOService.endTask({
				driverId: user.driverId,
				taskId,
				taskStatus: 'Completed',
				driverStatus: 'Completed',
				endTime,
			})


			if (result.code === 1) {
				let mileage = await Mileage.findByPk(taskId);
				mileage.endTime = endTime;
				mileage.endMileage = endMileage;
				mileage.mileageTraveled = endMileage - mileage.startMileage;
				await mileage.save();
				await sequelizeObj.query(` INSERT INTO mileage_history SELECT * FROM mileage WHERE taskId = ? `, { 
					type: QueryTypes.INSERT, replacements: [ mileage.taskId ]
				})

				log.info(`Start update driver mileage...`)
				await Driver.increment({ totalMileage: mileage.mileageTraveled }, { where: { driverId: user.driverId } })
				log.info(`Start update vehicle mileage...`)
				await Vehicle.update({ totalMileage: endMileage}, { where: { vehicleNo } })
				
				// childprocess: calculate 
				// distanceService.calculateMileageByTask({
				// 	driverId: user.driverId,
				// 	vehicleNo,
				// 	taskId,
				// 	startMileage: mileage.startMileage,
				// 	endMileage: mileage.endMileage,
				// 	timeZone: [moment(mileage.startTime).format('YYYY-MM-DD HH:mm:ss'), moment(mileage.endTime).format('YYYY-MM-DD HH:mm:ss')]
				// })

				return res.json(utils.response(1, 'success'));
			} else {
				return res.json(utils.response(0, result.msg));
			}
		} catch (error) {
			log.error(error);
			return res.json(utils.response(0, error));
		}
	},
	getTaskTrackingStatInfo: async function(req, res) {
		try {
			let { taskId, userId} = req.body;
			let user = await User.findByPk(userId);
			if (!user) {
				log.warn(`GetTaskTrackingStatInfo: UserId ${userId} do not exist.`)
				return res.json(utils.response(0, `UserId ${userId} do not exist.`));
			}
			let dataList = [];
			if (taskId.startsWith('DUTY')) {
				dataList = await sequelizeObj.query(`
					SELECT
						th.violationType, 
						count(th.violationType) as countNum
					FROM
						urgent_duty t
					LEFT JOIN track_history th on t.driverId = th.deviceId
					where t.dutyId=? and th.occTime BETWEEN t.mobileStartTime and t.mobileEndTime
					GROUP BY th.violationType
				`, { type: QueryTypes.SELECT, replacements: [taskId] })
			} else {
				dataList = await sequelizeObj.query(`
					SELECT
						th.violationType, 
						count(th.violationType) as countNum
					FROM
						task t
					LEFT JOIN track_history th on t.driverId = th.deviceId
					where t.taskId=? and th.occTime BETWEEN t.mobileStartTime and t.mobileEndTime
					GROUP BY th.violationType
				`, { type: QueryTypes.SELECT, replacements: [taskId] })
			}
			
			return res.json(utils.response(1, dataList));
		} catch (error) {
			log.error(`GetTaskTrackingStatInfo fail, errorMsg: ` + (error.message ? error.message : 'Get task tracking info fail!'))
			log.error(error);
			return res.json(utils.response(0, error.message ? error.message : 'Get task tracking info fail!'));
		}
	},
	checkTask: async function (taskId) {
		try {
			let task = await Task.findOne({ where: { taskId } })
			if (!task) {
				throw new Error(`TaskID ${ taskId } do not exist!`)
			} else {
				return task;
			}
		} catch (error) {
			throw error;
		}
	},
	updateTaskOptTime: async function (req, res) {
		let { userId, taskId, vehicleNo, serviceModeValue, operationTime, operationType, mileage, distance } = req.body;
		

		if (!taskId) {
			return res.json(utils.response(1, {}));
		}

		operationTime = moment(operationTime).format('YYYY-MM-DD HH:mm:ss')

		distance = distance / 1000; // km
		serviceModeValue = serviceModeValue.toLowerCase();
		operationType = operationType.toLowerCase();
		let user = await User.findByPk(userId);

		log.info(`UserId ${userId} exe updateTaskOptTime taskId: ${taskId}`)

		let task0 = null, indentId = null;
		const getTask = async function () {
			if (taskId.startsWith('DUTY')) {
				let temp = taskId.split('-')
				taskId = `DUTY-${ temp[1] }` 
				indentId = temp[2]
				task0 = await urgentService.getDutyById(taskId)
			} else {
				task0 = await Task.findByPk(taskId);
			}
		}
		await getTask();

		const checkTaskStatus = async function () {

			if (task0 && task0.dataFrom == 'SYSTEM') {
				log.info(`UserId ${userId} exe updateTaskOptTime taskId: ${taskId}, task is system, start update systemTask info.`)
	
				// Update 11-28
				if (!task0.mobileStartTime) {
					log.warn(`TaskID => ${ taskId }: has not started, can not ended.`)
					return res.json(utils.response(1, `TaskID => ${ taskId }: has not started, can not ended.`));
				}
	
				let systemTaskId = taskId;
				if (systemTaskId?.startsWith('AT-')) {
					systemTaskId = systemTaskId.replace('AT-', '');
				}
				
				let result = await mobileTOService.updateTaskOptTime({
					driverId: user.driverId,
					taskId: systemTaskId,
					serviceModeValue,
					operationTime,
					operationType,
				})
	
				if (result.code === 0) {
					log.info(`UserId ${userId} exe updateTaskOptTime taskId: ${taskId}, task is system, update systemTask fail: ${result.msg}`)
					return res.json(utils.response(0, result.msg));
				}
			} else {
				// While dataFrom "MT-ADMIN", change serviceModeValue to "XXXXXXXXXXXX", then will go Arrive > Depart > End
				serviceModeValue = 'XXXXXXXXXX'
			}
			
			// Task has both start time and end time, already finished
			if (task0.mobileEndTime && task0.mobileStartTime) {
				log.warn(`TaskID => ${ taskId }: has completed already.`)
				return res.json(utils.response(1, `TaskID => ${ taskId }: has completed already.`));
			}
			// Start 
			if (operationType == 'arrive') {
				log.warn(`TaskID => ${ taskId }: can not start here, should use startTask API.`)
				return res.json(utils.response(1, `TaskID => ${ taskId }: can not start here, should use startTask API.`));
	
				// if (task0.driverStatus.toLowerCase() !== 'ready') {
				// 	log.warn(`TaskID => ${ taskId }: has not ready, can not start.`)
				// 	return res.json(utils.response(1, `TaskID => ${ taskId }: has not ready, can not start.`));
				// } else {
				// 	// Already ready
				// 	if (task0.mobileStartTime) {
				// 		log.warn(`TaskID => ${ taskId }: has already started.`)
				// 		return res.json(utils.response(1, `TaskID => ${ taskId }: has already started.`));
				// 	} else {
				// 		// Check end time, while already exist, should after start time
				// 		if (task0.mobileEndTime) {
				// 			if (moment(operationTime).isBefore(task0.mobileEndTime)) {
				// 				// Continue
				// 			} else {
				// 				log.warn(`TaskID => ${ taskId }: mobile start time should before endTime.`)
				// 				return res.json(utils.response(1, `TaskID => ${ taskId }: mobile start time should before endTime.`));
				// 			}
				// 		} else {
				// 			// continue;
				// 		}
				// 	}
				// }
			}
			// End
			if (operationType == 'end') {
				if (task0.driverStatus.toLowerCase() !== 'started') {
					log.warn(`TaskID => ${ taskId }: has not started, can not ended.`)
					return res.json(utils.response(1, `TaskID => ${ taskId }: has not started, can not ended.`));
				} else {
					// Already started
					if (task0.mobileEndTime) {
						log.warn(`TaskID => ${ taskId }: has already ended.`)
						return res.json(utils.response(1, `TaskID => ${ taskId }: has already ended.`));
					} else {
						// Check start time, while already exist, should before end time
						if (task0.mobileStartTime) {
							if (moment(operationTime).isBefore(task0.mobileStartTime)) {
								log.warn(`TaskID => ${ taskId }: mobile end time ${ operationTime } should after start time ${ task0.mobileStartTime }.`)
								return res.json(utils.response(1, `TaskID => ${ taskId }: mobile end time should after start time.`));
							}
						} else {
							// Continue
						}
					}
				}
			}
		}
		await checkTaskStatus()

		await sequelizeObj.transaction(async (t1) => { 
			let isComplete = false;
			const checkTaskComplete = async function () {
				if (serviceModeValue === 'ferry service' && operationType === 'arrive') {
					// Task is completed here while serviceModeValue = 'ferry service'
					await Task.update({ mobileStartTime: operationTime }, { where: { taskId } })
	
					if (task0 && task0.dataFrom == 'MT-ADMIN') {
						await MtAdmin.update({ arrivalTime: operationTime }, { where: { id: taskId.split('-')[1] } })
					}
	
					isComplete = true;
				} else if (serviceModeValue === 'delivery') {
					if (operationType === 'arrive') {
						await Task.update({ mobileStartTime: operationTime }, { where: { taskId } })
						if (task0 && task0.dataFrom == 'MT-ADMIN') { 
							await MtAdmin.update({ arrivalTime: operationTime }, { where: { id: taskId.split('-')[1] } })
						}
					} else if (operationType === 'depart') {
						if (task0 && task0.dataFrom == 'MT-ADMIN') {
							await MtAdmin.update({ departTime: operationTime }, { where: { id: taskId.split('-')[1] } })
						}
	
						isComplete = true;
					}
				} else if (serviceModeValue === 'pickup') {
					if (operationType === 'arrive') {
						await Task.update({ mobileStartTime: operationTime }, { where: { taskId } })
						if (task0 && task0.dataFrom == 'MT-ADMIN') { 
							await MtAdmin.update({ arrivalTime: operationTime }, { where: { id: taskId.split('-')[1] } })
						}
					} else if (operationType === 'end') {
						if (task0 && task0.dataFrom == 'MT-ADMIN') { 
							await MtAdmin.update({ endTime: operationTime }, { where: { id: taskId.split('-')[1] } })
						}
	
						isComplete = true;
					}
				} else {
					// Attention
					if (operationType === 'arrive') {
						await Task.update({ mobileStartTime: operationTime }, { where: { taskId } });
						if (task0 && task0.dataFrom == 'MT-ADMIN') { 
							await MtAdmin.update({ arrivalTime: operationTime }, { where: { id: taskId.split('-')[1] } })
						}
					} else if (operationType === 'depart') {
						if (task0 && task0.dataFrom == 'MT-ADMIN') { 
							await MtAdmin.update({ departTime: operationTime }, { where: { id: taskId.split('-')[1] } })
						}
					} else if (operationType === 'end') {
						if (task0 && task0.dataFrom == 'MT-ADMIN') { 
							await MtAdmin.update({ endTime: operationTime }, { where: { id: taskId.split('-')[1] } });
						}
						isComplete = true;
					}
				}
			}
			await checkTaskComplete()

			if (isComplete) {
				log.info(`UserId ${userId} exe updateTaskOptTime taskId: ${taskId}, task is completed, start update mobiusTask info.`)
				let mileageTaskId = taskId + (indentId ? ('-' + indentId) : "");
				let mileageObj = await Mileage.findByPk(mileageTaskId);
				let vehicle = await Vehicle.findOne({ where: { vehicleNo } })
				if (!mileageObj) {
					log.warn(`UserId ${userId} exe updateTaskOptTime taskId: ${taskId}, task is completed, mileageObj is null, recreate.`)
					let vehicle = await Vehicle.findByPk(vehicleNo)
					let startMileage = vehicle ? vehicle.totalMileage : mileage;
					// Maybe start task exception
					await Mileage.create({
						taskId: mileageTaskId,
						driverId: user.driverId,
						vehicleNo,
						date: operationTime,
						startTime: operationTime,
						startMileage: startMileage
					})
					mileageObj = await Mileage.findByPk(mileageTaskId);
				}

				mileageObj.endTime = operationTime;
				mileageObj.endMileage = mileage;
				mileageObj.mileageTraveled = mileage - mileageObj.startMileage;
				mileageObj.mobileMileageTraveled = distance;
				
				// record for backup
				let updateTaskOptTimeObj = req.body;
				updateTaskOptTimeObj.startMileage = mileageObj.startMileage
				updateTaskOptTimeObj.startTime = mileageObj.startTime
				await stroreRecordIntoFile(updateTaskOptTimeObj);

				// note for now (2023-05-18)
				// if (mileageObj.mileageTraveled < 0) {
				// 	log.warn(`********************************`)
				// 	log.warn(`Wrong end mileage, please update your data.`)
				// 	log.warn(`TaskId => ${ taskId }`)
				// 	log.warn(`DriverId => ${ user.driverId }`)
				// 	log.warn(`Real start mileage is => ${ mileageObj.startMileage }`)
				// 	log.warn(`Received end mileage is => ${ mileage }`)
				// 	log.warn(`********************************`)
				// 	throw `Wrong end mileage, please update your data.`
				// }

				await mileageObj.save();
				await MileageHistory.destroy({ where: { taskId: mileageTaskId } })
				await sequelizeObj.query(` INSERT INTO mileage_history SELECT * FROM mileage WHERE taskId = ? `, { 
					type: QueryTypes.INSERT, replacements: [ mileageObj.taskId ]
				})

				if (taskId.startsWith('DUTY')) {
					// duty should be ready, maybe assign indent afternoon
					await sequelizeObj.query(`
						UPDATE urgent_duty SET \`status\` = ?, mobileStartTime = null 
						WHERE dutyId = ? and status != 'cancelled' 
					`, { type: QueryTypes.UPDATE, replacements: [ 'Ready', taskId ] })

					await sequelizeObj.query(`
						UPDATE urgent_indent SET \`status\` = ?, mobileEndTime = ? WHERE id = ? and status not in ('cancelled', 'completed')
					`, { type: QueryTypes.UPDATE, replacements: [ 'Completed', operationTime, indentId ] })

					await urgentService.updateDutyStatus(taskId.split('-')[1])

					let indent = await UrgentIndent.findByPk(indentId)

					// this is indent has been re-assigned, can not update system
					if (!indent.cancelBy) {
						// Update system data
						await sequelizeSystemObj.transaction(async t => {
							await sequelizeSystemObj.query(`
								UPDATE job SET \`status\` = ? WHERE requestId IN (?)
							`, { type: QueryTypes.UPDATE, replacements: [ 'Completed', [indent.requestId] ]})
							await sequelizeSystemObj.query(`
								UPDATE job_task SET \`taskStatus\` = ?, endTime = ?  WHERE requestId IN (?)
							`, { type: QueryTypes.UPDATE, replacements: [ 'Completed', operationTime, [indent.requestId] ]})
							await sequelizeSystemObj.query(`
								UPDATE driver SET \`status\` = ?  WHERE taskId IN (?)
							`, { type: QueryTypes.UPDATE, replacements: [ 'Completed', [indentId] ]})
						})
					}

				} else {
					await Task.update({ mobileEndTime: operationTime, driverStatus: 'completed', vehicleStatus: 'completed' }, { where: { taskId } });
				}

				// Update vehicle total mileage
				log.info(`UserId ${userId} exe updateTaskOptTime taskId: ${taskId}, task is completed, start update driver mileage.`)
				let driver = await Driver.findByPk(user.driverId);
				driver.totalMileage = driver.totalMileage + mileageObj.mileageTraveled;
				driver.updatedAt = moment().format('YYYY-MM-DD HH:mm:ss');
				await driver.save();
				// await Driver.increment({ totalMileage: mileageObj.mileageTraveled, updatedAt: moment().format('YYYY-MM-DD HH:mm:ss') }, { where: { driverId: user.driverId } })
				log.info(`UserId ${userId} exe updateTaskOptTime taskId: ${taskId}, task is completed, start update vehicle mileage.`)
				await Vehicle.update({ totalMileage: mileage}, { where: { vehicleNo } })
				let parentPermitType = 'none';
				if (vehicle) {
					let permitTypeConf = await PermitType.findOne({ where: { permitType : vehicle.permitType} });
					parentPermitType = vehicle.permitType;
					if (permitTypeConf && permitTypeConf.parent) {
						parentPermitType = permitTypeConf.parent;
					}

					let driverMileage = await DriverMileage.findOne({ where: { permitType: parentPermitType, driverId: user.driverId} })
					if (driverMileage) {
						driverMileage.mileage = driverMileage.mileage + mileageObj.mileageTraveled;
						driverMileage.updatedAt = moment().format('YYYY-MM-DD HH:mm:ss');
						await driverMileage.save();
					} else {
						await DriverMileage.create({
							driverId: user.driverId,
							permitType: parentPermitType,
							mileage: mileageObj.mileageTraveled
						});
					}

					//update driverPlatformConf  totalMileage
					let driverPlatformConf = await DriverPlatformConf.findOne({ where: { vehicleType: vehicle.vehicleType, driverId: user.driverId} })
					if (driverPlatformConf) {
						driverPlatformConf.totalMileage = (driverPlatformConf.totalMileage ? driverPlatformConf.totalMileage : 0) +  mileageObj.mileageTraveled;
						driverPlatformConf.lastDrivenDate = moment();
						await driverPlatformConf.save();
					}
				}
				// await driverService.createPlatform(taskId, indentId);

				// let errorMsg = await driverService.reCalcDriverLicensingStatus(user.driverId);
				// log.info(`UserId ${userId} exe updateTaskOptTime taskId: ${taskId}, reCalcDriverLicensingStatus result: ` + (errorMsg ? errorMsg : 'success!'));
			}
		})

		//re calc vehicle pm,avi time
		vehicleMaintenanceInfoCalcUtils.reCalcVehicleMaintenanceInfo(taskId, indentId);

		let task;
		let result = null;
		if (taskId.startsWith('DUTY')) {
			let duty = await urgentService.getDutyById(taskId)
			duty.taskStatus = 'Completed';
			result = {
				"code": 1,
				"message": "success",
				"data": duty,
			}
		} else {
			result = await mobileTOService.GETDriverTaskByTaskId(taskId);
		}
		if (result.code === 0) {
			return res.json(utils.response(0, result.msg));
		}
		task = result.data;
		task.limitSpeed = task.limitSpeed ? task.limitSpeed : 60
		
		let odd = await ODD.findOne({ where: { taskId: task.taskId }, order: [ [ 'updatedAt', 'DESC' ] ]})
		task.odd = odd ? odd.content : '';

		let assignedTask = await Task.findOne({ where: { taskId: task.taskId } })
		let mtRAC = await MT_RAC.findOne({ where: { taskId: task.taskId }, order: [ [ 'id', 'DESC' ] ] })

		if (taskId.startsWith('DUTY')) {
			task.taskId += '-' + indentId
			task.commanderContact = mtRAC ? { username: mtRAC.commander, hub: task.hub, node: task.node } : null;
		} else {
			task.commanderContact = mtRAC ? { username: mtRAC.commander, hub: assignedTask.hub, node: assignedTask.node } : null;
		}
		
		let mileageObj = await Mileage.findByPk(task.taskId)
		if (mileageObj) {
			task.startMileage = mileageObj.startMileage;
			task.endMileage = mileageObj.endMileage;
		} else {
			let vehicle = await Vehicle.findByPk(task.vehicleNumber)
			task.startMileage = vehicle ? vehicle.totalMileage : 0;
			task.endMileage = 0;
		}

		task.taskReady = true;
		task.taskStatus = 'Completed';

		log.info(`task => `, JSON.stringify(task, null, 4))

		return res.json(utils.response(1, task));
	},
	updateDriverStatus: async function (taskId) {
		try {
			let task = await Task.findOne({ where: { taskId } })
			let checkList = await CheckList.findAll({ where: { taskId } })
			let checkList1 = checkList.find(item => item.checkListName == CHECKLIST[1])
			let checkList2 = checkList.find(item => item.checkListName == CHECKLIST[2])
			let checkList4 = checkList.find(item => item.checkListName == CHECKLIST[4])
			let checkList5 = checkList.find(item => item.checkListName == CHECKLIST[5])
			if (checkList1 && checkList2 && checkList4) {
				if (task.driverStatus == 'waitcheck' && task.vehicleStatus == 'waitcheck') {
					if (task.purpose && task.purpose.toLowerCase() == 'wpt') {
						task.driverStatus = "ready"
						task.vehicleStatus = "ready"
						await task.save();
					} else if (checkList5) {
						task.driverStatus = "ready"
						task.vehicleStatus = "ready"
						await task.save();
					}
				}
			}
		} catch (error) {
			throw error
		}
	},
	updateUrgentStatus: async function (taskId, indentId) {
		try {
			let task = await UrgentDuty.findOne({ where: { dutyId: taskId } })
			let checkList = await CheckList.findAll({ where: { taskId } })
			let checkList1 = checkList.find(item => item.checkListName == CHECKLIST[1])
			let checkList2 = checkList.find(item => item.checkListName == CHECKLIST[2])
			let checkList4 = checkList.find(item => item.checkListName == CHECKLIST[4])
			let checkList5 = checkList.find(item => item.checkListName == CHECKLIST[5])
			if (checkList1 && checkList2 && checkList4 && checkList5) {
				if (task.status == 'waitcheck') {
					task.status = "Ready"
					await task.save();

					if (indentId) {
						// all indent should be ready
						let dutyId = taskId.split('-')[1]
						dutyId = Number.parseInt(dutyId)
						await UrgentIndent.update({ status: 'Ready' }, { where: { dutyId, status: 'waitcheck' } })
					}
				}
			}
		} catch (error) {
			throw error
		}
	},
	reportLateReason: async function (req, res) {
		let { taskId, userId, lateType, lateReason } = req.body;
		let user = await User.findByPk(userId);
		if (!user) {
			log.warn(`UserId ${userId} do not exist.`)
			throw new Error(`UserId ${userId} do not exist.`)
		}

		if (taskId.startsWith('DUTY')) {
			let idArray = taskId.split('-');
			if (idArray.length < 2) {
				log.warn(`TaskId ${taskId} format error.`)
				return res.json(utils.response(0, `TaskId ${taskId} format error.`));
			}
			taskId = `DUTY-${idArray[1]}`;
			let duty = await UrgentDuty.findOne({ where: { dutyId: taskId } })
			if (duty) {
				if (lateType == 'start') {
					duty.lateStartRemarks = lateReason;
					await duty.save();

					return res.json(utils.response(1, 'success.'));
				}
			} else {
				return res.json(utils.response(0, 'Task do not exist.'));
			}
		} else {
			let task = await Task.findOne({where : {taskId: taskId}})
			if (task) {
				if (lateType == 'start') {
					task.startLateReason = lateReason;
					await task.save();

					return res.json(utils.response(1, 'success.'));
				}
			} else {
				return res.json(utils.response(0, 'Task do not exist.'));
			}
		}
	},
}
