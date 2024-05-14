const { Sequelize, Op, QueryTypes } = require('sequelize');
const moment = require('moment');
const CONTENT = require('../util/content');

const { Track } = require('../model/event/track.js');
const { CompareResult } = require('../model/compareResult.js');

const updateCompareResult = async function () {
    try {
        const trackList = await Track.findAll({
			where: {
				violationType: [CONTENT.ViolationType.HardBraking, CONTENT.ViolationType.RapidAcc]	
			},
		});
        for (let track of trackList) {
			let accSpeed = 0, resultByFixed = '', resultByExcel = '';
			let preSpeed = track.speed;
			let curSpeed = track.endSpeed;
			let diffSpeed = curSpeed - preSpeed;
			if (diffSpeed == 0 || track.diffSecond == 0) {
				accSpeed = 0;
			} else {
				accSpeed = diffSpeed / track.diffSecond;
			}

			if (accSpeed < 0 && Math.abs(accSpeed) > 10.5) resultByFixed = CONTENT.ViolationType.HardBraking
			if (accSpeed > 0 && Math.abs(accSpeed) > 13.5) resultByFixed = CONTENT.ViolationType.RapidAcc

			if (preSpeed >= 20 && preSpeed < 25 && Math.abs(accSpeed) > 22.22) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 25 && preSpeed < 30 && Math.abs(accSpeed) > 26.04) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 30 && preSpeed < 35 && Math.abs(accSpeed) > 30.00) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 35 && preSpeed < 40 && Math.abs(accSpeed) > 33.11) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 40 && preSpeed < 45 && Math.abs(accSpeed) > 36.36) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 45 && preSpeed < 50 && Math.abs(accSpeed) > 37.50) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 50 && preSpeed < 55 && Math.abs(accSpeed) > 39.06) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 55 && preSpeed < 60 && Math.abs(accSpeed) > 39.80) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 60 && preSpeed < 65 && Math.abs(accSpeed) > 40.91) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 65 && preSpeed < 70 && Math.abs(accSpeed) > 41.42) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 70 && preSpeed < 75 && Math.abs(accSpeed) > 42.24) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 75 && preSpeed < 80 && Math.abs(accSpeed) > 42.29) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 80 && preSpeed < 85 && Math.abs(accSpeed) > 42.67) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 85 && preSpeed < 90 && Math.abs(accSpeed) > 43.01) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 90 && preSpeed < 95 && Math.abs(accSpeed) > 43.55) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc
			else if (preSpeed >= 95 && preSpeed < 100 && Math.abs(accSpeed) > 44.02) resultByExcel = accSpeed < 0 ? CONTENT.ViolationType.HardBraking : CONTENT.ViolationType.RapidAcc

			// console.log(`resultByFixed: ${resultByFixed ? resultByFixed : '-'} || resultByExcel: ${resultByExcel ? resultByExcel : '-'}`)
            await CompareResult.upsert({
				deviceId: track.deviceId,
				preSpeed: track.speed,
				preTime: track.occTime,
				curSpeed: track.endSpeed,
				curTime: track.endTime,
				diffSecond: track.diffSecond,
				diffSpeed,
				accSpeed: accSpeed,
				check: (resultByFixed && resultByFixed === resultByExcel) ? true : false,
				resultByFixed,
				resultByExcel,
			}, { fields: ['deviceId', 'preSpeed', 'preTime', 'curSpeed', 'curTime', 'diffSecond', 'diffSpeed', 'accSpeed', 'check', 'resultByFixed', 'resultByExcel'] })
        }
    } catch (err) {
        console.error('(updateCompareResult) : ', err);
    }
};
module.exports.updateCompareResult = updateCompareResult;