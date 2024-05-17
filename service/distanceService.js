const positionService = require('../service/positionService')

module.exports = {
	calculateMileageByTask: async function (params) {
		let { driverId, vehicleNo, taskId, timeZone } = params;

		// Calculate mobile & obd mileage
		positionService.calculateMileage({ taskId, driverId, vehicleNo, timeZone });
	},
	getPointDistance: function (point1, point2) {
		const radLat1 = point1.lat * Math.PI / 180.0;
		const radLat2 = point2.lat * Math.PI / 180.0;
		const a = radLat1 - radLat2;
		const b = point1.lng * Math.PI / 180.0 - point2.lng * Math.PI / 180.0;
		let s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
		s = s * 6378.137; // Equatorial radius
		s = Math.round(s * 10000) / 10000;
		return s; // return km
	},
	calculateDistance: function (pointList) {
		if (!pointList || !Array.isArray(pointList)) return 0;
		if (!pointList.length || pointList.length === 1) return 0;
		
		let index = 0, totalMileage = 0;
		for (let point of pointList) {
			// Last one point do not need cal
			if ((index + 1) === pointList.length) break;
			totalMileage += this.getPointDistance(point, pointList[index + 1]);
			index++;
		}
		return totalMileage;
	}
}