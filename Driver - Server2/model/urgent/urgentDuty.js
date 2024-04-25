const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.UrgentDuty = dbConf.sequelizeObj.define('urgentDuty', {
    id: {
        type: DataTypes.INTEGER,
        autoIncrement: true,
        primaryKey: true,
    },
    dutyId: {
        type: DataTypes.STRING(55),
    },
    configId: {
        type: DataTypes.INTEGER,
    },
    // indentDate: {
	// 	type: DataTypes.DATE,
	// },
    indentStartDate: {
		type: DataTypes.DATE,
	},
    indentEndDate: {
		type: DataTypes.DATE,
	},
    mobileStartTime: {
		type: DataTypes.DATE,
	},
    mobileEndTime: {
		type: DataTypes.DATE,
	},
    status: {
        type: DataTypes.STRING(55)
    },
    lateStartRemarks: {
        type: DataTypes.STRING(255)
    }
}, {
    tableName: 'urgent_duty',
})