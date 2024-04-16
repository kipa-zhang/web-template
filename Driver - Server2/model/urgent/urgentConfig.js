const { DataTypes } = require('sequelize');
const dbConf = require('../../db/dbConf');

module.exports.UrgentConfig = dbConf.sequelizeObj.define('urgentConfig', {
    id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
    },  
    purpose: {
        type: DataTypes.STRING(50),
    },
    hub: {
        type: DataTypes.STRING(50),
    },
    node: {
        type: DataTypes.STRING(50),
    },
    unitId: {
        type: DataTypes.BIGINT(15),
    },
    groupId: {
        type: DataTypes.BIGINT(15),
    },
    category: {
        type: DataTypes.STRING(50),
        defaultValue: 'MV'
    },
	indentStartDate: {
		type: DataTypes.DATE,
	},
    indentEndDate: {
		type: DataTypes.DATE,
	},
    startTime: {
        type: DataTypes.TIME,
	},
    endTime: {
        type: DataTypes.TIME,
	},
    vehicleType: {
        type: DataTypes.STRING(50),
    },
    vehicleNo: {
        type: DataTypes.STRING(30),
    },
    driverId: {
        type: DataTypes.BIGINT(15),
    }, 
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
    cancelledDateTime: {
        type: DataTypes.DATE
    },
    cancelledCause: {
        type: DataTypes.STRING(255),
    },
    amendedBy: {
        type: DataTypes.INTEGER(11),
    },
}, {
    tableName: 'urgent_config',
})