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
        type: DataTypes.STRING(15),
    },
    driverId: {
        type: DataTypes.BIGINT(15),
    }, 
    creator: {
        type: DataTypes.INTEGER(11),
        defaultValue: null,
    },
}, {
    tableName: 'urgent_config',
})