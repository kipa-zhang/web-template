const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.CompareResult = dbConf.sequelizeObj.define('compare_result', {
    devicePositionHistoryId: {
        type: DataTypes.STRING,
        defaultValue: 0,
        primaryKey: true,
    },
    deviceId: {
        type: DataTypes.STRING,
        allowNull: false,
    },
	preSpeed: {
        type: DataTypes.FLOAT,
        allowNull: false,
    },
    curSpeed: {
        type: DataTypes.FLOAT,
        allowNull: false,
    },
	diffSecond: {
		type: DataTypes.FLOAT,
        defaultValue: 0
	},
	diffSpeed: {
		type: DataTypes.FLOAT,
        defaultValue: 0
	},
	accSpeed: {
		type: DataTypes.FLOAT,
        defaultValue: 0
	},
	resultByFixed: {
		type: DataTypes.STRING,
        defaultValue: '',
        primaryKey: true,
	},
	resultByExcel: {
		type: DataTypes.STRING,
        defaultValue: ''
	},
    check: {
        type: DataTypes.TINYINT,
        defaultValue: 0
    },
    preTime: {
        type: DataTypes.DATE,
        allowNull: false,
    },
    curTime: {
        type: DataTypes.DATE,
        allowNull: false,
    },
  }, {
    // other options
    timestamps: false
});
