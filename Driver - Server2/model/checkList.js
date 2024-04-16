const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.CheckList = dbConf.sequelizeObj.define('check_list', {
    id: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
    },
    taskId: {
        type: DataTypes.STRING(20), 
    },
	indentId: {
        type: DataTypes.STRING(100),
    },
    driverId: {
        type: DataTypes.INTEGER(11),
    },
	vehicleNo: {
		type: DataTypes.STRING(55)
	},
	checkListName: {
		type: DataTypes.STRING(55)
	}
  }, {
    // other options
    timestamps: false
});
