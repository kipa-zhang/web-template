const { DataTypes, QueryTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.ServiceMode = dbConf.sequelizeObj.define('service_mode', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    serviceName: {
        type: DataTypes.STRING(255),
        allowNull: false,
    }
}, {
    tableName: 'service_mode',
    timestamps: false,
}
);
