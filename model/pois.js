const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');

module.exports.Pois = dbConf.sequelizeObj.define('pois', {
    id: {
        type: DataTypes.INTEGER(11), 
        primaryKey: true,
        autoIncrement: true,
    },
    poisName: {
        type: DataTypes.STRING(55),
        allowNull: false,
    },
    lat: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    },
    lng: {
        type: DataTypes.FLOAT(13, 10),
        defaultValue: 0
    }
}, {
    // other options
    tableName: 'pois',
    timestamps: false,
    indexes: [
        {
            name: 'idx_of_poisName',
            fields: ['poisName']
        }
    ],
});
