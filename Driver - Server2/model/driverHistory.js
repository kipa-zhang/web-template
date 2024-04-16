const { DataTypes } = require('sequelize');
const dbConf = require('../db/dbConf');
const utils = require('../util/utils');

module.exports.DriverHistory = dbConf.sequelizeObj.define('driver_history', {
    driverId: {
        type: DataTypes.INTEGER(11),
        primaryKey: true,
    },
    loginName: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    driverName: {
        type: DataTypes.STRING(255),
        allowNull: false,
    },
    nric: {
        type: DataTypes.STRING(55),
        get(){
            let val = this.getDataValue('nric');
            let newValue = val
            if(val) {
                if(val.length > 9) newValue = utils.decodeAESCode(val);
            }
            return newValue
        }
    },
    contactNumber: {
        type: DataTypes.STRING(55),
    },
    permitType: {
        type: DataTypes.STRING(255),
    },
    vehicleType: {
        type: DataTypes.STRING(3000),
    },
    unit: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    unitId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    groupId: {
        type: DataTypes.INTEGER(12),
        defaultValue: null,
    },
    totalMileage: {
        type: DataTypes.INTEGER(12),
        defaultValue: 0,
    },
    vocation: {
        type: DataTypes.STRING(55),
    },
    rank: {
        type: DataTypes.STRING(55),
    },
    enlistmentDate: {
        type: DataTypes.DATE,
    },
    operationallyReadyDate: {
        type: DataTypes.DATE,
    },
    birthday: {
        type: DataTypes.DATE, 
    },
    bloodType: {
        type: DataTypes.STRING(5), 
    },
    updatedAt: {
        type: DataTypes.DATE,
        defaultValue: DataTypes.NOW
    },
    creator: {
        type: DataTypes.INTEGER(12),
        allowNull: false,
    },
    vehicleNo: {
        type: DataTypes.VIRTUAL,
    },
    lat: {
        type: DataTypes.VIRTUAL,
    },
    lng: {
        type: DataTypes.VIRTUAL,
    },
    status: {
        type: DataTypes.STRING(20),
        defaultValue: 'Deployable',
        comment: `Available Status= Deployable, Deployed, On Leave`
    },
    overrideStatus: {
        type: DataTypes.STRING(20),
    },
    overrideStatusTime: {
        type: DataTypes.DATE,
    },
    licensingStatus: {
        type: DataTypes.STRING(20),
        defaultValue: 'Not Ready'
    },
    lastSOSDateTime: {
        type: DataTypes.DATE
    },
    state: {
        type: DataTypes.STRING(55)
    }, 
    permitNo: {
        type: DataTypes.STRING(55),
        defaultValue: ""
    }, 
    permitIssueDate: {
        type: DataTypes.DATE,
        defaultValue: null
    },
    permitStatus: {
        type: DataTypes.STRING(10),
        defaultValue: "valid"
    }, 
    permitInvalidReason: {
        type: DataTypes.STRING(64),
        defaultValue: null
    },
    civilianLicence: {
        type: DataTypes.STRING(255),
        defaultValue: null,
    },
    cardSerialNumber: {
        type: DataTypes.STRING(50),
        defaultValue: null,
    },
    dateOfIssue: {
        type: DataTypes.DATE, 
        defaultValue: null,
    },
}, {
    // other options
    tableName: 'driver_history',
    timestamps: false,
});
