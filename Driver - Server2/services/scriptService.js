const { PermitType } = require('../model/permitType.js');
const { Driver } = require('../model/driver.js');
const { User } = require('../model/user.js');
const { DriverPlatformConf } = require('../model/driverPlatformConf.js');

const runScript = async function () {
    try {
        let user = await User.findOne({ where: { userType: 'HQ' } })
        let driverList = await Driver.findAll();
        let permitTypeList = await PermitType.findAll();
        let driverPlatformConfList = await DriverPlatformConf.findAll();
        let newDriverPlatformConfList = []
        for (let driver of driverList) {
            if (!driver.vehicleType) continue;
            let allowedVehicleList = driver.vehicleType.split(',')
            for (let vehicleType of allowedVehicleList) {
                if (!vehicleType) continue;
                // let permitType = permitTypeList.find(item => item.vehicleType.split(',').indexOf(vehicleType) > -1)
                let permitType = permitTypeList.find(item => {
                    if (!item.vehicleType) return false;
                    let validVehicleTypes = item.vehicleType.split(',');
                    return validVehicleTypes.indexOf(vehicleType) > -1;
                });

                if (!permitType) continue;

                newDriverPlatformConfList.push({
                    driverId: driver.driverId,
                    permitType: permitType && permitType.permitType,
                    vehicleType: vehicleType,
                    creator: user.userId,
                })
                // newDriverPlatformConfList.push({
                //     driverId: driver.driverId,
                //     permitType: permitType.permitType,
                //     vehicleType: vehicleType,
                //     creator: user.userId,
                // })
            }
        }
        for (let driverPlatformConf of driverPlatformConfList) {
            for (let newDriverPlatformConf of newDriverPlatformConfList) {
                if (driverPlatformConf.driverId == newDriverPlatformConf.driverId
                    && driverPlatformConf.permitType == newDriverPlatformConf.permitType) {
                        newDriverPlatformConf.id = driverPlatformConf.id;
                    }
            }
        }
        await DriverPlatformConf.bulkCreate(newDriverPlatformConfList, { updateOnDuplicate: [ 'updatedAt' ] })
    } catch (error) {
        console.error(error)
    }
}

// runScript();