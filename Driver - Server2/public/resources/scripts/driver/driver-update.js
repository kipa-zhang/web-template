import { initDriverTaskViewPage } from './driver-view.js'
import { customPopupInfo } from '../common-script.js'

$(function () {

})

let currentUser = {};
let driverTaskId = {};
let unitList = [];
let vehicleList = [];
let routeList = [];

export async function  initUnitList () {
    const getUnitTypeList = function () {
        return axios.post('/getUnitList')
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage)
                return [];
            }
        });
    }
    unitList = await getUnitTypeList();
    return unitList;
};

export async function initVehicleList () {
    const getVehicleList = function ()  {
        return axios.post('/vehicle/getVehicleList')
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage
            } else {
                console.error(res.respMessage)
            }
        });
    }
    vehicleList = await getVehicleList();
    return vehicleList;
}

export async function initRouteList () {
    const getRouteList = function () {
        return axios.post('/route/getRouteList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    routeList = await getRouteList();
    return routeList;
};

export async function initUserEditPage (user) {
    driverTaskId = user.driverTaskId;
    const getDriverTaskById = function (driverTaskId) {
        return axios.post('/driver/getDriverTaskById', { driverTaskId })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    let userResult = await getDriverTaskById(driverTaskId);
    if (userResult.length) {
        currentUser.id = userResult[0].id;
        currentUser.driverName = userResult[0].driverName;
        currentUser.unitId = userResult[0].unitId;
        currentUser.unit = userResult[0].unit;
        currentUser.vehicleNo = userResult[0].vehicleNo;
        currentUser.routeNo = userResult[0].routeNo;
        $('#update-driver #driverName').val(currentUser.driverName);
        $('#update-driver').modal('show');
    } else {
        customPopupInfo('Attention', `UserId ${ user.userId } does not exist.`)
    }
    await initUnitList();
    const initUnitPage = function () {
        let _unitList = unitList.map(unit => { return unit.unit });
        _unitList = Array.from(new Set(_unitList))
        $('#update-driver .select-unit').empty()
        $('#update-driver .select-subUnit').empty()
        for (let unit of _unitList) {
            let html = `<option value="${ unit }">${ unit }</option>`
            $('#update-driver .select-unit').append(html)
        }
        $('#update-driver .select-unit').on('change', function () {
            let selectedUnit = $(this).val();
            $('#update-driver .select-subUnit').empty()
            for (let unit of unitList) {
                if (unit.unit === selectedUnit) {
                    let subUnitHtml = null;
                    subUnitHtml = `<option value="${ unit.id }">${ unit.subUnit ? unit.subUnit : '-' }</option>`
                    $('#update-driver .select-subUnit').append(subUnitHtml)
                }
            }
        })

        $('#update-driver .select-unit').val(currentUser.unit).change();
        $('#update-driver .select-subUnit').val(currentUser.unitId);

        
    }
    await initVehicleList();
    const initVehiclePage = function () {
     $('#update-driver .select-vehicle').empty()
        for (let vehicle of vehicleList) {
            let html = null;
            html = `<option value="${ vehicle.vehicleNo }">${ vehicle.vehicleNo }</option>`
            if(currentUser.vehicleNo === vehicle.vehicleNo){
                html = `<option value="${ vehicle.vehicleNo }" selected="selected">${ vehicle.vehicleNo }</option>`
            }
            $('#update-driver .select-vehicle').append(html)
        }
    }
    await initRouteList();
    const initSelectRoutePage = function () {
        $('#update-driver .select-route').empty()
        for (let route of routeList) { 
            let html = null;
            html = `<option value="${ route.routeNo }">${ route.routeName }</option>`
            if(currentUser.routeNo === route.routeNo){
                html = `<option value="${ route.routeNo }" selected="selected">${ route.routeName }</option>`
            }
            $('#update-driver .select-route').append(html)
        }
    }
    initUnitPage();
    initVehiclePage();
    initSelectRoutePage();
    $('#update-driver .update-driverTask').off('click').on('click', updateUserEventHandler)
}

const updateUserEventHandler = function () {
    let driverTask = {} 
    driverTask.unitId = $('#update-driver .select-subUnit option:selected').val();
    driverTask.driverTaskId = currentUser.id;
    driverTask.routeNo = $('#update-driver .select-route').val();
    driverTask.vehicleNo = $('#update-driver .select-vehicle').val();
    const updateUserRequest = function (driverTask) {
        return axios.post('/driver/updateDriverTask', { driverTask })
            .then(function (res) {
                if (res.respCode === 1) {
                    return true
                } else {
                    customPopupInfo('Attention', res.respMessage)
                    return false
                }
            });
    }
    updateUserRequest(driverTask).then(result => {
        if (result) {
            $('#update-driver').modal('hide');
            initDriverTaskViewPage();
        } else {
            
        }

    })
    
}