import { initRouteList } from '../route/route-view.js'
import { initDriverTaskViewPage } from '../driver/driver-view.js'
import { customPopupInfo } from '../common-script.js'

$(function () {
    
})

let availableDriverList = [], unitList = [], routeList = [], vehicleList = [];

export async function initDriverTaskCreatePage () {
    const getAvailableDriverList = function () {
        return axios.post('/driver/availableDriverList')
            .then(function (res) {
                if (res.respCode === 1) {
                    availableDriverList = res.respMessage
                } else {
                    console.error(res.respMessage)
                }
            });
    }
    const getUnitList = function () {
        return axios.post('/getUnitList')
            .then(function (res) {
                if (res.respCode === 1) {
                    unitList = res.respMessage
                } else {
                    console.error(res.respMessage)
                }
            });
    }
    const getVehicleList = function () {
        return axios.post('/vehicle/getVehicleList')
            .then(function (res) {
                if (res.respCode === 1) {
                    vehicleList = res.respMessage
                } else {
                    console.error(res.respMessage)
                }
            });
    }

    const initDriverListPage = function () {
        $('#create-driver .select-driverId').empty();
        for (let driver of availableDriverList) {
            let html = `<option value="${ driver.driverId }">${ driver.driverName }</option>`
            $('#create-driver .select-driverId').append(html)
        }
        $('#create-driver .select-driverId').off('change').on('change', function () {
            let driverId = $(this).val();
            availableDriverList.some(driver => {
                if (driver.driverId == driverId) {
                    if (driver.unit) {
                        $('#create-driver .select-unit').val(driver.unit).trigger('change').prop('disabled', true);
                    } else {
                        $('#create-driver .select-unit').prop('disabled', false);
                    }
                    setTimeout(() => {
                        if (driver.subUnit) {
                            $('#create-driver .select-subUnit').val(driver.subUnit).prop('disabled', true);
                        } else {
                            $('#create-driver .select-subUnit').prop('disabled', false);
                        }
                    }, 100)
                    
                    return true;
                }
            })
        })
        $('#create-driver .select-driverId').trigger('change');
    }
    const initSelectRoutePage = function () {
        $('#create-driver .select-route').empty()
        for (let route of routeList) {
            let html = `<option value="${ route.routeNo }">${ route.routeName }</option>`
            $('#create-driver .select-route').append(html)
        }
    }
    const initVehiclePage = function () {
        $('#create-driver .select-vehicle').empty()
        for (let vehicle of vehicleList) {
            let html = `<option value="${ vehicle.vehicleNo }">${ vehicle.vehicleNo }</option>`
            $('#create-driver .select-vehicle').append(html)
        }
    }
    const initUnitPage = function () {
        let _unitList = unitList.map(unit => { return unit.unit });
        _unitList = Array.from(new Set(_unitList))
        $('#create-driver .select-unit').empty()
        $('#create-driver .select-subUnit').empty()
        for (let unit of _unitList) {
            let html = `<option value="${ unit }">${ unit }</option>`
            $('#create-driver .select-unit').append(html)
        }
        let subUnitHtml = `<option value="${ unitList[0].subUnit }" data-id="${ unitList[0].id }">${ unitList[0].subUnit }</option>`
        $('#create-driver .select-subUnit').append(subUnitHtml)

        // Change subUnit while unit changed.
        $('#create-driver .select-unit').on('change', function () {
            let selectedUnit = $(this).val();
            $('#create-driver .select-subUnit').empty()
            for (let unit of unitList) {
                if (unit.unit.toLowerCase() === selectedUnit.toLowerCase()) {
                    let subUnitHtml = `<option value="${ unit.subUnit }" data-id="${ unit.id }">${ unit.subUnit ? unit.subUnit : '-' }</option>`
                    $('#create-driver .select-subUnit').append(subUnitHtml)
                }
            }
        })
    }

    const initBtnClickEventHandler = function () {
        const createDriverTaskRequest = function (driverTask) {
            return axios.post('/driver/createDriverTask', { driverTask })
                .then(function (res) {
                    if (res.respCode === 1) {
                        return true;
                    } else {
                        customPopupInfo('Attention', res.respMessage);
                        console.error(res.respMessage)
                        return false;
                    }
                });
        }
        $('#create-driver .create-driverTask').off('click').on('click', () => {
            let driverTask = {}
            driverTask.driverId = $('#create-driver .select-driverId').val();
            driverTask.highPriority = $('#create-driver .form-check-input[name="highPriority"]').prop('checked');
            driverTask.routeNo = $('#create-driver .select-route').val();
            driverTask.unitId = $('#create-driver .select-subUnit option:selected').data('id')
            driverTask.vehicleNo = $('#create-driver .select-vehicle').val();
            createDriverTaskRequest(driverTask).then(result => {
                if (result) {
                    $('#create-driver').modal('hide');
                    initDriverTaskViewPage();
                } else {
                }
            })
        })
    }
    
    await getAvailableDriverList();
    await getUnitList();
    await getVehicleList();
    routeList = await initRouteList();
    initUnitPage();
    initVehiclePage();
    initDriverListPage();
    initSelectRoutePage();
    initBtnClickEventHandler();

    // show view driver create module
    $('#create-driver').modal('show');
};


