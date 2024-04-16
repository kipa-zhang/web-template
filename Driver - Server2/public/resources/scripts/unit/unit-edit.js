import { initUnitViewPage } from '../unit/unit-view.js'
import { customPopupInfo } from '../common-script.js'

$(function () {
    $('.create-unit').off('click').on('click', editUnitHandler);

    $('.cancel-create-unit').on('click', function () {
        $('#edit-unit .unit').val('');
        $('#edit-unit .subUnit').val('');
    });
})

let currentUnit = {};


const initUnitList = async function (unitId) {
    const getUnit = function (unitId) {
        return axios.post('/getUnit', { id: unitId })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage)
                    return [];
                }
            });
    }
    currentUnit = await getUnit(unitId);
    return currentUnit;
};

const getCurrentUserList = function () {
    return axios.post('/getCurrentUser')
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage
            } else {
                console.error(res.respMessage)
                return [];
            }
        });
}

export async function initUnitEditPage (unitId) {
    if (unitId) {
        await initUnitList(unitId);
        $('#unit-modal-title').html('Edit Unit');
        let userList = await getCurrentUserList()
        clearEditUnitPage(userList[0].unit);
    } else {
        currentUnit = {};
        $('#unit-modal-title').html('Create Unit');
		clearEditUnitPage(Cookies.get('hub'));
    }
    $('#edit-unit').modal('show');
}


const generateUnit = function () {
    currentUnit.unit = $('#edit-unit .unit').val().trim();
    currentUnit.subUnit = $('#edit-unit .subUnit').val() ? $('#edit-unit .subUnit').val() : null;
}

const clearEditUnitPage = function (unit) {
    console.log(unit)
    if (Cookies.get('userType') === 'UNIT') {
        $('#edit-unit .unit').prop('readonly', 'true');
        $('#edit-unit .unit').prop('disabled', 'true');
    }
    $('#edit-unit .unit').val(currentUnit.unit ? currentUnit.unit : unit);
    $('#edit-unit .subUnit').val(currentUnit.subUnit ? currentUnit.subUnit : null);
}

const editUnitHandler = async function () {
    generateUnit();
    if (!currentUnit.unit) {
        customPopupInfo('Attention', 'Unit can not be empty.')
        return;
    }
    let result = false;
    if (currentUnit.id) {
        result = await updateUnitRequest(currentUnit)
    } else {
        result = await createUnitRequest(currentUnit)
    }
    if (result) {
        currentUnit = {};
        $('#edit-unit').modal('hide');
        initUnitViewPage();
        clearEditUnitPage();
    }
}
const createUnitRequest = async function (unit) {
    return await axios.post('/createUnit', { unit })
        .then(function (res) {
            if (res.respCode === 1) {
                return true
            } else {
                customPopupInfo('Attention', res.respMessage);
                return false
            }
        });
            
}
const updateUnitRequest = function (unit) {
    return axios.post('/updateUnit', { unit })
        .then(function (res) {
            if (res.respCode === 1) {
                return true
            } else {
                customPopupInfo('Attention', res.respMessage);
                return false
            }
        });
}
