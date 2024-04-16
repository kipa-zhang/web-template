import { customPopupInfo, customConfirm } from '../common-script.js'
import { initUnitEditPage } from '../unit/unit-edit.js'

$(function () {
    $('#view-unit .unit-search').on('keyup', _.debounce(() => {
        filterOfUsername = $('#view-unit .unit-search').val();
        initDataTables();
    }, 500))
})

let filterOfUnit;
let unitList = [];
let unitViewDataTable = null;

export async function initUnitList () {
    const getUnitList = function () {
        return axios.post('/getUnitList', { unit: filterOfUnit })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return [];
                }
            });
    } 
    
    unitList = await getUnitList();
    return unitList;
};

const initDataTables = async function () {
    await initUnitList();
    unitViewDataTable = $('#unit-list').DataTable({
        destroy: true,
        data: unitList,
        columns: [
            { title: 'Unit ID', data: 'id', sortable: true },
            { title: 'Unit', data: 'unit', defaultContent: '-', sortable: true },
            { title: 'Sub-Unit', data: 'subUnit', defaultContent: '-', sortable: true },
            { title: 'Action', data: null, render: function (data, type, row, meta) {      
                let html = ``
                let operation = row.operation.split(',')
                if (operation.includes('Edit')) {
                    html += ` <button type="button" class="btn btn-primary btn-sm custom-btn-green btn-edit-unit" onclick="initUnitEditEventHandler(${ row.id })">Edit</button> `
                }
                if (operation.includes('Delete')) {
                    html += ` <button type="button" class="btn btn-primary btn-sm custom-btn-danger btn-delete-unit" onclick="initUnitDeleteEventHandler(${ row.id })">Delete</button> `
                }

                return html
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        // searching: false,
        pageLength: 8,
        // scrollY: 200,
        // scrollCollapse: true,
        // stateSave: true, // keep page, searching, filter
    });
    $('#view-unit').modal('show');
}

export function initUnitViewPage () {
    initDataTables();
    $('#view-unit').modal('show');
}

window.initUnitEditEventHandler = function (unitId) {
    initUnitEditPage(unitId);
    $('#view-unit').modal('hide');
}
window.initUnitDeleteEventHandler = async function (unitId) {
    const deleteUnit = function (unitId) {
        return axios.post('/deleteUnit', { unitId })
            .then(function (res) {
                if (res.respCode === 1) {
                    return true
                } else {
                    console.error(res.respMessage)
                    customPopupInfo('Attention', res.respMessage)
                    return false
                }
            });
    } 
    
    customConfirm('Confirm', 'Are you sure to delete this unit ?', async function () {
        let result = await deleteUnit(unitId)
        if (result) {
            initUnitViewPage();
        } else {
            
        }
    })
    
}
