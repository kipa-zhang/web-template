import { initUserEditPage } from './driver-update.js'
import { customPopupInfo, customConfirm } from '../common-script.js'

$(function () {
    $('#view-driver .driver-search').on('keyup', _.debounce(() => {
        filterOfDriverName = $('#view-driver .driver-search').val();
        initDataTables();
    }, 500))
})

let filterOfDriverName = null;
let driverTaskList = [];
let driverViewDataTable = null;

export async function initDriverTaskList () {
    const getDriverTaskList = function () {
        return axios.post('/driver/getDriverTaskList', { driverName: filterOfDriverName })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    driverTaskList = await getDriverTaskList();
    return driverTaskList;
};

const initDataTables = async function () {
    await initDriverTaskList();
    driverViewDataTable = $('#driver-list').DataTable({
        destroy: true,
        data: driverTaskList,
        columns: [
            // { title: 'Selected', data: null, render: function (data, type, row, meta) {
            //     return `
            //         <div class="form-check">
            //             <input class="form-check-input" type="checkbox" value="">
            //         </div>
            //     `
            // } },
            { title: 'Driver Name', data: 'driverName', sortable: true },
            { title: 'Route Assigned', data: 'routeNo', defaultContent: '-', sortable: false },
            { title: 'Vehicle No', data: 'vehicleNo', defaultContent: '-', sortable: false },
            { title: 'From Address', data: 'fromAddress', defaultContent: '-', sortable: false },
            { title: 'To Address', data: 'toAddress', defaultContent: '-', sortable: false },
            // { title: 'Start Time', data: 'startTime', defaultContent: '-', sortable: false },
            // { title: 'End Time', data: 'endTime', defaultContent: '-', sortable: false },
            { title: 'State', data: 'status', defaultContent: '-', sortable: false },
            { title: 'Action', data: null, with: '30%', render: function (data, type, row, meta) {
                return `
                    <button type="button" class="btn btn-primary custom-btn-green" onclick="deleteDriverEventHandler(${ row.driverTaskId })">Delete</button>
                    <button type="button" class="btn btn-primary custom-btn-green" onclick="editDriverEventHandler(${ row.driverTaskId })">Edit</button>
                `;
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        autoWidth: false,
        // searching: true,
        pageLength: 8,
        // scrollY: 200,
        // scrollCollapse: true,
        // stateSave: true, // keep page, searching, filter
    });
}

export async function initDriverTaskViewPage () {
    initDataTables();

    // show view driver module
    $('#view-driver').modal('show');
}

window.deleteDriverEventHandler = async function (driverTaskId) {
    customConfirm('Confirm', 'Are you sure to delete this driver task ?', async function () { 
        axios.post('/driver/deleteDriverTask', { driverTaskId })
        .then(function (res) {
            if (res.respCode === 1) {
                initDataTables()
            } else {
                console.error(res.respMessage)
                customPopupInfo('Attention', res.respMessage)
            }
        });
    })
    
}

window.editDriverEventHandler = async function (driverTaskId) {
    initUserEditPage({ driverTaskId });
    $('#view-driver').modal('hide');
}