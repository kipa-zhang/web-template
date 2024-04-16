import { customPopupInfo, customConfirm } from '../common-script.js'
$(function () {

})

let waypointList = [
];
let waypointViewDataTable = null;

export async function initWaypointList () {
    const getWaypointList = function () {
        return axios.post('/route/getWaypointList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    waypointList = await getWaypointList();
    return waypointList;
};

const initDataTables = async function () {
    await initWaypointList();
    waypointViewDataTable = $('#waypoint-upload-list').DataTable({
        destroy: true,
        data: waypointList,
        columns: [
            { title: 'Name', data: 'waypointName', sortable: true },
            { title: 'Latitude', data: 'lat', defaultContent: '-', sortable: true },
            { title: 'Longitude', data: 'lng', defaultContent: '-', sortable: false },
            { title: 'Delete', data: null, render: function (data, type, row, meta) {

                let operationList = row.operation.split(',')
                let html = `  `

                if (operationList.includes('Delete')) {
                    html += ` <button type="button" class="btn btn-primary custom-btn-green" onclick="deleteWaypointHandler(${ row.id })">Delete</button> ` 
                }
                return html
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        searching: false,
        pageLength: 8,
        // scrollY: 200,
        // scrollCollapse: true,
        // stateSave: true, // keep page, searching, filter
    });
}

export async function initWaypointUploadPage () {
    // show view route module
    $('#view-waypoint-upload').modal('show');
    
    await initDataTables();
    waypointUploadEventHandler();
}

let waypointUploadIns = null;
const waypointUploadEventHandler = function () {
    layui.use('upload', function() {
        let upload = layui.upload;
        if (waypointUploadIns) return;
        waypointUploadIns = upload.render({
            elem: '#waypoint-upload',
            url: '/upload/uploadWaypoint',
            accept: 'file',
            acceptMime: 'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            // multiple: true,
            done: function (res) {
                initDataTables();
            },
            error: function (error) {
                console.error(error);
                initCustomModal(`Upload waypoint failed.`)
            }
        });
    });
}

window.deleteWaypointHandler = function (waypointId) {
    customConfirm('Confirm', 'Are you sure to delete this waypoint ?', async function () {
        axios.post('/route/deleteWaypoint', { waypoint: { id: waypointId } })
            .then(function (res) {
                if (res.respCode === 1) {
                    initDataTables();
                } else {
                    console.error(res.respMessage)
                    customPopupInfo('Attention', res.respMessage)
                }
            });
    })
    
}