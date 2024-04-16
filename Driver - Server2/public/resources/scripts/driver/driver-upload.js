import { initCustomModal } from '../common-script.js'

$(function () {
    
})

let driverList = [];
let driverViewDataTable = null;

export async function initDriverList () {
    const getDriverList = function () {
        return axios.post('/driver/getDriverList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    driverList = await getDriverList();
    return driverList;
};

const initDataTables = async function () {
    driverList = await initDriverList();
    driverViewDataTable = $('#driver-upload-list').DataTable({
        destroy: true,
        data: driverList,
        columns: [
            // { title: 'Driver Id', data: 'driverId', sortable: true },
            { title: 'Driver Name', data: 'driverName', width: '20%', defaultContent: '-', sortable: true },
            // { title: 'Vehicle No', data: 'vehicleNo', width: '50%',defaultContent: '-', sortable: false },
            { title: 'Unit', data: 'unit', width: '15%', defaultContent: '-', sortable: false },
            { title: 'Sub Unit', data: 'subUnit', width: '15%', defaultContent: '-', sortable: false },
        ],
        autoWidth: false,
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

export async function initDriverUploadPage () {
    // show view route module
    $('#view-driver-upload').modal('show');
    driverUploadEventHandler();

    await initDataTables();  
}

let driverUploadIns = null;
const driverUploadEventHandler = function () {
    layui.use('upload', function(){
        let upload = layui.upload;
        if (driverUploadIns) return;
        driverUploadIns = upload.render({
            elem: '#driver-upload',
            url: '/upload/uploadDriver',
            accept: 'file',
            acceptMime: 'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            before: function () {
                $('#loadingModal').modal('show');
            },
            // multiple: true,
            done: function (res) {
                console.log(res)
                $.alert({
                    title: 'Warn!',
                    content: res.respMessage,
                });
                initDataTables();
                setTimeout(() => {
                    $('#loadingModal').modal('hide');
                }, 500)
            },
            error: function (error) {
                console.error(error);
                initCustomModal(`Upload driver failed.`)
            }
        });
    });
}
