
$(function () {
    
})

let vehicleList = [
];
let deviceViewDataTable = null;

export async function initVehicleList () {
    const getVehicleList = function () {
        return axios.post('/vehicle/getVehicleList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    vehicleList = await getVehicleList();
    return vehicleList;
};

const initDataTables = async function () {
    await initVehicleList();
    
    deviceViewDataTable = $('#vehicle-upload-list').DataTable({
        destroy: true,
        data: vehicleList,
        columns: [
            { title: 'Vehicle No', data: 'vehicleNo', width: '15%', defaultContent: '-', sortable: true },
            { title: 'Hardware ID', data: 'deviceId', width: '20%', defaultContent: '-', sortable: true },
            // { title: 'Drivername', data: 'driverName', width: '35%',defaultContent: '-', sortable: false },
            { title: 'Unit', data: 'unit', width: '15%', defaultContent: '-', sortable: false },
            { title: 'Sub Unit', data: 'subUnit', width: '15%', defaultContent: '-', sortable: false },
        ],
        bFilter: false,
        bInfo: false,
        autoWidth: false,
        lengthChange: false,
        searching: false,
        pageLength: 8,
        // scrollY: 200,
        // scrollCollapse: true,
        // stateSave: true, // keep page, searching, filter
    });
}

export async function initVehicleUploadPage () {
    // show view route module
    $('#view-vehicle-upload').modal('show');

    await initDataTables();
    vehicleUploadEventHandler()
}

let vehicleUploadIns = null;
const vehicleUploadEventHandler = function () {
    layui.use('upload', function(){
        let upload = layui.upload;
        if (vehicleUploadIns) return;
        vehicleUploadIns = upload.render({
            elem: '#vehicle-upload',
            url: '/upload/uploadVehicle',
            accept: 'file',
            acceptMime: 'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            before: function () {
                $('#loadingModal').modal('show');
            },
            // multiple: true,
            done: function (res) {
                $.alert({
                    title: 'Info',
                    content: res.respMessage,
                });

                setTimeout(() => {
                    $('#loadingModal').modal('hide');
                }, 500)
                initDataTables();
            },
            error: function (error) {
                console.error(error);
                initCustomModal(`Upload vehicle failed.`)
            }
        });
    });
}