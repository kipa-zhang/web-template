let currentInterval = null;
let layer = null;
let dataTable

layui.use('layer', function(){
    layer = layui.layer;
});

$(function () {
    $("#indent-filter button[name='clean-all']").on("click", cleanAllClick)
    initSelectedAndPage();
    initDetail();
    clickSelect();
    initLayDate();
    
});

const initSelectedAndPage = async function () {
    const getVehicleType = async function () {
        return axios.post('/indentAssigned/getVehicleType')
        .then(function (res) {
            if (res.data.respCode === 1) {
                return res.data.respMessage;
            } else {
                console.error(res.data.respMessage);
                return null;
            }
        });
    }

    const initVehicleType = function (vehicleTypeList) {
        let html ;
        $("#indent-vehicle-type").empty();
        $("#indent-vehicle-type").append(`<option value="">Vehicle Type: All</option>`)
        for(let vehicleType of vehicleTypeList){
            if(vehicleType.typeOfVehicle) html += `<option value='${ vehicleType.typeOfVehicle }'>${ vehicleType.typeOfVehicle }</option>`;
        }
        $('#indent-vehicle-type').append(html);
    }

    const initTaskState = function () {
        $("#task-status").empty()
        let data = `<option value="">Task Status: All</option>`
        data += `<option value="unassigned">Unassigned</option>`
        data += `<option value="assigned">Assigned</option>`
        data += `<option value="started">Started</option>`
        data += `<option value="arrived">Arrived</option>`
        data += `<option value="Completed">Completed</option>`
        data += `<option value="failed">Failed</option>`
        data += `<option value="cancelled">Cancelled</option>`
        data += `<option value="cancelled(3rd)">Cancelled(3rd)</option>`
        data += `<option value="Late Trip">Late Trip</option>`
        data += `<option value="declined">Declined</option>`
        data += `<option value="No Show">No Show</option>`
        $("#task-status").append(data)
    }

    let vehicleTypeList = await getVehicleType();
    initVehicleType(vehicleTypeList);
    initTaskState();

    $('#collapseExample').show();
    $('.data-detail').show();
    $('.data-sector-detail').hide();
}


// const filterOnChange = async function () {
//     let area = Cookies.get('selectedUnit')
//     let unitId = ''
//     let node = Cookies.get('selectedSubUnit')
//     let status = $("#task-status option:selected").val() ? $("#task-status option:selected").val() : '';
//     let vehicleType = $("#indent-filter select[name='indent-vehicle-type'] option:selected").val() ? $("#indent-filter select[name='indent-vehicle-type'] option:selected").val() : '';
//     let tripNo =  $("#indent-filter input[name='trip-no']").val() ? $("#indent-filter input[name='trip-no']").val() : '';
//     let execution_date = $("#indent-filter input[name='execution-date']").val() ? $("#indent-filter input[name='execution-date']").val() : '';
//     let created_date = $("#indent-filter input[name='created-date']").val() ? $("#indent-filter input[name='created-date']").val() : '';
    
//     initDetail(node, unitId, status, vehicleType, tripNo, execution_date, created_date, area);
// }

const clickSelect = function (){
    $("#indent-filter input[name='trip-no']").on("keyup",
        function () {
            dataTable.ajax.reload(null, true) 
        }
     )
    $("#indent-filter select[name='indent-vehicle-type']").on("change",
        function () {
            dataTable.ajax.reload(null, true) 
        }
    )
    $("#indent-filter select[name='indent-hub-type']").on("change",
        function () {
            dataTable.ajax.reload(null, true) 
        }
    )
    $("#indent-filter select[name='indent-node-type']").on("change",
        function () {
            dataTable.ajax.reload(null, true) 
        }
    )
    $("#task-status").on("change",
        function () {
            dataTable.ajax.reload(null, true) 
        }
    )
}

const initLayDate = function () {
    layui.use('laydate', function(){
        let laydate = layui.laydate;

        laydate.render({
            elem: '#execution-date',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            range: '~',
            btns: ['clear', 'confirm'],
            done: function () {
                dataTable.ajax.reload(null, true)
            }
        });
        laydate.render({
            elem: '#created-date',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            done: function () {
                dataTable.ajax.reload(null, true)
            }
        });
    });
}

const initDetail = async function () {
    const initDataTable = function () {
        dataTable = $('.jobTask-table').DataTable({
            "ordering": true,
            "searching": false,
            "paging": true,
            "autoWidth": false,
            "fixedHeader": true,
            "scrollX": "auto",
            // "scrollY": "700px",
            "scrollCollapse": true,
            "language": PageHelper.language(),
            "lengthMenu": PageHelper.lengthMenu(),
            "dom": PageHelper.dom(),
            "pageLength": PageHelper.pageLength(),
            "processing": false,
            "serverSide": true,
            "destroy": true,
            "sAjaxDataProp": "data",
            "ajax": {
                url: "/indentAssigned/getAssignableTaskList",
                type: "POST",
                data: function (d) {
                    let hub = Cookies.get('selectedUnit')
                    let unitId = ''
                    let node = Cookies.get('selectedSubUnit')
                    let status = $("#task-status option:selected").val() ? $("#task-status option:selected").val() : '';
                    let vehicleType = $("#indent-filter select[name='indent-vehicle-type'] option:selected").val() ? $("#indent-filter select[name='indent-vehicle-type'] option:selected").val() : '';
                    let tripNo =  $("#indent-filter input[name='trip-no']").val() ? $("#indent-filter input[name='trip-no']").val() : '';
                    let execution_date = $("#indent-filter input[name='execution-date']").val() ? $("#indent-filter input[name='execution-date']").val() : '';
                    let created_date = $("#indent-filter input[name='created-date']").val() ? $("#indent-filter input[name='created-date']").val() : '';

                    let option = { 
                        "node": node ? node : '',
                        "unit": unitId, 
                        "status": status, 
                        "vehicleType": vehicleType, 
                        "tripNo": tripNo, 
                        "execution_date": execution_date,
                        "created_date": created_date,
                        "hub": hub,
                        "pageNum": d.start, 
                        "pageLength": d.length
                    }

                    return option
                },
            },
            columns: [
                { 
                    title: 'S/N', 
                    width: '4%', 
                    data: null, 
                    sortable: false,
                    render: function (data, type, full, meta) {
                        return meta.row + 1 + meta.settings._iDisplayStart
                    }
                },
                { 
                    title: 'Trip ID', 
                    width: '6%', 
                    data: 'tripNo', 
                    sortable: false,
                    defaultContent: '-' 
                },
                { 
                    title: 'Job ID', 
                    width: '6%', 
                    data: 'externalJobId', 
                    sortable: false ,
                    defaultContent: '-' 
                },
                { 
                    title: 'Service Mode', 
                    width: '6%', 
                    data: 'serviceMode', 
                    sortable: false ,
                    defaultContent: '-' 
                },
                { 
                    title: 'Task Status', 
                    width: '8%', 
                    data: 'taskStatus', 
                    sortable: false ,
                    defaultContent: '-' 
                },
                { 
                    data: "tsp", 
                    title: "TSP",
                    width: '7%',
                    sortable: false,
                    defaultContent: '-' 
                },
                {
                    title: "Location",
                    data: "pickupDestination", 
                    width: '12%',
                    sortable: false,
                    defaultContent: '-' ,
                    render: function (data, type, full, meta) {
                        if (!data) {
                            return "-";
                        }
                        return `<div class="color-pickup-destination">${ full.pickupDestination }</div>
                            <div class="icon-down-div"><span class="iconfont icon-down"></span></div>
                            <div class="color-dropoff-destination">${ full.dropoffDestination }</div>`
                    }
                },
                { 
                    title: 'POC Name', 
                    width: '9%', 
                    data: 'poc', 
                    sortable: false ,
                    defaultContent: '-' 
                },
                { 
                    title: 'POC Number', 
                    width: '11%', 
                    data: 'pocNumber', 
                    sortable: true ,
                    defaultContent: '-' 
                },
                { 
                    title: 'Driver Name<br>(Driver Status)', 
                    width: '12%', 
                    data: 'name', 
                    sortable: false,
                    defaultContent: '-',
                    render: function (data, type, full, meta) {
                        if (data) {
                            return `${ data }<br>(${ full.status })`
                        }
                        return '-'
                    }
                },
                { 
                    title: 'Driver Number', 
                    width: '12%', 
                    data: 'contactNumber', 
                    sortable: true ,
                    defaultContent: '-' 
                },
                { 
                    title: 'Vehicle Number', 
                    width: '20%', 
                    data: 'vehicleNumber', 
                    sortable: true ,
                    defaultContent: '-' 
                }
            ]
        });
        
    }
    initDataTable()
}

const cleanAllClick = function () {
    $("#task-status").val("")
    $("#indent-filter select[name='indent-vehicle-type']").val("")
    $("#indent-filter input[name='execution-date']").val("")
    $("#indent-filter input[name='created-date']").val("")
    $("#indent-filter input[name='trip-no']").val("")
    dataTable.ajax.reload(null, true)
}

window.reloadHtml = function (hub, node) {
    setTimeout(
        function(){
            dataTable.ajax.reload(null, true)
        },
        300
    )
}