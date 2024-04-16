
let userType = Cookies.get('userType')
let driverAssignedTaskTable
let tableColumnField = ['', 'vehicleNo', 'username'];
let past = 0;
let layer = null;
const resourceType = 'TO'

$(function () {

    $(".status-filter-item").on("click", function() {
        $(".status-filter-item").removeClass("active");
        $(this).addClass("active");
        past = $(this).attr("past");
        FilterOnChange();
    });

    $('.search-input').on('keyup', _.debounce( FilterOnChange, 500 ))

    $('.select-warning-input').on('change', function () {
        FilterOnChange();
    });

    $('.btn-create-incident').on('click', function() {
        $('#create-incident').modal('show');
    })

})

const initDriverAssignedIndent = async function (showMileageWarning) {
    if (showMileageWarning == 1) {
        $('.status-filter-item').removeClass('active');
        $('.status-filter-item.status-filter-item-past').addClass('active');
        $('.select-warning-input').prop('checked', 'checked');
    }

    driverAssignedTaskTable = $('.assigned-indents-table').DataTable({
        "ordering": true,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/driver/getTODriverAssignedTasks",
            type: "POST",
            data: function (d) {
                let params = GetFilerParameters()
                past = $(".status-filter-nav .active").attr("past");
                let warningStatus = $('.select-warning-input').prop('checked') ? 1 : 0;
                params.past = past;
                params.warningStatus = warningStatus;
                params.driverId = currentEditDriverId;
                params.start = d.start
                params.length = d.length
                params.searchCondition = $('.search-input').val().trim()
                return params
            },
        },
        "initComplete" : function (settings, json) {
            // row margin-top: fill by div height;
            // $('.assigned-indents-table tbody tr').before('<div style="width: 100%;height: 10px;"/>');
            // $('.assigned-indents-table thead tr th:first').removeClass("sorting_asc");
        },
        "createdRow": function (row, data, index) {
        },
        "columnDefs": [
            {
            },
        ],
        "columns": [
            {
                "data": "purpose", 
                "title": "Purpose", 
                "orderable": false,
                "render": function (data, type, full, meta) {
                    return `<label style="font-weight: bolder;">${ (full.dataFrom == 'MT-ADMIN' ? 'MT-' : '') + data }</label>`
                }
            },
            {
                "data": "hub", 
                "title": "Reporting Location", 
                "orderable": false,
                "render": function (data, type, full, meta) {
                    return `<div><span style="font-weight: bolder;">${full.hub +'('+ (full.node ? full.node : '-') +")"}</span></div>`
                }
            },
            {
                "class": "text-center", "data": "pickupDestination", "title": "Location","orderable": false,
                "render": function (data, type, full, meta) {
                    if (!data) {
                        return "-";
                    }
                    return `<div style="display: flex;justify-content: center;align-items: center;"><div class="color-pickup-destination">${full.pickupDestination}</div>
                        <div style="text-align: center;"><img alt="" src="/images/right.svg"></div>
                        <div class="color-dropoff-destination">${full.dropoffDestination}</div>`
                }
            },
            {
                "data": "vehicleNo", "title": "Vehicle No.", "orderable": false
            },
            {
                "data": "indentStartTime", "title": "Execution Date", "orderable": false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        let currentDate = moment();
                        let exeDate = moment(data);
                        if (past == 0) {
                            let days = exeDate.diff(currentDate, 'day');
                            return `<div>In ${days} day</div>
                                <div><span style="color: #6c757d;">${exeDate.format("DD/MM/YY, HH:mm")}</span></div>`;
                        } else {
                            let days = currentDate.diff(exeDate, 'day');
                            return `<div>Past ${days} day</div>
                                <div><span style="color: #6c757d;">${exeDate.format("DD/MM/YY, HH:mm")}</span></div>`;
                        }
                        
                    }
                    return "-"
                }
            },
            {
                "class": "text-center", "data": "startMileage", "title": "Start Mileage","orderable": false,
                "render": function (data, type, full, meta) {
                    let oldStartMileage = full.startMileage
                    let newStartMileage = '-';
                    if (full.status && full.status != 'Cancelled' && full.status != 'Rejected') {
                        oldStartMileage = full.oldStartMileage
                        newStartMileage = full.startMileage
                    }
                    oldStartMileage = !oldStartMileage ? '0 km' : oldStartMileage;
                    newStartMileage = !newStartMileage ? '0 km' : newStartMileage;
                    return `
                        <div>Orig: ${oldStartMileage}</div>
                        <div>Edit: ${newStartMileage}</div>
                    `;
                }
            },
            {
                "class": "text-center", "data": "endMileage", "title": "End Mileage","orderable": false,
                "render": function (data, type, full, meta) {
                    let oldEndMileage = full.endMileage
                    let newEndMileage = '-';
                    if (full.status && full.status != 'Cancelled' && full.status != 'Rejected') {
                        oldEndMileage = full.oldEndMileage
                        newEndMileage = full.endMileage
                    }
                    oldEndMileage = !oldEndMileage ? '0 km' : oldEndMileage;
                    newEndMileage = !newEndMileage ? '0 km' : newEndMileage;
                    return `
                        <div>Orig: ${oldEndMileage}</div>
                        <div>Edit: ${newEndMileage}</div>
                    `;
                }
            },
            {
                "class": "text-center", "data": "mileageTraveled", "title": "Total Mileage","orderable": false,
                "render": function (data, type, full, meta) {
                    if (!data) {
                        return "-";
                    } else if (data > 100) {
                        return `${data} km <img alt="" style="width: 20px; cursor: pointer; margin-top: -4px; padding-left: 4px;" src="../images/warn-mileage.svg">`;
                    }
                    return data + ' km'
                }
            },
            {
                "class": "text-center", "data": "endMileage", "title": "Action","orderable": false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        let operationList = full.operation.split(',')
                        let editBtn = `
                            <button class="btn btn-sm custom-btn-blue" style="margin-left: 20px; width: 80px;color: white;font-weight: bold;" 
                                onclick="editTaskMileageInfo('${full.taskId}', ${full.startMileage}, ${full.endMileage})">Edit</button>
                        `
                        let endorseBtn = `
                            <button class="btn btn-sm" style="margin-left: 20px;border: solid 1px #1B9063; width: 80px; background-color: #1B9063;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Endorsed')">Endorse</button>
                            <button class="btn btn-sm custom-btn-gray" style="width: 80px; background-color: #1B9063;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Cancelled')">Cancel</button>
                            <button class="btn btn-sm custom-btn-danger" style="width: 80px;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Rejected')">Reject</button>  
                        `
                        let approveBtn = `
                            <button class="btn btn-sm" style="margin-left: 20px;border: solid 1px #1B9063; width: 80px; background-color: #1B9063;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Approved')">Approve</button>
                            <button class="btn btn-sm custom-btn-gray" style="width: 80px; color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Cancelled')">Cancel</button>
                            <button class="btn btn-sm custom-btn-danger" style="width: 80px; color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Rejected')">Reject</button> 
                        `
                        if (!full.status || full.status == 'Cancelled' || full.status == 'Rejected') {
                            if (operationList.includes('Edit')) {
                                return editBtn
                            } else {
                                return full.status ?? '-'
                            }
                        } else if (full.status.toLowerCase() == 'edited') {
                            if (operationList.includes('Endorse')) {
                                return endorseBtn
                            } else {
                                return full.status ?? '-'
                            }
                        } else if (full.status.toLowerCase() == 'endorsed') {
                            if (operationList.includes('Approve')) {
                                return approveBtn
                            } else {
                                return full.status ?? '-'
                            } 
                        } else if (full.status.toLowerCase() == 'approved') {
                            return 'Approved'
                        }
                    }
                    return "-"
                }
            }
        ]
    });

    setTimeout(() => {
        if (past == 0) {
            if(driverAssignedTaskTable.rows()[0]) $('.circle-number.indentAssignedCount').html(driverAssignedTaskTable.rows()[0].length);
        }
    }, 100)
}

const GetFilerParameters = function () {
    let unit = Cookies.get('selectedUnit')
    let subUnit = Cookies.get('selectedSubUnit')
    // console.log(unit)
    // console.log(subUnit)
    return { unit, subUnit }
}

const CleanAllClick = function () {
    $("#vehicle-filter select[name='unit']").val("")
    $("#vehicle-filter select[name='subunit']").val("")
    $("#vehicle-filter select[name='vehicleStatus']").val("")
    $("#vehicle-filter input[name='vehicleNumber']").val("")
    $("#vehicle-filter input[name='executionDate']").val("")
    driverAssignedTaskTable.ajax.reload(null, true)
}

const InitFilter = async function () {
    initUnitData();
   
    $("#vehicle-filter select[name='unit']").on("change", FilterOnChange)
    $("#vehicle-filter select[name='subunit']").on("change", FilterOnChange)
    $("#vehicle-filter select[name='vehicleStatus']").on("change", FilterOnChange)
    $("#vehicle-filter input[name='vehicleNumber']").on("keyup", FilterOnChange)
    $("#vehicle-filter input[name='executionDate']").on("change", FilterOnChange)
    $("#vehicle-filter button[name='clean-all']").on("click", CleanAllClick)
}

const initUnitData = function() {
    axios.post("/unit/getUnitPermissionList", {}).then(async res => {
        let unitData = res.data.respMessage;
        $("#unit").empty();
        let optionHtml = `<option value="">Camps: All</option>`;
        for (let item of unitData) {
            optionHtml += `<option value="${item.unit}" >${item.unit}</option>`
        }
        $("#unit").append(optionHtml);
    })

    $("#unit").on("change", function () {
        let unit = $(this).val()
        initSubUnitData(unit);
    });
}

const initSubUnitData = function(unit) {
    axios.post("/unit/getSubUnitPermissionList", {unit: unit}).then(async res => {
        let subunitData = res.respMessage;
        $("#subunit").empty();
        let optionHtml = `<option value="">Class: All</option>`;
        for (let item of subunitData) {
            optionHtml += `<option value="${item.subUnit}" >${item.subUnit}</option>`
        }
        $("#subunit").append(optionHtml);
    })
}

const FilterOnChange = async function () {
    await driverAssignedTaskTable.ajax.reload(function () {
        //$('.circle-number.indentAssignedCount').html(driverAssignedTaskTable.rows()[0].length)
    }, true)
    

    setTimeout(function() {
        // $('.assigned-indents-table tbody tr').before('<div style="width: 100%;height: 10px;"/>');
        $('.assigned-indents-table thead tr th:first').removeClass("sorting_asc");
    }, 1000);
}

window.reloadHtml = function () {
    window.location.reload();
}
