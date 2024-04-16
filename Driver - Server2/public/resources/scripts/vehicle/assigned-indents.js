
let userType = Cookies.get('userType')
let table
let tableColumnField = ['', 'vehicleNo', 'username'];
let past = 0;
const resourceType = 'Vehicle'

$(function () {
    $(".status-filter-item").on("click", function() {
        $(".status-filter-item").removeClass("active");
        $(this).addClass("active");
        past = $(this).attr("past");
        FilterOnChange();
    });

    $('.select-warning-input').on('change', function () {
        FilterOnChange();
    });

    $(".search-input-assign").on("keyup", function() {
        FilterOnChange();
    });
})

const initVehicleAssignedIndent = function(showMileageWarning) {
    if (showMileageWarning == 1) {
        $('.status-filter-item').removeClass('active');
        $('.status-filter-item.status-filter-item-past').addClass('active');
        $('.select-warning-input').prop('checked', 'checked');
    }

    table = $('.assigned-indents-table').DataTable({
        "ordering": false,
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
            url: "/vehicle/getVehicleAssignedTasks",
            type: "POST",
            data: function (d) {
                let params = {}
                let searchParam = $(".search-input").val();
                past = $(".status-filter-nav .active").attr("past");
                let warningStatus = $('.select-warning-input').prop('checked') ? 1 : 0;
                params.past = past;
                params.warningStatus = warningStatus;
                params.vehicleNo = currentVehicleNo;
                params.start = d.start
                params.length = d.length
                params.searchParam = searchParam;
                return params
            },
            dataSrc: function(data) {
                $(".indentAssignedCount").text(data.recordsTotal ? data.recordsTotal : 0);
                return data.respMessage;
            }
        },
        "initComplete" : function (settings, json) {
            $('.assigned-indents-table thead tr th:first').removeClass("sorting_asc");
        },
        "createdRow": function (row, data, index) {
        },
        "columnDefs": [
            {
            },
        ],
        "columns": [
            {
                "data": "purpose", "title": "Purpose", "orderable": false
            },
            {
                "data": "unit", "title": "Unit", "orderable": false,
                "render": function (data, type, full, meta) {
                    return `<div><span>${full.unit +'('+ (full.subUnit ? full.subUnit : '') +")"}</span></div>`
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
                "data": "driverName", "title": "Transport Operator", "orderable": false
            },
            {
                "data": "indentStartTime", "title": "Execution Date","orderable": false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        let currentDate = moment();
                        let exeDate = moment(data);
                        if (past == 0) {
                            let days = exeDate.diff(currentDate, 'day');
                            return `<div>In ${days} day</div>
                                <div><span style="color: #6c757d;">${exeDate.format("DD/MM/YYYY, HH:mm")}</span></div>`;
                        } else {
                            let days = currentDate.diff(exeDate, 'day');
                            return `<div>Past ${days} day</div>
                                <div><span style="color: #6c757d;">${exeDate.format("DD/MM/YYYY, HH:mm")}</span></div>`;
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
                            <button class="btn btn-sm custom-btn-blue" style="margin-left: 20px;color: white;font-weight: bold;" 
                                onclick="editTaskMileageInfo('${full.taskId}', ${full.startMileage}, ${full.endMileage})">Edit</button>
                        `
                        let endorseBtn = `
                            <button class="btn btn-sm" style="margin-left: 20px;border: solid 1px #1B9063; width: 80px; background-color: #1B9063;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Endorsed')">Endorse</button>
                            <button class="btn btn-sm custom-btn-gray" style="width: 80px;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Cancelled')">Cancel</button>
                            <button class="btn btn-sm custom-btn-danger" style="width: 80px;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Rejected')">Reject</button>  
                        `
                        let approveBtn = `
                            <button class="btn btn-sm" style="margin-left: 20px;border: solid 1px #1B9063; width: 80px; background-color: #1B9063;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Approved')">Approve</button>
                            <button class="btn btn-sm custom-btn-gray" style="width: 80px;color: white;font-weight: bold;" 
                                onclick="updateTaskMileageStatus('${full.taskId}', 'Cancelled')">Cancel</button>  
                            <button class="btn btn-sm custom-btn-danger" style="width: 80px;color: white;font-weight: bold;" 
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
}

const FilterOnChange = async function () {
    await table.ajax.reload(null, true)
}

