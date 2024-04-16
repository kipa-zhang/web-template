
let userType = Cookies.get('userType')
let table
let tableColumnField = ['vehicleNo', 'unit'];
let vehicleStatusSelect = null;

window.reloadHtml = function () {
    table.ajax.reload(null, true)
}

let vehicleStatusColor = [{status: 'Deployed', color: '#FAD028'}, {status: 'Loan Out', color: 'blue'}, {status: 'Deployable', color: '#6EB825'}, 
    {status: 'Under Maintenance', color: '#f705ce'}, {status: 'Out Of Service', color: '#3e3b3b'}, {status: 'On Hold', color: 'red'}]

$(function () {

    table = $('.saf-vehicle-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "searching": false,
        "ordering": true,
        //"stateSave": true,
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
            url: "/vehicle/getVehicleTasks",
            type: "POST",
            data: function (d) {
                let params = GetFilterParameters()
                params.start = d.start
                params.length = d.length
                let order = d.order;
                for (let orderField of order) {
                    if (tableColumnField[orderField.column] == "vehicleNo") {
                        params.vehicleNoOrder = orderField.dir;
                    }
                    if (tableColumnField[orderField.column] == "unit") {
                        params.hubOrder = orderField.dir;
                    }
                }
                return params
            },
        },
        "initComplete": function (settings, json) {
            // $(".saf-vehicle-table thead tr th:first").append(`<input type="checkbox" class="checkAll" onchange="checkAllOrNot()" />`);
            // $(".saf-vehicle-table thead tr th:first").removeClass('sorting_asc');
        },  
        "columns": [
            // { 
            //     data: 'vehicleNo',  orderable: false, width: '5%', 
            //     render: function (data, type, full, meta) {
            //         $(".checkAll").prop("checked", false);
            //         return `<input class="checkVehicle" type="checkbox" vehicleLocation="${full.position ? full.position : ''}" value="${data}">`;
            //     }
            // },
            {
                "data": "vehicleNo", "title": "Vehicle", orderable: true,
                "render": function (data, type, full, meta) {
                    return `<span class="vehicleNo-column" style="font-weight: 600;" onclick="goNavPage('${data}', '${full.currentStatus}')" role="button" tabindex="0">${data}</span><br/>
                            <span style="color: #6c757d;">${full.vehicleType}</span>
                        `;
                }
            },
            {
                "class": "loanTripId-column", "data": "loanIndentId", "title": "Task Id", orderable: false,
                "render": function (data, type, full, meta) {
                    if (full.loanTaskId) {
                        return `<span class="vehicleNo-column" style="font-weight: 600;">${data}</span><br/>
                            <span style="color: #6c757d;">${full.loanTaskId}</span>
                        `;
                    } else {
                        return '-';
                    }
                }
            },
            {
                "class": "text-center", "data": "unit", "title": "Ownership", orderable: false,
                "render": function (data, type, full, meta) {
                    return `<div>${full.unit ? full.unit : ''}</div>
                        <div><span style="color: #6c757d; font-size: 0.75rem;">${full.subUnit ? full.subUnit : '-'}</span></div>`
                }
            },
            {
                "class": "text-center hotoUnit-column", "data": "hotoUnit", "title": "Current Node", orderable: false,
                "render": function (data, type, full, meta) {
                    return `<div>${full.currentUnit ?? '-'}</div>
                        <div><span style="color: #6c757d; font-size: 0.75rem;">${full.currentSubUnit ?? '-'}</span></div>`
                }
            },
            {
                "class": "text-center customer-loan-date-column", "data": "loanStartDate", "title": "Loan Date", orderable: false,
                "render": function (data, type, full, meta) {
                    return `Start: <label>${ data ? moment(data).format('DD/MM/YYYY HH:mm:ss') : '-' }</label> 
                    <br> End: <label>${ full.loanEndDate ? moment(full.loanEndDate).format('DD/MM/YYYY HH:mm:ss') : '-' }</label>`
                }
            },
            {
                "data": "nextAviTime", "title": "Next Avi", orderable: false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return moment(data).format("DD/MM/YYYY");
                    }
                    return "-"
                }
            },
            // {
            //     "data": "nextPmTime", "title": "Next Pm", orderable: false,
            //     "render": function (data, type, full, meta) {
            //         if (data) {
            //             return moment(data).format("DD MMM YY");
            //         }
            //         return "-"
            //     }
            // },
            {
                "data": "nextWpt1Time", "title": "Next Wpt", orderable: false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return moment(data).format("DD/MM/YYYY");
                    }
                    return "-"
                }
            },
            {
                "data": "location", orderable: false,
                "title": "<label id='locationTh'>Location<label>",
                "render": function (data, type, full, meta) {
                    return `<img alt="" onclick="viewVehicleMap('${full.vehicleNo}')" role="button" style="width: 20px; cursor: pointer; margin-top: -4px;" src="../scripts/driverTo/icons/map.svg">`;
                }
            },
            {
                "data": "indentStartTime", "title": "Upcoming Indent", orderable: false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `<div>${moment(data).format("DD/MM/YYYY, HH:mm")}</div>
                        <div><span style="color: #6c757d; font-size: 0.75rem;">#${full.indentId}</span></div>`
                    }
                    return "-"
                }
            },
            {
                "data": "upcomingEvent",
                "title": "Upcoming Event",
                "orderable": false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `
                            <div onclick="markAsUnAvailable(${meta.row})" role="button">
                                <div>${data.startTime ? moment(data.startTime).format("DD/MM/YYYY, HH:mm") : '-'}</div>
                                <div>${data.endTime ? moment(data.endTime).format("DD/MM/YYYY, HH:mm") : '-'}</div>
                            </div>
                        `
                    }
                    return "-"
                }
            },
            {
                "data": "currentStatus", "title": "Status", orderable: false,
                render: function (data, type, full, meta) {
                    // if (data && data == 'waitcheck') {
                    //     data = 'Pending'
                    // }
                    // data = _.capitalize(data)
                    let statusColor = vehicleStatusColor.find(item => item.status == data)
                    if (!statusColor) {
                        statusColor = 'orange';
                    } else {
                        statusColor = statusColor.color;
                    }
                    return `
                        <div class="div-table">
                            <div class="div-table-cell">
                                <div class="circle-status">
                                    <div class="div-table">
                                        <div class="div-table-cell">
                                            <div class="div-circle driver-statusColor" style="background-color: ${statusColor};"></div>
                                        </div>
                                        <div class="div-table-cell">
                                            <label class="driver-status">${ data }</label>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    `;
                }
            },
            {
                "data": "vehicleNo", "title": "Action",
                class: 'vehicle-action',
                orderable: false, width: '100px', 
                "render": function (data, type, full, meta) {
                    let result = ``;
                    let operationList = (full.operation).toLowerCase().split(',')
                    if (full.currentStatus == 'Deactivate') {
                        if (operationList.includes('reactivate')) {
                            result = `
                                <img alt="" src='../images/Reset.svg' style='width: 24px; height: 24px; ' onclick="reactivateVehicle('${data}')" role="button" title='Reactivate'/>
                            `
                        }
                        return `<div style="width: 100%; height: 30px; display: flex; justify-content: center; align-items: center;">${result}</div>`;
                    }
                    if (full.currentStatus != 'Loan Out' && (operationList.includes('mark event') || operationList.includes('cancel event') || operationList.includes('update event'))) {
                        result += `
                            <img alt="" src='../images/Mark Leave.svg' style='width: 24px; height: 24px; margin-left: 5px;' onclick="markAsUnAvailable(${meta.row},true)" role="button" title='Mark/Cancel/Update Event'/>
                        `    
                    }
                    // if(Cookies.get('userType').toUpperCase() != 'CUSTOMER'){
                        if (operationList.includes('deactivate')) {
                            result += `
                                <img alt="" src='../images/Deactivate.svg' style='width: 22px; height: 22px; margin-left: 5px;' onclick="deleteVehicle('${data}')" role="button" title='Deactivate'/>
                            `
                        }
                    // } else 
                    if (full.currentStatus == 'Deployable' && full.loanTaskId && operationList.includes('return')) {
                        result += `
                            <img alt="" src='../images/returnResources.svg' style='width: 24px; height: 24px; margin-left: 5px;' onclick="returnResources('${full.vehicleNo}')" role="button" title='Return'/>
                        `
                    }
                    if (operationList.includes('viewindent') && full.currentStatus != 'Deactivate' && full.vehicleMileageWaringTaskNum > 0) {
                        result += `
                            <img alt="" src='../images/warn-mileage.svg' style='width: 26px; height: 26px; margin-left: 5px;' onclick="viewMileageWarnIndent('${full.vehicleNo}')" role="button" title='Mileage Warning'/>
                        `
                    }        
                    return `<div style="width: 60px; height: 30px; display: flex; justify-content: flex-start; align-items: center;">${result}</div>`;
                }
            }
        ]
    });

    InitFilter();
    // if(Cookies.get('userType').toUpperCase() == 'CUSTOMER') {
    //     $('.vehicle-opt-btn').hide()
    //     table.column(3).visible(false);
    // } else {
        table.column(1).visible(false);
        table.column(4).visible(false);
    // }
})

const goNavPage = function (vehicleNo, currentStatus) {
    //window.location.href = "/vehicle/vehicleDetail?taskId=" + taskId + "&vehicleNo=" + vehicleNo;
    if (currentStatus != 'Deactivate') {
        window.open("/vehicle/vehicleDetail?vehicleNo=" + vehicleNo);
    }
}

const viewMileageWarnIndent = function (vehicleNo) {
    window.open("/vehicle/vehicleDetail?vehicleNo=" + vehicleNo + "&defaultTab=indents");
}

const checkAllOrNot = function () {
    let checkAll = $(".checkAll").prop("checked");
    if (checkAll === true) {
        $(".checkVehicle").each(function () {
            if ($(this).attr("disabled") != 'disabled') {
                $(".checkVehicle").prop("checked", false);
            }
        });
    } else {
        $(".checkVehicle").prop("checked", false);
    }
}

const GetFilterParameters = function () {
    let status = vehicleStatusSelect ? vehicleStatusSelect.getValue() : '';
    let vehicleDataType = $("#vehicle-filter select[name='vehicleDataType']").val()
    let aviDate = $("#vehicle-filter input[name='aviDate']").val()
    let pmDate = $("#vehicle-filter input[name='uqmDate']").val()
    let wptDate = $("#vehicle-filter input[name='wptDate']").val()
    let searchParam = $(".search-input").val()
    let unit = Cookies.get('selectedUnit') ? Cookies.get('selectedUnit') : Cookies.get('hub')
    let subUnit = Cookies.get('selectedSubUnit') ? Cookies.get('selectedSubUnit') : Cookies.get('node')
    
    let selectGroup = Cookies.get('selectedCustomer') == '1' ? 1 : 0
    let groupId = Cookies.get('selectedGroup')
    return {
        selectGroup, groupId,
        "unit": unit ? unit : '',
        "subUnit": subUnit ? subUnit : '',
        "vehicleStatus": status,
        "vehicleDataType": vehicleDataType,
        "searchParam": searchParam,
        aviDate: aviDate,
        pmDate: pmDate,
        wptDate: wptDate,
    }
}

const CleanAllClick = function () {
    $("#vehicle-filter select[name='unit']").val("")
    $("#vehicle-filter select[name='subunit']").val("")
    $("#vehicle-filter select[name='vehicleStatus']").val("")
    $("#vehicle-filter select[name='vehicleDataType']").val("")
    $(".search-input").val("")
    $("#vehicle-filter input").val("")
    vehicleStatusSelect.clearAll()
    table.ajax.reload(null, true)
}

const InitFilter = async function () {
    initUnitData();

    axios.post("/vehicle/getTOVehicleStatusList").then(async res => {
        let vehicleStatusList = res.data.respMessage;
        // $("#driverStatus").empty();
        // let optionHtml = `<option value="">Status: All</option>`;
        // for (let item in driverStatusList) {
        //     optionHtml += `<option value="${ driverStatusList[item] }" >Status: ${ driverStatusList[item] }</option>`
        // }
        // $("#driverStatus").append(optionHtml);

        let data = []
        for (let item in vehicleStatusList) {
            data.push({ id: vehicleStatusList[item], name: vehicleStatusList[item] })
        }

        vehicleStatusSelect = $("#vehicleStatus").multipleSelect({
            dataKey: 'id',
            dataName: 'name',
            searchable: false,
            data: data,
            selectItemCallback: function () {
                FilterOnChange()
            },
            selectAllCallback: function () {
                FilterOnChange()
            },
            cleanAllCallback: function () {
                FilterOnChange()
            },
        });
    })

    $("#vehicle-filter select[name='unit']").on("change", FilterOnChange)
    $("#vehicle-filter select[name='subunit']").on("change", FilterOnChange)
    $("#vehicle-filter input[name='vehicleStatus']").on("change", FilterOnChange)
    $("#vehicle-filter select[name='vehicleDataType']").on("change", FilterOnChange)
    $(".search-input").on("keyup", function() {
        let searchParam = $(".search-input").val();
        if (!searchParam || searchParam.length >= 3) {
            FilterOnChange();
        }
    })
    // $("#vehicle-filter input[name='executionDate']").on("change", FilterOnChange)
    $("#vehicle-filter button[name='clean-all']").on("click", CleanAllClick)
    $(".dropdown-menu li").on('click', AddNewFilter)
}


const initUnitData = function () {
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

const initSubUnitData = function (unit) {
    axios.post("/unit/getSubUnitPermissionList", { unit: unit }).then(async res => {
        let subunitData = res.respMessage;
        $("#subunit").empty();
        let optionHtml = `<option value="">Class: All</option>`;
        for (let item of subunitData) {
            optionHtml += `<option value="${item.subUnit}" >${item.subUnit}</option>`
        }
        $("#subunit").append(optionHtml);
    })
}

const refreshPocation = function () {
    let vehicleNoTdArray = $(".vehicleNo-column");
    let vehicleNoArray = [];
    for (let tdEle of vehicleNoTdArray) {
        vehicleNoArray.push($(tdEle).text());
    }
    if (vehicleNoArray.length == 0) {
        return;
    }
    axios.post("/vehicle/getVehicleLastPosition", { vehicleNoArray: vehicleNoArray }).then(async res => {
        let vehicleLastPosList = res.result.driverList;
        if (vehicleLastPosList && vehicleLastPosList.length > 0) {
            for (let vehicleLastPos of vehicleLastPosList) {
                $(".location-label-" + vehicleLastPos.vehicleNo).text(vehicleLastPos.position);
            }
        }
    })
}

const deleteVehicle = async function (vehicleNo) {
    await axios.post("/vehicle/getVehicleEffectiveData", { vehicleNo: vehicleNo }).then(async res => {
        if (res.respCode == 1) {
            let effectiveDataList = res.respMessage;
            let taskList = effectiveDataList ? effectiveDataList.taskList : [];
            let hotoList = effectiveDataList ? effectiveDataList.hotoList : [];
            let loanList = effectiveDataList ? effectiveDataList.loanList : [];
            if ((taskList && taskList.length > 0) || (hotoList && hotoList.length > 0) || (loanList && loanList.length > 0)) {
                let startedTask = taskList ? taskList.find(item => item.driverStatus.toLowerCase() == 'started') : null;
                if (startedTask) {
                    $.confirm({
                        title: 'Fail Info',
                        content: `The following task:${startedTask.taskId} is started, can't deactivate Vehicle: ${vehicleNo}.`,
                        buttons: {
                            confirm: {
                                btnClass: 'btn-green',
                                action: function () {
                                    if (res.respCode == 1) {
                                        table.ajax.reload(null, true)
                                    }
                                },
                            }
                        }
                    });
                } else {
                    $('#vehicle-task-info').modal('show');
                    initVehicleTaskList(vehicleNo, effectiveDataList);
                }
            } else {
                $.confirm({
                    title: 'Confirm Deactivate',
                    content: 'Are you sure you want to deactivate Vehicle No. ' + vehicleNo,
                    buttons: {
                        cancel: function () {
                            
                        },
                        confirm: {
                            btnClass: 'btn-green',
                            action: function () {
                                confirmDeleteVehicle(vehicleNo);
                            },
                        }
                    }
                });
            }
        }
    })
}

const reactivateVehicle = function (vehicleNo) {
    $.confirm({
        title: 'Confirm Reactivate',
        content: 'Are you sure you want to reactivate Vehicle No. ' + vehicleNo,
        buttons: {
            cancel: function () {
                
            },
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    confirmReactivateVehicle(vehicleNo);
                },
            }
        }
    });
}

const confirmReactivateVehicle = function(vehicleNo) {
    axios.post("/vehicle/reactivateVehicle", { vehicleNo: vehicleNo }).then(async res => {
        $.confirm({
            title: res.respCode == 1 ? 'Success Info' : 'Fail Info',
            content: res.respMessage,
            buttons: {
                confirm: {
                    btnClass: 'btn-green',
                    action: function () {
                        if (res.respCode == 1) {
                            table.ajax.reload(null, true)
                        }
                    },
                }
            }
        });
    })
}

const FilterOnChange = async function () {
    table.ajax.reload(null, true)
}

const AddNewFilter = function () {
    let active = $(this).hasClass('active')
    let item = $(this).attr("data-item")

    active ? $(this).removeClass('active') : $(this).addClass('active')

    let display = $(`#filter input[name='${item}']`).parent().parent().css('display')
    if (display == "block") {
        $(`#filter input[name='${item}']`).parent().parent().css('display', 'none')
        $(`#filter input[name='${item}']`).attr('disabled', true)
    } else {
        $(`#filter input[name='${item}']`).parent().parent().css('display', 'block')
        $(`#filter input[name='${item}']`).attr('disabled', false)
    }
    $(`#filter input[name='${item}']`).val("")

}

const markAsUnAvailable = function(e, markLeave = false) {
    let data = table.row(e).data()
    $('#vehicle-markAsUnavailable').modal('show');
    let start = moment().format("YYYY-MM-DD")
    let end = moment().format("YYYY-MM-DD")
    if(!markLeave){
        start = moment(data.upcomingEvent.startTime).format("YYYY-MM-DD")
        end = moment(data.upcomingEvent.endTime).format("YYYY-MM-DD")
    }
    initMarkAsUnAvailablePage(data.vehicleNo, start, end);
}

const markAsUnAvailableCallback = function() {
    table.ajax.reload(null, true)
}

const returnResources = function(vehicleNo) {
    $.confirm({
        title: 'Confirm Return',
        content: `<div style="padding-bottom: 10px;">Are you sure to return vehicle[${vehicleNo}] ?</div>
            <textarea id="returnResourceRemarks" style="min-width: 360px;border-color: #ced4da; border-radius: 10px;padding: 5px;" rows="3" placeholder="Please enter the remarks"></textarea>`,
        buttons: {
            confirm: function () {
                let remarks = $("#returnResourceRemarks").val();

                axios.post("/loanOut/returnVehicleResources", { vehicleNo: vehicleNo, returnRemark: remarks }).then(async res => {
                    $.confirm({
                        title: res.respCode == 1 ? 'Success Info' : 'Fail Info',
                        content: res.respMessage,
                        buttons: {
                            confirm: {
                                btnClass: 'btn-green',
                                action: function () {
                                    if (res.respCode == 1) {
                                        table.ajax.reload(null, true)
                                    }
                                },
                            }
                        }
                    });
                })
            },
            cancel: function () {
            }
        }
    });
}