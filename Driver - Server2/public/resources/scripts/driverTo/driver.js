let userType = Cookies.get('userType')
let table
let tableColumnField = ['driverName', 'hub'];
let driverStatusSelect, permitTypeSelect;

let driverStatusColor = [{status: 'Deployed', color: '#FAD028'}, {status: 'Loan Out', color: 'blue'}, {status: 'Deployable', color: '#6EB825'}, 
    {status: 'On Leave', color: '#3e3b3b'}, {status: 'Permit Invalid', color: 'red'}]

$(function () {
    $('.search-input').on('keyup', function() {
        let searchParam = $(".search-input").val();
        if (!searchParam || searchParam.length >= 4) {
            FilterOnChange();
        }
    });

    initDriverTable();
    InitFilter();
    initClickAddDriver();
})

window.showDriverNric = function(el, nric, type) {
    nric = nric == 'null' ? null : nric;
    let option = $(el).closest('td');
    if(type == 'noShow') {
        option.find('.img-showNRIC').show()
        option.find('.img-noShowNRIC').hide()
        option.find('.view-driver-nric').text(nric ? ((nric).toString()).substr(0, 1) + '****' + ((nric).toString()).substr(((nric).toString()).length-4, 4) : '-')
    } else {
        option.find('.img-noShowNRIC').show()
        option.find('.img-showNRIC').hide()
        option.find('.view-driver-nric').text(nric ? nric : '-')
    }
}

const initDriverTable = function () {
    table = $('.saf-driver-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
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
            url: "/driver/getTODriverList",
            type: "POST",
            data: function (d) {
                let params = GetFilerParameters()
                params.start = d.start;
                params.length = d.length;
                let order = d.order;
                for (let orderField of order) {
                    if(tableColumnField[orderField.column] == "driverName") {
                        params.driverNameOrder = orderField.dir;
                    }
                    if(tableColumnField[orderField.column] == "hub") {
                        params.hubOrder = orderField.dir;
                    }
                }
                params.searchCondition = $('.search-input').val().trim()
                return params
            },
        },
        "initComplete" : function (settings, json) {
            // row margin-top: fill by div height;
            // $('.saf-driver-table tbody tr').before('<div style="width: 100%;height: 10px;"/>');
            $(".saf-driver-table thead tr th:first").removeClass('sorting_asc');
        },
        "columns": [
            // {
            //     "data": null, 
            //     "title": "S/N", 
            //     "orderable": false,
            //     "render": function (data, type, full, meta) {
            //         return `
            //             <input class="show-live-location" type="checkbox" data-driverid="${ full.driverId }">
            //         `
            //     }
            // },
            {
                "data": "driverName",
                "title": "Name",
                "render": function (data, type, full, meta) {
                    let operationList = full.operation.split(',')
                    if (operationList.includes('View Full NRIC')) {
                        return `
                            <div class="view-driver-info" style="cursor: pointer;" onclick="redirectToDriverInfo(${ full.driverId }, '${full.currentStatus}')" role="button" tabindex="0">${ full.driverName }</div>
                            <div><span style="color: #6c757d;">
                            <label class="view-driver-nric">${ full.nric ?  ((full.nric).toString()).substr(0, 1) + '****' + ((full.nric).toString()).substr(((full.nric).toString()).length-4, 4) : '-' }</label>
                            <img alt="" class="img-showNRIC" style="width: 20px; cursor: pointer;" src="../images/show.svg" onclick="showDriverNric(this, '${ full.nric }', 'show')" role="button"/>
                            <img alt="" class="img-noShowNRIC" style="width: 20px; cursor: pointer;display: none;" src="../images/noShow.svg" onclick="showDriverNric(this, '${ full.nric }', 'noShow')" role="button"/>
                            </span></div>
                        `
                    } else {
                        return `
                            <div class="view-driver-info" style="cursor: pointer;" onclick="redirectToDriverInfo(${ full.driverId }, '${full.currentStatus}')" role="button" tabindex="0">${ full.driverName }</div>
                            <div><span style="color: #6c757d;">
                            <label class="view-driver-nric">${ full.nric ?  ((full.nric).toString()).substr(0, 1) + '****' + ((full.nric).toString()).substr(((full.nric).toString()).length-4, 4) : '-' }</label>
                            </span></div>
                        `
                    }
                }
            },
            {
                "class": "loanTripId-column", "data": "loanIndentId", "title": "Task Id", orderable: false,
                "render": function (data, type, full, meta) {
                    return `<span style="font-weight: 600;">${data ?? '-'}</span><br/>
                        <span style="color: #6c757d;">${full.loanTaskId ?? '-'}</span>
                    `;
                }
            },
            {
                "data": "unit",
                "class": "text-center",
                "title": "Ownership",
                orderable: false,
                "render": function (data, type, full, meta) {
                    return `
                        <div>${ full.unit ?? '-'  }</div>
                        <div><span style="color: #6c757d;">${ full.subUnit ?? '-' }</span></div>
                    `
                }
            },
            {
                "class": "text-center hotoUnit-column", "data": "currentUnit", "title": "Current Node", orderable: false, width: '10%', 
                "render": function (data, type, full, meta) {
                    return `<div>${full.currentUnit ?? '-'}</div>
                        <div><span style="color: #6c757d; font-size: 0.75rem;">${full.currentSubUnit ?? '-'}</span></div>`
                }
            },
            {
                "class": "text-center customer-loan-date-column", "data": "loanStartDate", "title": "Loan Date", orderable: false,
                "render": function (data, type, full, meta) {
                    return `Start: <label>${data ? moment(data).format('DD/MM/YYYY HH:mm:ss') : '-'}</label> <br> End: <label>${full.loanEndDate ? moment(full.loanEndDate).format('YYYY-MM-DD HH:mm:ss') : '-'}</label>`
                }
            },
            {
                "data": "totalMileage", 
                "title": "Mileage",
                "orderable": false,
                render: function (data) {
                    return numeral(data).format('0,0')
                }
            },
            {
                "data": "permitType", 
                "title": "Class",
                "orderable": false,
                render: function (data) {
                    if (data) {
                        let permitList = data.split(',');
                        if (permitList.length > 10) {
                            return `${ permitList.slice(0, 5) },<br>${ permitList.slice(5, 10) },<br>${ permitList.slice(10) }`
                        } else if (permitList.length > 5) {
                            return `${ permitList.slice(0, 5) },<br>${ permitList.slice(5) }`
                        } else {
                            return data
                        }
                    } else {
                        return '-';
                    }
                }
            },
            // {
            //     "data": "vehicleTypeList", 
            //     "title": "Platforms",
            //     "orderable": false,
            // },
            {
                "data": null, 
                "title": "Location",
                "orderable": false,
                "render": function (data, type, full, meta) {
                    return `
                        <img alt="" onclick="showLiveLocationHandler(${ full.driverId })" role="button" style="width: 20px; cursor: pointer; margin-top: -4px;" src="../scripts/driverTo/icons/map.svg">
                    `;
                }
            },
            {
                "data": "indentId",
                "title": "Upcoming Indent",
                "orderable": false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `
                            <div>${moment(full.indentStartTime).format("DD/MM/YYYY, HH:mm")}</div>
                            <div style="font-color: #F3F3F3;">#${ full.indentId }</div>
                        `
                    }
                    return "-"
                }
            },
            {
                "data": "upcomingLeave",
                "title": "Upcoming Leave",
                "orderable": false,
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `
                            <div onclick="markAsUnAvailable(${meta.row})" role="button" tabindex="0">
                                <div>${data.startTime ? moment(data.startTime).format("DD/MM/YYYY, HH:mm") : '-'}</div>
                                <div>${data.endTime ? moment(data.endTime).format("DD/MM/YYYY, HH:mm") : '-'}</div>
                            </div>
                        `
                    }
                    return "-"
                }
            },
            {
                "data": "operationallyReadyDate", 
                "title": "ORD Date",
                "orderable": false,
                render: function (data) {
                    if (data) {
                        if (moment().format("YYYY-MM-DD") >= moment(data).format("YYYY-MM-DD")) {
                            return `<div style="color: red;">${moment(data).format("DD/MM/YYYY")}</div>`
                        } else {
                            return `<div>${moment(data).format("DD/MM/YYYY")}</div>`
                        }
                    }
                    return "-"
                }
            },
            {
                "data": "currentStatus", 
                "title": "Status",
                "class": "text-center", 
                "orderable": false,
                "render": function (data, type, full, meta) {
                    // if (data == 'waitcheck') data = 'Pending'
                    // data = _.capitalize(data)
                    let statusColor = driverStatusColor.find(item => item.status == data)
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
                        <div>${full.lastLoginTime ? moment(full.lastLoginTime).format("DD/MM/YYYY HH:mm") : '-'}</div>
                    `;
                }
            },
            {
                "data": "driverId", "title": "Action", orderable: false,
                class: 'driver-action', width: '100px', 
                "render": function (data, type, full, meta) {
                    let result = ``;

                    let operationList = full.operation.split(',')

                    if (full.currentStatus == 'Deactivate' && operationList.includes('Reactivate')) {
                        result = `
                            <img alt="" src='../images/Reset.svg' style='width: 24px; height: 24px; ' onclick="reactivateDriver('${data}', '${ full.driverName }')" role="button" title='Reactivate'/>
                        `
                        return `<div style="width: 100%; height: 30px; display: flex; justify-content: center; align-items: center;">${result}</div>`;
                    }

                    if (!full.operationallyReadyDate || moment(full.operationallyReadyDate).format("YYYY-MM-DD") > moment().format("YYYY-MM-DD")) {
                        if (full.currentStatus != 'Loan Out' && (operationList.includes('Mark Leave') || operationList.includes('Cancel Leave') || operationList.includes('Update Leave'))) {
                            result += `
                                <img alt="" src='../images/Mark Leave.svg' style='width: 24px; height: 24px; margin-left: 5px;' onclick="markAsUnAvailable(${meta.row},true)" role="button" title='Mark/Cancel/Update Leave'/>
                            `;
                        }
                    }

                    if(operationList.includes('Deactivate')) {
                        result += `
                            <img alt="" src='../images/Deactivate.svg' style='width: 22px; height: 22px; margin-left: 5px;' onclick="deleteDriver('${data}', '${ full.driverName }')" role="button" title='Deactivate'/>
                        `
                    } else if (full.currentStatus == 'Deployable' && full.loanTaskId && operationList.includes('Return')) {
                        result += `
                            <img alt="" src='../images/returnResources.svg' style='width: 24px; height: 24px; margin-left: 5px;' onclick="returnResources('${full.driverId}', '${full.driverName}')" role="button" title='Return'/>
                        `
                    }
                    if (operationList.includes('ViewIndent') && full.currentStatus != 'Deactivate' && full.driverMileageWaringTaskNum > 0) {
                        result += `
                            <img alt="" src='../images/warn-mileage.svg' style='width: 26px; height: 26px; margin-left: 5px;' onclick="viewMileageWarnIndent('${full.driverId}')" role="button" title='Mileage Warning'/>
                        `
                    }
                    return `<div style="width: 60px; height: 30px; display: flex; justify-content: flex-start; align-items: center;">${result}</div>`;
                }
            }
        ]
    });

    // if(Cookies.get('userType').toUpperCase() == 'CUSTOMER') {
    //     table.column(3).visible(false);
    // } else {
        table.column(1).visible(false);
        table.column(4).visible(false);
    // }
}

const GetFilerParameters = function () {
    // let driverStatus = $("#driver-filter select[name='driverStatus']").val()
    let driverStatus = driverStatusSelect ? driverStatusSelect.getValue() : '';
    // let permitType = $("#driver-filter select[name='permitType']").val()
    let permitType = permitTypeSelect ? permitTypeSelect.getValue() : '';
    // let unit = $("#driver-filter select[name='unit']").val()
    // let subUnit = $("#driver-filter select[name='subunit']").val()
    let driverDataType = $("#driver-filter select[name='driverDataType']").val()

    let driverORDStatus = $(".driverORDStatus").val();
    
    let unit = Cookies.get('selectedUnit') ? Cookies.get('selectedUnit') : Cookies.get('hub')
    let subUnit = Cookies.get('selectedSubUnit') ? Cookies.get('selectedSubUnit') : Cookies.get('node')
    let selectGroup = Cookies.get('selectedCustomer') == '1' ? 1 : 0
    let groupId = Cookies.get('selectedGroup')
    // console.log(unit)
    // console.log(subUnit)
    return { unit, subUnit, selectGroup, groupId, driverStatus, permitType, driverORDStatus, driverDataType }
}

const CleanAllClick = function () {
    $("#driver-filter select[name='unit']").val("")
    $("#driver-filter select[name='subunit']").val("")
    $("#driver-filter select[name='driverDataType']").val("")
    $(".search-input").val("");
    $(".driverORDStatus").val("");
    // $("#driver-filter select[name='permitType']").val("")
    // $("#driver-filter select[name='driverStatus']").val("")
    driverStatusSelect.clearAll()
    permitTypeSelect.clearAll()
    table.ajax.reload(null, true)
}

const InitFilter = async function () {
    const initUnitData = function() {
        axios.post("/unit/getUnitPermissionList", {}).then(async res => {
            let unitData = res.data.respMessage;
            $("#unit").empty();
            let optionHtml = `<option value="">Hub: All</option>`;
            for (let item of unitData) {
                optionHtml += `<option value="${item.unit}" >Hub: ${item.unit}</option>`
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
            let optionHtml = `<option value="">Node: All</option>`;
            for (let item of subunitData) {
                optionHtml += `<option value="${item.subUnit}" >Node: ${item.subUnit}</option>`
            }
            $("#subunit").append(optionHtml);
        })
    }
    const initPermitTypeData = function () {
        axios.post("/driver/getPermitTypeList").then(async res => {
            let permitTypeList = res.data.respMessage;
            // $("#permitType").empty();
            // let optionHtml = `<option value="">Permit Type: All</option>`;
            // for (let item of permitTypeList) {
            //     optionHtml += `<option value="${item.permitType}" >Permit Type: ${item.permitType}</option>`
            // }
            // $("#permitType").append(optionHtml);

            let data = []
            for (let item of permitTypeList) {
                data.push({ id: item.permitType, name: item.permitType })
            }

            permitTypeSelect = $("#permitType").multipleSelect({
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
    }
    const initDriverStatusData = function () {
        axios.post("/driver/getTODriverStatusList").then(async res => {

            if (res.data.respCode == -100) {
                window.location = '../login'
            }

            let driverStatusList = res.data.respMessage;
            // $("#driverStatus").empty();
            // let optionHtml = `<option value="">Status: All</option>`;
            // for (let item in driverStatusList) {
            //     optionHtml += `<option value="${ driverStatusList[item] }" >Status: ${ driverStatusList[item] }</option>`
            // }
            // $("#driverStatus").append(optionHtml);

            let data = []
            for (let item in driverStatusList) {
                data.push({ id: driverStatusList[item], name: driverStatusList[item] })
            }

            driverStatusSelect = $("#driverStatus").multipleSelect({
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
    }

    initUnitData();
    initPermitTypeData();
    initDriverStatusData();

    $("#driver-filter select[name='unit']").on("change", FilterOnChange)
    $("#driver-filter select[name='subunit']").on("change", FilterOnChange)
    $("#driver-filter select[name='permitType']").on("change", FilterOnChange)
    $("#driver-filter select[name='driverStatus']").on("change", FilterOnChange)
    $(".driverORDStatus").on("change", FilterOnChange);
    $("#driver-filter select[name='driverDataType']").on("change", FilterOnChange);
    $("#driver-filter button[name='clean-all']").on("click", CleanAllClick)
}

const FilterOnChange = async function () {
    await table.ajax.reload(null, true)
    // setTimeout(() => {
    //     $('.saf-driver-table tbody tr').before('<div style="width: 100%;height: 10px;"/>');
    // }, 1000);
}

const redirectToDriverInfo = function (driverId, currentStatus) {
    if (currentStatus != 'Deactivate') { 
        //window.location.href = `../driver/driver-info`;
        window.open("/driver/driver-info?driverId="+driverId);
    }
}

const viewMileageWarnIndent = function (driverId) {
    //window.location.href = `../driver/driver-info`;
    window.open("/driver/driver-info?defaultTab=indents&driverId="+driverId);
}

window.reloadHtml = function () {
    window.location.reload();
}

const initClickAddDriver = function () {
    // if(Cookies.get('userType').toUpperCase() == 'CUSTOMER') {
    //     $('.driver-add-div').hide();
    // }
    $('.driver-add-btn').on('click', function () {
        $('#view-driver-edit').modal('show');
        $('#view-driver-edit .modal-title').text('New Driver');
        $('#view-driver-edit .opt-btn-div-edit').val('create')
        initClickEditDriver()
    });
}

const deleteDriver = async function (driverId, driverName) {
    await axios.post("/driver/getDriverEffectiveData", { driverId: driverId }).then(async res => {
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
                        content: `The following task:${startedTask.taskId} is started, can't deactivate Driver: ${driverName}.`,
                        buttons: {
                            confirm: {
                                btnClass: 'btn-green',
                                action: function () {
                                    if (res.respCode == 1) {
                                        table.ajax.reload(null, true)
                                    }
                                }
                            }
                        }
                    });
                } else {
                    $('#driver-task-info').modal('show');
                    initDriverTaskList(driverId, driverName, effectiveDataList);
                }
            } else {
                $.confirm({
                    title: 'Confirm Deactivate',
                    content: 'Are you sure you want to deactivate Driver:' + driverName,
                    buttons: {
                        cancel: function () {
                            
                        },
                        confirm: {
                            btnClass: 'btn-green',
                            action: function () {
                                confirmDeleteDriver(driverId);
                            }
                        }
                    }
                });
            }
        }
    })
}

const reactivateDriver = function (driverId, driverName) {
    $.confirm({
        title: 'Confirm Reactivate',
        content: 'Are you sure you want to reactivate Driver:' + driverName,
        buttons: {
            cancel: function () {
                
            },
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    confirmReactivateDriver(driverId);
                }
            }
        }
    });
}

const confirmReactivateDriver = function(driverId) {
    axios.post("/driver/reactivateDriver", { driverId: driverId }).then(async res => {
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
                    }
                }
            }
        });
    })
}

const markAsUnAvailable = function(e, markLeave = false) {
    let data = table.row(e).data()
    $('#driver-markAsUnavailable').modal('show');
    let start = moment().format("YYYY-MM-DD")
    let end = moment().format("YYYY-MM-DD")
    if(!markLeave){
        start = moment(data.upcomingLeave.startTime).format("YYYY-MM-DD")
        end = moment(data.upcomingLeave.endTime).format("YYYY-MM-DD")
    }
    initMarkAsUnAvailablePage(data.driverId, data.driverName, start, end);
}

const markAsUnAvailableCallback = function() {
    table.ajax.reload(null, true)
}

const returnResources = function(driverId, driverName) {
    $.confirm({
        title: 'Confirm Return',
        content: `<div style="padding-bottom: 10px;">Are you sure return driver[${driverName}] ?</div>
            <textarea id="returnResourceRemarks" style="min-width: 360px;border-color: #ced4da; border-radius: 10px;padding: 5px;" rows="3" placeholder="Please enter the remarks"></textarea>`,
        buttons: {
            confirm: function () {
                let remarks = $("#returnResourceRemarks").val();

                axios.post("/loanOut/returnDriverResources", { driverId: driverId, returnRemark: remarks }).then(async res => {
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
                                }
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