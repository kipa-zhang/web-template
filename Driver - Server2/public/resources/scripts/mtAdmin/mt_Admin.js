let data ;
let userType = Cookies.get('userType');
let userId = Cookies.get('userId');
let userNode = Cookies.get('node')
let dataTable
let driverObj = null;
let vehicleTypeList;
let mtUnitId = null;
let driverList = null;
let vehicleList = null;
let editCreator = null;
let titleText = 'MT-Admin Task'
$(async function () {
    initDateTime();
    initDate()
    initDetail();
    addMtAdmin();
    initMtAdminPage()
    clickSelect()
    $('.mtAdminCancel').off('click').on('click', function () {
        clearPageData();
    });

    $(document).off('click').on("click", function (e) {
        let target = e.target;
        if (target.id != "search1" && target.id != "search2" && target.id != "pickupDestination" && target.id != "dropoffDestination"
         && target.id != "search3" && target.id != "search4" && target.id != "vehicleNo" && target.id != "driver" && target.id != "search5" && target.id != "typeOfVehicle") {
            $('.search-select').css("display", "");
            $('.unit-search-select').css("display", "");
        }
    });
    initVehicleType()
    $('.select-group-div').hide()
    if(userType.toUpperCase() == 'HQ' || userType.toUpperCase() == 'CUSTOMER' || userType.toUpperCase() == 'ADMINISTRATOR') {
        $('.select-group-div').show()
        initGroup()
    }
})

window.getUserByUserId = async function(creator) {
    return await axios.post('/mtAdmin/getUserByUserId', { creator: creator })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
    });
}

const clickSelect = function () {
    $('#clearAll').off('click').on('click', function () {
        $(".selected-Purpose").val("")
        $(".execution-date").val("")
        $(".created-date").val("")
        if(userType.toUpperCase() != 'CUSTOMER') {
            $(".select-hub").val("all")
            $('.select-hub').trigger('change')
        }
        $('.screen-taskId').val('')
        $('.screen-vehicleNo').val('')
        $('.screen-driverName').val('')
        if(userType.toUpperCase() == 'HQ' || userType.toUpperCase() == 'CUSTOMER' || userType.toUpperCase() == 'ADMINISTRATOR') $('.select-group').val('')
        if(dataTable) dataTable.ajax.reload(null, true)
    });
    if(userType.toUpperCase() != 'CUSTOMER') {
        $(".select-hub").off('change').on("change", function () {
            dataTable.ajax.reload(null, true) 
        })
        $(".select-node").off('change').on("change", function () {
                dataTable.ajax.reload(null, true) 
            })
    }
    $('.screen-taskId').on('keyup', function () {
        dataTable.ajax.reload(null, true) 
    })

    $('.screen-vehicleNo').on('keyup', function () {
        let number = ($(this).val()).split("")
        if(number.length > 2 || number.length == 0) {
            dataTable.ajax.reload(null, true) 
        }
    })
    $('.screen-driverName').on('keyup', function () {
        let number = ($(this).val()).split("")
        if(number.length > 3 || number.length == 0) {
            dataTable.ajax.reload(null, true) 
        }
    })
}

window.showJustification = function (e) {
    let row = dataTable.row($(e).data('row')).data()
    $.alert({
        title: 'Justification',
        content: row.cancelledCause
    });
}

window.showRemarks = function (e) {
    let row = dataTable.row($(e).data('row')).data()
    $.alert({
        title: 'Remarks',
        content: row.remarks
    });
}

window.showActivity = function (e) {
    let row = dataTable.row($(e).data('row')).data()
    $.alert({
        title: 'Activity',
        content: row.activityName
    });
}

const initDetail = function () {
    dataTable = $('.data-list').DataTable({
        "ordering": true,
        "searching": false,
        "paging": true,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollX": "auto",
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
            url: "/mtAdmin/getMtAdminList",
            type: "POST",
            data: function (d) {
                let hub = $(".select-hub option:selected").val()
                let node = $(".select-node option:selected").val()
                if(node) node = node.substring(node.lastIndexOf(":")+1, node.length)
                if(hub) hub = hub.toLowerCase() === 'all' ? null : hub;
                if(node) node = node.toLowerCase() === 'all' ? null : node;
                hub = hub ? hub : null;
                node = node ? node : null;
                let taskIdDateOrder
                let endDateOrder
                for (let orderField of d.order) {
                    if(orderField.column == 0) {
                        taskIdDateOrder = orderField.dir;
                    } else if(orderField.column == 11) {
                        endDateOrder = orderField.dir;
                    }
                }    
                let option = { 
                    'purpose': $('.selected-Purpose').val(),
                    'execution_date': $('.execution-date').val() ? $('.execution-date').val() : null,
                    'created_date': $('.created-date').val() ? $('.created-date').val() : null,
                    'hub': hub,
                    'node': node,
                    "userId": userId, 
                    "taskId": $('.screen-taskId').val() ? $('.screen-taskId').val() : null,
                    "vehicleNo": $('.screen-vehicleNo').val() ? $('.screen-vehicleNo').val() : null,
                    "driverName": $('.screen-driverName').val() ? $('.screen-driverName').val() : null,
                    "endDateOrder": endDateOrder,
                    "taskIdDateOrder": taskIdDateOrder,
                    "groupId": $('.select-group').val() ? $('.select-group').val() : null,
                    "pageNum": d.start, 
                    "pageLength": d.length
                }
                if (option.execution_date) {
                    if (option.execution_date.indexOf('~') != -1) {
                        const dates = option.execution_date.split(' ~ ')
                        if(dates.length > 0) {
                            dates[0] = moment(dates[0], 'DD/MM/YYYY').format('YYYY-MM-DD')
                            dates[1] = moment(dates[1], 'DD/MM/YYYY').format('YYYY-MM-DD')
                            option.execution_date = dates.join(' ~ ')
                        }
                    } else {
                        option.execution_date = moment(option.execution_date, 'DD/MM/YYYY').format('YYYY-MM-DD')
                    }
                }
                if(option.created_date) {
                    option.created_date = moment(option.created_date, 'DD/MM/YYYY').format('YYYY-MM-DD')
                }
                return option
            }
        },   
        columns: [
            { 
                data: 'taskId', 
                title: "Task ID",
                sortable: true 
            },
            { 
                data: 'hub', 
                title: "Hub",
                sortable: false 
            },
            { 
                data: 'node', 
                title: "Node",
                sortable: false 
            },
            { 
                title: 'Purpose', 
                data: 'purpose', 
                sortable: false,
                defaultContent: '-' 
            },
            { 
                title: 'Activity', 
                data: 'activityName', 
                sortable: false,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    if (data.length < 20) {
                        return data
                    } else {
                        return `
                            <span class="d-inline-block text-truncate" style="max-width: 90px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showActivity(this);" role="button" tabindex="0">
                                ${ data ? data : '' }
                            </span>
                        `
                    }
                } 
            },
            { 
                title: 'Remarks', 
                data: 'remarks', 
                sortable: false,
                defaultContent: '-' ,
                render: function (data, type, full, meta){
                    if (data) {
                        return `
                        <div>
                            <span class="d-inline-block text-truncate" style="max-width: 90px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showRemarks(this);" role="button" tabindex="0">
                                ${ data ? data : '' }
                            </span><br>
                        </div>
                        `
                    } else {
                        return '-'
                    }
                }
            },
            { 
                title: 'Vehicle No <br/> Resource', 
                data: 'vehicleNumber', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNumber ?? '-' }<br>${ full.vehicleType ?? '-' }`
                } 
            },
            { 
                title: 'Driver Name <br/> (Mobile No.)', 
                data: 'driver_name', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    return `${ data ?? '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
                } 
            },
            {   
                title: "category",
                data: "category", 
                sortable: false ,
                defaultContent: '-' 
            },
            {
                title: "Service Mode",
                data: "serviceMode", 
                sortable: false ,
                defaultContent: '-' ,
            },
            {
                title: "Location",
                data: "reportingLocation", 
                sortable: false ,
                defaultContent: '' ,
                render: function (data, type, full, meta) {
                    if (!data) {
                        return "";
                    }
                    return `<div>
                        <div class="color-pickup-destination">${ full.reportingLocation }</div>
                        <div class="icon-down-div"><span class="iconfont icon-down"></span></div>
                        <div class="color-dropoff-destination">${ full.destination }</div>
                    </div>`
                }
            },
            { 
                title: 'Execution Time', 
                sortable: true ,
                defaultContent: '-'  ,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label> <label>${ full.startDate ? moment(full.startDate).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.endDate ? moment(full.endDate).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                }
            },
            { 
                title: 'Justification', 	
                data: 'cancelledCause', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    if (full.cancelledDateTime) {
                        return `
                        <div>
                            <span class="d-inline-block text-truncate" style="max-width: 90px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this);" role="button" tabindex="0">
                                ${ data ? data : '' }
                            </span><br>
                            <label class="fw-bold">Amended by:</label> <label>${ full.amendedByUsername ? full.amendedByUsername : '' }</label><br>
                            <label class="fw-bold">Date Time:</label> <label>${ moment(full.cancelledDateTime).format('DD/MM/YYYY HH:mm:ss') }</label>
                        </div>
                        `
                    } else {
                        return '-'
                    }
                }
            },
            { 
                title: 'Action', 
                width: '14%', 
                data: 'id', 
                sortable: false,
                defaultContent: '-' ,
                render: function (data, type, full, meta) {
                    if(full.mobileStartTime || (full.driverStatus).toLowerCase() == 'cancelled') {
                        if((full.driverStatus).toLowerCase() == 'waitcheck') {
                            return `<div style="color: '#6C6C6C';font-weight: bold;">Pending</div>`
                        } else {
                            return `${ full.driverStatus ? `<div style="color: '#6C6C6C';font-weight: bold;">${ _.capitalize(full.driverStatus) }</div>` : '' }` 
                        }
                        
                    } else {
                        let html = ``
                        let operation = full.operation.toLowerCase().split(',')
                        if (operation.includes('edit')) {
                            html += `<button type="button" class="px-2 py-0 btn-assigned btn custom-btn-blue" onclick="updateMtAdminById('${data}', '${ full.taskId }')">Edit</button> `
                        }
                        if (operation.includes('cancel')) {
                            html += `<button type="button" class="px-2 py-0 btn-assigned btn btn-danger" onclick="deleteMtAdminById('${data}', '${ full.taskId }')">Cancel</button> `
                        }
                        if (!html) html = `<div style="color: '#6C6C6C';font-weight: bold;">${ _.capitalize(full.driverStatus == 'waitcheck' ? 'Pending' : full.driverStatus) }</div>`
                        return html
                    }
                    
                }
            },
        ],
    });
}

const initHubAndNode = async function () {
    if(!userNode && userType.toUpperCase() != 'CUSTOMER') {
        $('.hub-select-row').show()
        $(".node-select-row").show()
        const getHubNode = async function () {
            return axios.post('/mtAdmin/getHubNode')
                    .then(function (res) {
                        return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
    
        const initUnitTypePage = function (unitList) {
            $('.hub-select').empty();
            $(".node-select").empty();
            // if(unitList.length > 0)unitList = unitList.sort((a,b) => (a.subUnit) =="" ? 1 :(b.subUnit)=="" ? -1 : (a.subUnit)>(b.subUnit) ? 1 :-1);
            let __unitList = unitList.map(unit => { return unit.unit });
            __unitList = Array.from(new Set(__unitList));
            if( __unitList.length > 1){
                let html = `<option></option>`
                for (let __unit of __unitList) {
                    html += `<option name="unitType"  value="${ __unit }">${ __unit }</option>`
                }
                $('.hub-select').append(html); 
            } else {
                let html = `<option name="unitType"  value="${ __unitList[0] }">${ __unitList[0] }</option>`
                $('.hub-select').append(html); 
            }
    
            $('.hub-select').off('change').on('change' , function () {
                let selectedUnit = $(this).val();
                $(".node-select").empty();
                for (let unit of unitList) {
                    if (unit.unit === selectedUnit) {
                        let html2 = ``;
                        if(unit.subUnit){
                            html2 = `<option name="subUnitType" data-unitId="${ unit.id }" value="${ unit.subUnit }">${ unit.subUnit }</option>`
                        }else{
                            html2 = `<option name="subUnitType"  data-unitId="${ unit.id }" value="${ unit.subUnit }">-</option>`
                        }
                        $(".node-select").append(html2);
                    } else {
                        continue;
                    }
                }
                clearDriverAndVehicleAndType()
            })
            $(".node-select").off('change').on('change' , function () {
                clearDriverAndVehicleAndType()
            })

            if(__unitList.length == 1) {
                $('.hub-select').val(__unitList[0])
                $('.hub-select').trigger('change')
                $('.hub-select').css('background-color', 'rgb(233, 236, 239)')
                $('.hub-select').prop('disabled', 'disabled')
            } else {
                $('.hub-select').css('background-color', 'white')
                $('.hub-select').removeAttr('disabled', 'disabled')
            }
        }
    
        initUnitTypePage(await getHubNode())
    }
}

window.initPurpose = async function (creator){
    const getPurposeModelist = async function (creator) {
        return await axios.post('/mtAdmin/getPurposeModelist', { creator: creator })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
    }

    const initPurposeType = function (purposeTypeList) {
        $('#purposeType').empty()
        let html =' <option></option>';
        for(let purposeType of purposeTypeList){
            html+= `<option data-id="${ purposeType.id }">${ purposeType.purposeName }</option>`;
        }
        $('#purposeType').append(html)
    }
    initPurposeType(await getPurposeModelist(creator));
}

const initGroup = async function() {
    const initGroupList = async function () {
        return await axios.post('/getGroupList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
    }
    const initGroupSelect = function (groupList) {
        $('.select-group').empty()
        let html =`<option value=''>Group: All</option>`;
        for(let item of groupList){
            html+= `<option value="${ item.id }">${ item.groupName }</option>`;
        }
        $('.select-group').append(html)
    }
    initGroupSelect(await initGroupList());
    $('.select-group').off('change').on('change', function () {
        dataTable.ajax.reload(null, true)
    })
}

const initMtAdminPage = async function () { 
    const getPurposeModeType = async function () {
        return await axios.get('/mtAdmin/getPurposeModeType')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
    }
    const initPurposeType = function (purposeTypeList) {
        if(userType){
            if(userType.toUpperCase() != 'CUSTOMER') {
                purposeTypeList.push({id: 111, purposeName: 'Maintenance'})
            }
        }
        $('.selected-Purpose').empty()
        let html2 =' <option value="">Purpose:All</option>';
        for(let purposeType of purposeTypeList){
            html2+= `<option data-id="${ purposeType.id }">${ purposeType.purposeName }</option>`;
        }
        $('.selected-Purpose').append(html2)
    }
    initPurposeType(await getPurposeModeType());
    await initPurpose(null)
    const initServiceMode = function (serviceModeList) {
        $('#serviceMode').empty()
        if(userType.toUpperCase() == 'CUSTOMER') {
            for(let serviceMode of serviceModeList){
                $('#serviceMode').append( `<option>${ serviceMode }</option>`);
            }
        } else {
            let html = ``;
            for(let serviceMode of serviceModeList){
                html+= `<option>${ serviceMode }</option>`;
            }
            $('#serviceMode').append(html)
        }
    }

    let serviceModeList = ['1-way', 'Disposal'];
    initServiceMode(serviceModeList)

    $('#pickupDestination').off('click').on('click', function () {
        DestinationOnFocus(this)
    });

    $('#dropoffDestination').off('click').on('click', function () {
        DestinationOnFocus(this)
    });
    const DestinationOnFocus = async function (e) {
        $('.search-select').css("display", "");
        $(".search-select input").css("display", "block");
        $(".search-select input").val("");
        $(e).next().css("display", "block")
        $(e).next().find(".form-search-select").empty()
        let locationList = await GetDestination()
        for (let item of locationList) {
            $(e).next().find(".form-search-select").append(`<li data-id="${item.id}">${item.locationName}</li>`)
        }
    }
   
    let locationList = await GetDestination()
    $(".search-select input").on("keyup", function () {
        let val = $(this).val()
        let filterDestination = locationList.filter(item => item.locationName.toLowerCase().indexOf(val.toLowerCase()) != -1)
        InsertFilterOption(this, filterDestination)
    })
    
    const InsertFilterOption = function (element, filterDestination) {
        $(element).next().empty()
        for (let item of filterDestination) {
            $(element).next().append(`<li data-secured="${item.secured}" data-id="${item.id}">${item.locationName}</li>`)
        }
    }
    $("#typeOfVehicle-shadow input").on("keyup", function () {
        let val = $(this).val()
        if(vehicleTypeList.length > 0){
            let filterDestination = vehicleTypeList.filter(item => item.toLowerCase().indexOf(val.toLowerCase()) != -1)
            InsertFilterOption3(this, filterDestination)
        }
    })
    
    const InsertFilterOption3 = function (element, filterDestination) {
        $(element).next().empty()
        for (let item of filterDestination) {
            $(element).next().append(`<li>${item}</li>`)
        }
    }
    $(".form-search-select").on("mousedown", "li", function () {
        let val = $(this).text()
        let secured = $(this).data("secured")
        let id = $(this).data("id")
        $(this).parent().parent().prev().val(val)
        $(this).parent().parent().prev().attr("data-secured", secured)
        $(this).parent().parent().prev().attr("data-id", id)
        $(this).parent().parent().css("display", "none")
        if ($(this).parent().parent().prev().attr("id") == "dropoffDestination") {
            // initServiceProvider(vehicle, $ServiceMode.val(), dropoffDestination, executionTime)
        } else {
            let attrDisabled = $("#dropoffDestination").attr("disabled")
            if (attrDisabled) {
                $("#dropoffDestination").val(val)
                $("#dropoffDestination").attr("data-secured", secured)
                $("#dropoffDestination").attr("data-id", id)
            }
        }
    })
    $("#vehicleNo-shadow .form-search-select").on("mousedown", "li", function () {
        let val = $(this).text()
        let id = $(this).attr("data-unitid")
        $(this).parent().parent().prev().val(val)
        $(this).parent().parent().prev().attr("data-unitid", id)
        $(this).parent().parent().css("display", "none")
    })
    
    $("#typeOfVehicle-shadow .form-search-select").on("mousedown", "li", function () {
        let val = $(this).text()
        $(this).parent().parent().prev().val(val)
        $('#vehicleNo').val('');
        $('#vehicleNo').attr('data-unitId', null)
        $('#driver').val('');
        $('#driver').attr('data-unitId', null)
        $('#driver').attr('data-id', null)
        $('#driver').attr('data-value', null)
        $('#driver-shadow .form-search-select').empty()   
        $('#vehicleNo-shadow .form-search-select').empty()
        initVehicleDriverPage()
    })
    $('#purposeType').off('change').on('change', async function() {
        if($('#purposeType').val() === 'Others') {
            $('.remarks-field').css('display', '')
        } else {
            $('#remarks').val('')
            $('.remarks-field').css('display', 'none')
        }
        if($('#purposeType').val()) {
            $('#periodStartDate').val('') 
            $('#periodEndDate').val('')
            $('#typeOfVehicle').val('')
            $('#typeOfVehicle-shadow .form-search-select').empty()
            $('#typeOfVehicle').trigger('click')
            $('#vehicleNo').val('');
            $('#vehicleNo').attr('data-unitId', null)
            $('#vehicleNo-shadow .form-search-select').empty()
            $('#driver').val('');
            $('#driver').attr('data-unitId', null)
            $('#driver').attr('data-id', null)
            $('#driver').attr('data-value', null)
            $('#driver-shadow .form-search-select').empty()   
            initDateTime();
        }
        
    });
    $('.selected-Purpose').off('change').on('change', function () {
        dataTable.ajax.reload(null, true)
    })

    const getHubNode = async function(userType, userId) {
        return await axios.post('/mtAdmin/getHubNode', { userType, userId })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        });
    }
    
    const initUnit = async function (unitList) {
        if(unitList) {
            let __unitList = unitList.map(unit => { return unit.unit });
            $('.select-hub').empty();
            __unitList = Array.from(new Set(__unitList));
            let html = `<option value="all">Hub:All</option>`;
            for (let __unit of __unitList) {
                html += `<option name="unitType" value="${ __unit }">${ __unit }</option>`
            }
            $('.select-hub').append(html); 

            $('.select-hub').off('change').on('change' , function () {
                if(unitList.length > 0) {
                    let selectedUnit = $(this).val();
                    $(".select-node").empty();
                    let html2
                    if(unitList.length > 1) {
                        html2 = `<option value="all">Node:All</option>`;
                    } else {
                        html2 = `<option name="subUnitType" value='${ unitList[0].subUnit }'>Node:All</option>`;
                    }
                    for (let unit of unitList) {
                        if (unit.unit === selectedUnit && unit.subUnit) {
                            if((unit.subUnit).toLowerCase() === 'null') continue
                            html2 += `<option name="subUnitType" data-id="${ unit.id }" value='${ unit.subUnit }'>${ unit.subUnit }</option>`
                        } else {
                            continue;
                        }
                    }
                    $(".select-node").append(html2);
                }
                dataTable.ajax.reload(null, true)
            })
        }
    }

    if(userType.toUpperCase() != 'CUSTOMER'){ 
        let unitList = await getHubNode(userType, userId);
       initUnit(unitList); 
    } 

    if(userType.toUpperCase() == 'CUSTOMER' || userNode){
        $('.select-hub-div').hide()
        $('.select-node-div').hide()
        $('.hub-select-row').hide()
        $('.node-select-row').hide()
    } else {
        $('.select-hub-div').show()
        $('.select-node-div').show()
        $('.hub-select-row').show()
        $('.node-select-row').show()
    }

    if(userType.toUpperCase() == 'CUSTOMER'){
        $('.top-title').text('Create Task')
        $('.addMtAdmin').text('+ Add New Task')
        titleText = 'Task'
    }
}

const getVehicleType = async function(purpose, startDate, endDate, unit, subUnit){
    let newUserType = userType
    let editUserId = userId
    if(editCreator){
        let editUser = await getUserByUserId(editCreator);
        if(editUser) {
            newUserType = editUser.userType; 
            editUserId = editUser.userId
        }
    } 
    if(newUserType.toUpperCase() != 'CUSTOMER'){
        return await axios.post('/mtAdmin/getVehicleType', { purpose, startDate, endDate, unit, subUnit })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        });
    } else {
        return await axios.post('/mtAdmin/getVehicleTypeByGroup', { purpose, editUserId, startDate, endDate })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        }); 
    }
    
}

const initVehicleDriverPage = async function () {
    if(!($('#periodEndDate').val() && $('#periodStartDate').val())) {
        $('#typeOfVehicle-shadow .form-search-select').empty()
        return
    }
    if(!$('#typeOfVehicle').val()){
        $('#vehicleNo-shadow .form-search-select').empty()
        $('#driver-shadow .form-search-select').empty()
    }

    const initVehicleDriver = async function (userId) {
        $('#typeOfVehicle').attr("disabled",false);
        // $('.typeOfVehicle-row').css("display", "block");
        const vehicleOnFocus = async function (e) {
            if($('#typeOfVehicle').val()){
                $('.search-select').css("display", "");
                $(".search-select input").css("display", "block");
                $(".search-select input").val("");
                $(e).next().css("display", "block")
                $(e).next().find(".form-search-select").empty()
                let user = await getUnitIdByUserId(userId);
                let unitId = $('.node-select option:selected').attr('data-unitId') ? $('.node-select option:selected').attr('data-unitId') : user.unitId;
                let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                // vehicleList = await getVehicleList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, $('#typeOfVehicle').val(), $('#periodStartDate').val(), $('#periodEndDate').val(), $('.hub-select option:selected').val(), $('.node-select option:selected').val());
                vehicleList = await getVehicleList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, $('#typeOfVehicle').val(), startTime, endTime, $('.hub-select option:selected').val(), $('.node-select option:selected').val());
                $('#vehicleNo-shadow .form-search-select').empty()
                for(let vehicle of vehicleList){
                    $(e).next().find(".form-search-select").append(`<li data-unitId="${ vehicle.unitId }">${vehicle.vehicleNo}</li>`)
                } 
            } 
        }
        $('#vehicleNo').off('click').on('click', function () {
            vehicleOnFocus(this)
        });
        $("#vehicleNo-shadow input").on("keyup", function () {
            if(vehicleList) {
                let val = $(this).val()
                let filterDestination = vehicleList.filter(item => item.vehicleNo.toLowerCase().indexOf(val.toLowerCase()) != -1)
                InsertFilterOption2(this, filterDestination)
            }
        })
        const InsertFilterOption2 = function (element, filterDestination) {
            $(element).next().empty()
            for (let item of filterDestination) {
                $(element).next().append(`<li data-unitId="${item.unitId}">${item.vehicleNo}</li>`)
            }
        }
    }
    initVehicleDriver(userId)

    const initDriver = async function () {
        const driverOnFocus = async function (e) {
            if($('#typeOfVehicle').val()){
                $('.search-select').css("display", "");
                $(".search-select input").css("display", "block");
                $(".search-select input").val("");
                $(e).next().css("display", "block")
                $(e).next().find(".form-search-select").empty();
                let user = await getUnitIdByUserId(userId);
                let unitId = $('.node-select option:selected').attr('data-unitId') ? $('.node-select option:selected').attr('data-unitId') : user.unitId
                let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                // driverList = await getDriverList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, $('#typeOfVehicle').val(), $('#periodStartDate').val(), $('#periodEndDate').val());
                driverList = await getDriverList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, $('#typeOfVehicle').val(), startTime, endTime);
                if(driverList.length > 0) driverList = driverList.sort((a, b) => a.driverName.localeCompare(b.driverName));
                $('#driver-shadow .form-search-select').empty()
                for(let driver of driverList){
                    $(e).next().find(".form-search-select").append(`<li data-id="${driver.driverId}" data-unitId="${ driver.unitId }" value="${driver.driverName}">${driver.driverName}(${ driver.contactNumber ? driver.contactNumber : '-'})</li>`)
                } 
            }
        }

        $('#driver').off('click').on('click', function() {
            driverOnFocus(this)
        } )
        
        $("#driver-shadow input").on("keyup", function () {
            if(driverList) {
                let val = $(this).val()
                let filterDestination = driverList.filter(item => item.driverName.toLowerCase().indexOf(val.toLowerCase()) != -1)
                InsertFilterOption3(this, filterDestination)
            }
        })
        const InsertFilterOption3 = function (element, filterDestination) {
            $(element).next().empty()
            for (let driver of filterDestination) {
                $(element).next().append(`<li data-id="${driver.driverId}" data-unitId="${ driver.unitId }" value="${driver.driverName}">${driver.driverName}(${ driver.contactNumber ? driver.contactNumber : '-'})</li>`)
            }
        }
    
        $("#driver-shadow .form-search-select").on("mousedown", "li", function () {
            let val = $(this).attr("value")
            let text = $(this).text()
            let id = $(this).attr("data-id")
            let unitId = $(this).attr("data-unitId")
            $(this).parent().parent().prev().attr('data-value', val)
            $(this).parent().parent().prev().val(text)
            $(this).parent().parent().prev().attr("data-unitid", unitId)
            $(this).parent().parent().prev().attr("data-id", id)
            $(this).parent().parent().css("display", "none")
        })
    }
    initDriver()
}

const noSecond = function () {
    let timeDom = $('.layui-laydate-footer').find("span[lay-type='datetime']")[0];
    $(timeDom).on('click', function () {
        $(".laydate-time-list>li:last").css("display", "none");
        $(".laydate-time-list>li").css("width", "50%")
        $(".laydate-time-list>li").css("height", "100%")
    });
}

const DisabledLayDate = function () {
    let elem = $(".layui-laydate-content");
    let driverLeaveDays = []
    layui.each(elem.find('tr'), function (trIndex, trElem) {
        layui.each($(trElem).find('td'), function (tdIndex, tdElem) {
            let tdTemp = $(tdElem);
            if (driverLeaveDays && driverLeaveDays.indexOf(tdTemp.attr("lay-ymd")) > -1) {
                tdTemp.addClass('laydate-disabled');
                tdTemp.css('color', 'orange');
            }
        });
    });
}

const clearDriverAndVehicleAndType = function() {
    $('#typeOfVehicle').val('');
    $('#vehicleNo').val('');
    $('#vehicleNo').attr('data-unitId', null)
    $('#vehicleNo-shadow .form-search-select').empty()
    $('#driver').val('');
    $('#driver').attr('data-unitId', null)
    $('#driver').attr('data-id', null)
    $('#driver').attr('data-value', null)
    $('#driver-shadow .form-search-select').empty()   
    
}

const initVehicleType = function () {
    $('#typeOfVehicle').off('click').on('click', function () {
        if ($('#periodStartDate').val() && $('#periodEndDate').val()) {
            resourceOnFocus(this)
        }
    })
}

const verifyVehicleTypeAndVehicleAndDriver = async function () {
    const verifyVehicleType = async function(purpose, startDate, endDate, hub, node, vehicleType){
        if(userType.toUpperCase() != 'CUSTOMER') {
            return await axios.post('/mtAdmin/verifyVehicleType', { startDate, endDate, hub, node, vehicleType })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
        } else {
            return await axios.post('/mtAdmin/getVehicleNoByGroup', { purpose, userId, vehicleType, startDate, endDate })
            .then(function (res) {
                if (res.respCode === 1) {
                    if((res.respMessage).length > 0) {
                        return true
                    } else {
                        return false
                    }
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
        }
    }
    if ($('#periodStartDate').val() && $('#periodEndDate').val() && $('#typeOfVehicle').val()) {
        let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
        let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
        // let newVehicleType = await verifyVehicleType($('#purposeType').val(), $('#periodStartDate').val(), $('#periodEndDate').val(), $('.hub-select option:selected').val(), $('.node-select option:selected').val(), $('#typeOfVehicle').val());
        let newVehicleType = await verifyVehicleType($('#purposeType').val(), startTime, endTime, $('.hub-select option:selected').val(), $('.node-select option:selected').val(), $('#typeOfVehicle').val());
        if(newVehicleType) {
            let user = await getUnitIdByUserId(userId);
            let unitId = $('.node-select option:selected').attr('data-unitId') ? $('.node-select option:selected').attr('data-unitId') : user.unitId
            // let newVehicleList = await getVehicleList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, $('#typeOfVehicle').val(), $('#periodStartDate').val(), $('#periodEndDate').val(), $('.hub-select').val(), $('.node-select').val());
            let newVehicleList = await getVehicleList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, $('#typeOfVehicle').val(), startTime, endTime, $('.hub-select').val(), $('.node-select').val());
            let result = newVehicleList.some(item=> item.vehicleNo == $('#vehicleNo').val())
            if(result){
                let newDriverList = null;
                let user = await getUnitIdByUserId(userId);
                let unitId = $('.node-select option:selected').attr('data-unitId') ? $('.node-select option:selected').attr('data-unitId') : user.unitId
                // newDriverList = await getDriverList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, $('#typeOfVehicle').val(), $('#periodStartDate').val(), $('#periodEndDate').val());
                newDriverList = await getDriverList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, $('#typeOfVehicle').val(), startTime, endTime);
                let driverResult = newDriverList.some(item=> item.driverId == $('#driver').attr('data-id'))
                if(!driverResult) {
                    $('#driver').val('');
                    $('#driver').attr('data-unitId', null)
                    $('#driver').attr('data-id', null)
                    $('#driver').attr('data-value', null)
                    $('#driver-shadow .form-search-select').empty()   
                }
            } else {
                $('#vehicleNo').val('');
                $('#vehicleNo').attr('data-unitId', null)
                $('#vehicleNo-shadow .form-search-select').empty()          
            }
        } else {
            clearDriverAndVehicleAndType()
        }
    } else {
        clearDriverAndVehicleAndType()
    }
}

const initDateTime = async function(){
    layui.use(['laydate'], function () {
        let laydate = layui.laydate;
        let optStr = {
            elem: "#periodStartDate",
            lang: 'en',
            type: 'datetime',
            trigger: 'click',
            format: 'dd/MM/yyyy HH:mm',
            btns: ['clear', 'confirm'],
            holidays: [parent.publidHolidays],
            ready: () => { 
                noSecond()
                DisabledLayDate();
            },
            change: (value) => { 
                noSecond()
                DisabledLayDate();
            },
            done: (value) => {
                if (value) {
                    value = moment(value, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm')
                    let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                    if(endTime) {
                        if (moment(value).isSameOrAfter(moment(endTime))) {
                            $.alert({
                                title: 'Warn',
                                content: 'End time should be later than start time!',
                            });
                            $('#periodEndDate').val(null)
                        }
                        verifyVehicleTypeAndVehicleAndDriver()
                    }
                } else {
                    clearDriverAndVehicleAndType()
                }
            }
        };
        optStr['min'] = moment().format('YYYY-MM-DD HH:mm:ss')
        // if($('#purposeType').val() == 'WPT') {
        //     let newDate = new Date();
        //     let year = newDate.getFullYear();
        //     let month = newDate.getMonth();
        //     let day = newDate.getDate();
        //     let week = newDate.getDay();
        //     let endDate = new Date(year, month, day - week + 7)      
        //     let weekEndDate = moment(endDate).format('YYYY-MM-DD') + ' 23:59:59'
        //     optStr['max'] = moment(weekEndDate).format('YYYY-MM-DD HH:mm:ss')
        // }
        laydate.render(optStr);
    });

    layui.use(['laydate'], function () {
        let laydate = layui.laydate;
        let optStr = {
            elem: '#periodEndDate',
            lang: 'en',
            type: 'datetime',
            trigger: 'click',
            format: 'dd/MM/yyyy HH:mm',
            btns: ['clear', 'confirm'],
            holidays: [parent.publidHolidays],
            ready: () => { 
                noSecond()
                DisabledLayDate();
            },
            change: () => { 
                noSecond()
                DisabledLayDate();
            },
            done: (value) => {
                let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                value = value ? moment(value, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
                if (startTime && value) {
                    if (moment(startTime).isSameOrAfter(moment(value))) {
                        $.alert({
                            title: 'Warn',
                            content: 'End time should be later than start time!',
                        });
                        $('#periodEndDate').val(null)
                    }
                    verifyVehicleTypeAndVehicleAndDriver()
                }  else {
                    clearDriverAndVehicleAndType()
                }
                
            }
        };
        optStr['min'] = moment().format('YYYY-MM-DD HH:mm:ss')
        // if($('#purposeType').val() == 'WPT') {
        //     let newDate = new Date();
        //     let year = newDate.getFullYear();
        //     let month = newDate.getMonth();
        //     let day = newDate.getDate();
        //     let week = newDate.getDay();
        //     let endDate = new Date(year, month, day - week + 7)      
        //     let weekEndDate = moment(endDate).format('YYYY-MM-DD') + ' 23:59:59'
        //     optStr['max'] = moment(weekEndDate).format('YYYY-MM-DD HH:mm:ss')
        // }
        laydate.render(optStr);
    });
}

const initDate = function () {
    layui.use('laydate', function(){
        let laydate = layui.laydate;
        laydate.render({
            elem: '.execution-date',
            format: 'dd/MM/yyyy',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            range: '~',
            btns: ['clear', 'confirm'],
            done: function () {
                dataTable.ajax.reload(null, true)
            }
        });
    });

    layui.use('laydate', function(){
        let laydate = layui.laydate;
        laydate.render({
            elem: '.created-date',
            format: 'dd/MM/yyyy',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            btns: ['clear', 'confirm'],
            done: function () {
                dataTable.ajax.reload(null, true)
            }
        });
    });
}

const getUnitId = async function (unit, subUnit) {
    return axios.post('/mtAdmin/getUnitId', { unit, subUnit })
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const getDriverList = async function (purpose, unitId, vehicleType, startDate, endDate) {
    let newUserType = userType
    let editUserId = userId
    if(editCreator){
        let editUser = await getUserByUserId(editCreator);
        if(editUser) {
            newUserType = editUser.userType; 
            editUserId = editUser.userId
        }
    } 
    if(newUserType.toUpperCase() != 'CUSTOMER'){
        if(unitId && unitId != '' && startDate && endDate && purpose){
            return axios.post('/mtAdmin/getDriverList', { purpose, unitId, vehicleType, startDate, endDate })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
        } else {
        return []
        }
    } else {
        if(startDate && endDate){
            return axios.post('/mtAdmin/getDriverDatatByGroup', { editUserId, vehicleType, startDate, endDate })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
        } else {
        return []
        }
    }
}

const getVehicleList = async function (purpose, unitId, vehicleType,startDate, endDate, unit, subUnit) {
    let newUserType = userType
    let editUserId = userId
    if(editCreator){
        let editUser = await getUserByUserId(editCreator);
        if(editUser) {
            newUserType = editUser.userType; 
            editUserId = editUser.userId
        }
    } 
    if(newUserType.toUpperCase() != 'CUSTOMER'){
        if(startDate && endDate && vehicleType && (unitId || unit || subUnit) && purpose) {
            return axios.post('/mtAdmin/getVehicleList', { purpose, unitId, vehicleType,startDate, endDate, unit, subUnit })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return [];
                }
            });
        } else {
            return []
        }
    } else {
        if(startDate && endDate && vehicleType && purpose) {
            return axios.post('/mtAdmin/getVehicleNoByGroup', { purpose, editUserId, vehicleType, startDate, endDate })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return [];
                }
            });
        } else {
            return []
        } 
    }
}

window.updateMtAdminById = async function (mtAdminId, taskId) {
    const getMtAdminByMtAdminId = async function (mtAdminId) {
        return axios.post('/mtAdmin/getMtAdminByMtAdminId',{ mtAdminId })
        .then(function (res) {
            if(res.respMessage === false){
                $.confirm({
                    title: 'Warn',
                    content: 'The task has already started and cannot be operated.',
                    buttons: {
                        ok: function (){
                            dataTable.ajax.reload(null, false)
                        }
                    }
                });
            } else {
                if (res.respCode === 1) {
                    return res.respMessage; 
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            }
        });
    }
    data = await getMtAdminByMtAdminId(mtAdminId);
    if(data){
        await initPurpose(data.creator)
        editCreator = data.creator
        $('#mtAdminModal').modal("show");
        $('.modal-title').text('Edit ' + titleText);
        $('#purposeType').val(data.purpose);
        $('#purposeType').trigger('change')
        $('#additionalRemarks').val(data.activityName);
        $('#remarks').val(data.remarks);
        $('#serviceMode').val(data.serviceMode);
        if(data.hub){
            if(data.hub != '-') {
                $('.hub-select').val(data.hub);
                $('.hub-select').trigger('change')
                $('.node-select').val(data.node ? data.node : 'null');
            }
        } 
        $('#periodStartDate').val(moment(data.startDate).format('DD/MM/YYYY HH:mm'));
        $('#periodEndDate').val(moment(data.endDate).format('DD/MM/YYYY HH:mm'));
        if(data.endDate) $('#periodEndDate').trigger('change')
        if (data.endDate && data.startDate) {
            resourceOnFocus(this)
        }
        $('#typeOfVehicle').val(data.vehicleType);
        initVehicleDriverPage()
        mtUnitId = data.unitId

        verifyVehicleTypeAndVehicleAndDriver()
        if(data.vehicleNumber) $('#vehicleNo').val(data.vehicleNumber);
        $('#vehicleNo').attr("data-unitid", data.unitId)
        if(data.driverId){
            driverObj = {
                driverName: data.driverName,
                unitId: data.unitId,
                driverId: data.driverId,
                contactNumber: data.contactNumber
            }
    
            $('#driver').attr('data-value', data.driverName)
            $('#driver').val(`${ data.driverName }(${ data.contactNumber ? data.contactNumber : '-' })`)
            $('#driver').attr("data-unitid", data.unitId)
            $('#driver').attr("data-id", data.driverId)
        }
        
        $('#pickupDestination').val(data.reportingLocation);
        $('#dropoffDestination').val(data.destination);
        if(mtAdminId){;
            confirmMtAdmin(mtAdminId, taskId)
        }
    
        $('#mtAdminCancel').off('click').on('click', function () {
            mtAdminId = null;
            clearPageData();
        });
        $('.hub-select-row').hide()
        $('.node-select-row').hide()
    }  
}

window.deleteMtAdminById = async function (mtAdminId, taskId) {
    $.alert({
        title: 'Warn',
        content: `<div style="padding-bottom: 10px;text-align: center;">Please confirm the cancellation and state the reason.</div>
        <textarea id="cancelledCause" style="min-width: 360px;border-color: #ced4da; border-radius: 10px;padding-left: 10px;padding-top: 10px;margin-left: 1.2rem;" rows="3" placeholder="Please enter the cancellation reason"></textarea>`,
        buttons: {
            yes: {
                btnClass: 'custom-btn-green', 
                action: function(){
                    let cancelledCause = null;
                    if($('#cancelledCause').val() == '') cancelledCause = null
                    $('#cancelledCause').val() ? cancelledCause = $('#cancelledCause').val() : cancelledCause = null
                    return axios.post('/mtAdmin/deleteMtAdminByMtAdminId',{ mtAdminId, cancelledCause: cancelledCause, taskId })
                    .then(function (res) {
                        if(res.respMessage === false){
                            $.confirm({
                                title: 'Warn',
                                content: 'The task has already started and cannot be operated.',
                                buttons: {
                                    ok: {
                                        btnClass: 'btn-green',
                                        action: function () {
                                            dataTable.ajax.reload(null, false)
                                        }
                                    }
                                }
                            });
                        } else {
                            if (res.respCode === 1) {
                                dataTable.ajax.reload(null, false)
                                return res.respMessage;
                            } else {
                                $.alert({
                                    title: 'Warn',
                                    content: res.respMessage
                                });
                                dataTable.ajax.reload(null, false)
                                return null;
                            }
                        }
                    });
                }
            },
            no: {
                btnClass: 'btn-outline-secondary', 
                action: function(){
                    dataTable.ajax.reload(null, false)
                }
            }
        }
    });
}

const getUnitIdByUserId = async function (userId) {
    return axios.post('/mtAdmin/getUnitIdByUserId',{ userId })
    .then(function (res) {
        if (res.respCode === 1) {
            return res.respMessage;
        } else {
            console.error(res.respMessage);
            return null;
        }
    });
}

const addMtAdmin = async function () {
    $('#addMtAdmin').off('click').on('click', async function () {
        $('.hub-select').removeAttr('disabled')
        $('.hub-select').css('background-color', 'white')
        $('.node-select').css('background-color', 'white')
        $('.node-select').removeAttr('disabled')
        $('.modal-title').text('New ' + titleText);
        clearPageData();
        initHubAndNode()
        confirmMtAdmin(null);
        await initPurpose(0)
    });

    $('#mtAdminCancel').off('click').on('click', function () {
        $('.hub-select').removeAttr('disabled')
        $('.hub-select').css('background-color', 'white')
        $('.node-select').css('background-color', 'white')
        $('.node-select').removeAttr('disabled')
        clearPageData();
    });
}

const confirmMtAdmin = function (mtAdminId, taskId) {
    const ValidAssignTask = function (data, mtAdminId) {
        let errorLabel = {
            purpose: 'purpose',
            activityName: 'Activity Name',
            remarks: 'remarks',
            category: 'category',
            serviceMode: 'service Mode',
            vehicleType: 'Resource',
            vehicleNumber: 'Vehicle No',
            driverName: 'Driver',
            reportingLocation: 'Reporting Location',
            destination: 'Destination',
            poc: 'POC',
            mobileNumber: 'Mobile Number',
            startDate: 'Start Date',
            endDate: 'End Date',
        }
        if(!userNode && !mtAdminId && userType.toUpperCase() != 'CUSTOMER') errorLabel.unitId = 'Hub/Node'
        for (let key in data) {
            if(!data[key]) {
                if($('#purposeType').val() != 'Others') {
                    if(key == 'remarks') continue
                }  
                // if(userType.toUpperCase() === 'HQ') {
                //     if(key == 'vehicleType') continue
                // }
                $.alert({
                    title: 'Warn',
                    content: `${ errorLabel[key] } is required.`,
                });
                return false;
            }
        }
        return true
    }

    const createMtAdmin = async function (mtAdmin) {
        return axios.post('/mtAdmin/createMtAdmin',{ mtAdmin })
        .then(function (res) {
           return res
        });
    }
    const updateMtAdminByMtAdminId = async function (mtAdmin, taskId) {
        return axios.post('/mtAdmin/updateMtAdminByMtAdminId',{ mtAdmin, taskId, businessType: 'mt-admin' })
        .then(function (res) {
            return res;
        });
    }
    $('#mtAdminConfirm').off('click').on('click', async function () {
        let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
        let endDateTime = endTime ? new Date(Date.parse((moment(endTime).format('YYYY-MM-DD HH:mm:ss')))) : null;
        let nowDateTime = new Date(Date.parse((moment().format('YYYY-MM-DD HH:mm:ss'))));
        if(endDateTime && $('#periodStartDate').val()) {
            if($('#purposeType').val() == 'WPT') {
                let weekStartTime = moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm');
                let newDate = new Date(Date.parse((moment(weekStartTime).format('YYYY-MM-DD HH:mm:ss'))));
                let year = newDate.getFullYear();
                let month = newDate.getMonth();
                let day = newDate.getDate();
                let week = newDate.getDay();
                let endDate = new Date(year, month, day - week + 7)      
                let weekEndDate = moment(endDate).format('YYYY-MM-DD') + ' 23:59:59'
                let weekEndTime = moment(weekEndDate).format('YYYY-MM-DD HH:mm:ss')
                if(endTime > weekEndTime) {
                    $.alert({
                        title: 'Warn',
                        content: `WPT tasks cannot span weeks.`,
                    });
                    return
                }
            }
            if(endDateTime >= nowDateTime){
                let ValidAssignTaskObj = {
                    purpose: $('#purposeType').val(),
                    activityName: $('#additionalRemarks').val(),
                    vehicleNumber: $('#vehicleNo').val(),
                    vehicleType: $('#typeOfVehicle').val() ? $('#typeOfVehicle').val() : '',
                    driverName: $('#driver').attr('data-value'),
                    remarks: $('#remarks').val(),
                    category: 'MV',
                    serviceMode: $('#serviceMode').val(),
                    reportingLocation: $('#pickupDestination').val(),
                    destination: $('#dropoffDestination').val(),
                    startDate: $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null,
                    endDate: $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null,
                }
                if(!userNode && !mtAdminId && userType.toUpperCase() != 'CUSTOMER') ValidAssignTaskObj.unitId = $('.hub-select option:selected').val()
                let state = ValidAssignTask(ValidAssignTaskObj, mtAdminId)
                if(state){
                    $(this).addClass('btn-mtAdminConfirm')
                    let mtAdmin = {
                        id: mtAdminId,
                        activityName: $('#additionalRemarks').val(),
                        purpose: $('#purposeType').val(),
                        unitId: null,
                        startDate: $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null,
                        endDate: $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null,
                        vehicleNumber: $('#vehicleNo').val(),
                        vehicleType: $('#typeOfVehicle').val() ? $('#typeOfVehicle').val() : '',
                        driverName: $('#driver').attr('data-value'),
                        driverId:  $('#driver').attr('data-id'),
                        remarks: $('#remarks').val() ? $('#remarks').val() : '',
                        category: $("input[name='category']:checked").val() ? 'MV' : null,
                        serviceMode: $('#serviceMode').val(),
                        reportingLocation: $('#pickupDestination').val(),
                        destination: $('#dropoffDestination').val(),
                        dataType: 'mt'
                    }
                    if(userType.toUpperCase() != 'CUSTOMER') {
                        if(mtAdminId) {
                            mtAdmin.unitId = mtUnitId
                        } else if(mtAdmin.id === null)  {
                            let unitObj = await getUnitId($('.hub-select option:selected').val(), $('.node-select option:selected').val())
                            mtAdmin.unitId = unitObj.id
                        }
                    }
                    $('#modal-waiting').modal('show');
                    if(mtAdminId) {
                        await updateMtAdminByMtAdminId(mtAdmin, taskId).then((res) => {
                            if (res.respCode === 1) {
                                $('#mtAdminModal').modal('hide');
                                dataTable.ajax.reload(null, false)
                                mtAdmin = null
                                clearPageData(); 
                                return
                            } else {
                                if (res.respMessage == false){
                                    $('#mtAdminModal').modal('hide');
                                    $.alert({
                                        title: 'Warn',
                                        content: 'Fail to modify.',
                                    });
                                    mtAdmin = null
                                    clearPageData();
                                    return
                                } else {
                                    $.alert({
                                        title: 'Warn',
                                        content: res.respMessage,
                                    });
                                    return
                                }
                            }
                           
                            
                        });
                        $(this).removeClass('btn-mtAdminConfirm')
                        $('#modal-waiting').modal('hide');
                    } 
                    else if(mtAdmin.id === null) {
                        await createMtAdmin(mtAdmin).then((res) => {
                            if (res.respCode === 1) {
                                $('#mtAdminModal').modal('hide');
                                dataTable.ajax.reload(null, false)
                                mtAdmin = null
                                clearPageData();
                                return
                            } else {
                                if (res.respMessage === false){
                                    $('#mtAdminModal').modal('hide');
                                    $.alert({
                                        title: 'Warn',
                                        content: 'Creation failure.',
                                    });
                                    mtAdmin = null
                                    clearPageData();
                                    return
                                } else {
                                    $.alert({
                                        title: 'Warn',
                                        content: res.respMessage,
                                    });
                                    return
                                }
                            }
                        });
                        $(this).removeClass('btn-mtAdminConfirm')
                        $('#modal-waiting').modal('hide');
                    }
                }
                dataTable.ajax.reload(null, false) 
            } else {
                $.alert({
                    title: 'Warn',
                    content: `The task time has expired. Please select a new one.`,
                });
            }
        }
    })
}

const clearPageData = function () {
    data = ''
    $('#purposeType').val('');
    $('#purposeType').trigger('change')
    $('#additionalRemarks').val('');
    $('#remarks').val('');
    if(userType.toUpperCase() != 'CUSTOMER') $('#serviceMode').val('');
    $('.hub-select').val('');
    $('.hub-select').trigger('change')
    $('.node-select').val('');
    $('#typeOfVehicle').val('');
    $('#typeOfVehicle').trigger('click')
    $('#typeOfVehicle-shadow .form-search-select').empty()
    $('#vehicleNo').val('');
    $('#vehicleNo').attr('data-unitId', null)
    $('#driver').val('');
    $('#driver').attr('data-unitId', null)
    $('#driver').attr('data-id', null)
    $('#driver').attr('data-value', null)
    $('#driver-shadow .form-search-select').empty()   
    $('#vehicleNo-shadow .form-search-select').empty()
    $('#pickupDestination').val('');
    $('#dropoffDestination').val('');
    $('#periodStartDate').val('');
    $('#periodEndDate').val('');
    driverObj = null
    mtUnitId = null
    editCreator = null
}

const changeTypeOfVehicle = async function (vehicle = null) {
    const OnCheckDriver = function (check) {
        let checked = $(check).prop('checked')
        $("#driver").prop("checked", checked)
        if (checked) {
            $(".noOfDriver").css('display', 'block')
        } else {
            $("#noOfDriver").val('');
            $(".noOfDriver").css('display', 'none')
        }
    }
    if (!vehicle) {
        vehicle = $('#typeOfVehicle').val()
    }
    if (vehicle) {
        await axios.post("/checkVehicleDriver", { vehicle }).then(res => {
            if (res.data.data == 1) {
                $('#driver-row').css('display', 'block')
            } else {
                $('#driver-row').css('display', 'none')
            }
            OnCheckDriver(false)
        })
    }
}

window.GetDestination = async function(){
    return axios.post('/mtAdmin/GetDestination')
    .then(function (res) {
        if (res.respCode === 1) {
            return res.respMessage;
        } else {
            console.error(res.respMessage);
            return null;
        }
    });
}

const resourceOnFocus = async function (e) {
    if($('#periodEndDate').val()){
        $('.search-select').css("display", "");
        $(".search-select input").css("display", "block");
        $(".search-select input").val("");
        $(e).next().css("display", "block")
        $(e).next().find(".form-search-select").empty()
        let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
        let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null;
        // vehicleTypeList = await getVehicleType($("#purposeType").val(), $('#periodStartDate').val(), $('#periodEndDate').val(), $('.hub-select option:selected').val(), $('.node-select option:selected').val());
        vehicleTypeList = await getVehicleType($("#purposeType").val(), startTime, endTime, $('.hub-select option:selected').val(), $('.node-select option:selected').val());
        if(vehicleTypeList.length > 0) vehicleTypeList = vehicleTypeList.sort()
        $('#typeOfVehicle-shadow .form-search-select').empty()
        for(let vehicleType of vehicleTypeList){
            $(e).next().find(".form-search-select").append(`<li>${ vehicleType }</li>`)
        } 
    }
}