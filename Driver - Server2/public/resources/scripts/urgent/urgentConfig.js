let data ;
let userType = Cookies.get('userType');
let userId = Cookies.get('userId');
let userNode = Cookies.get('node')
let dataTable
let driverObj = null;
let mtUnitId = null;
let driverList = null;
let vehicleList = null;
let editCreator = null;
let urgentDutyRequect = null;
$(async function () {
    initDateTime();
    initDate()
    initDetail();
    addUrgentDuty();
    initPage()
    clickSelect()
    initHubAndNode()
    $('.urgentDutyCancel').off('click').on('click', function () {
        clearPageData();
    });

    $(document).off('click').on("click", function (e) {
        let target = e.target;
        if (target.id != "search1" && target.id != "search2" 
         && target.id != "search3" && target.id != "search4" && target.id != "vehicleNo" && target.id != "driver" && target.id != "search5" && target.id != "typeOfVehicle") {
            $('.search-select').css("display", "");
            $('.unit-search-select').css("display", "");
        }
    });

    setInterval(function(){
        if($(window).width() < 992) {
            $('.div-hr-modal').removeAttr('style', "")
        } else {
            $('.div-hr-modal').attr('style', "padding-right: 4rem;border-right: 2px dashed #D7D7D7;")
        }
    }, 100)
    $('.select-group-div').hide()
    if(userType.toUpperCase() == 'HQ' || userType.toUpperCase() == 'CUSTOMER' || userType.toUpperCase() == 'ADMINISTRATOR') {
        $('.select-group-div').show()
        initGroup()
    }
})

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
        $(".select-hub").val("all")
        $('.select-hub').trigger('change')
        dataTable.ajax.reload(null, true)
    })
}

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
        $(".selected-request").val("")
        $(".created-date").val("")
        $(".select-hub").val("all")
        $('.select-hub').trigger('change')
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

window.updateUrgentDutyById = async function (id) {
    const getUrgentDutyById = async function (id) {
        return axios.post('/urgent/getUrgentDutyById',{ id })
        .then(function (res) {
            if (res.respCode != 1) {
                $.confirm({
                    title: 'Warn',
                    content: res.respMessage,
                    buttons: {
                        ok: {
                            btnClass: 'btn-green',
                            action: function () {
                                dataTable.ajax.reload(null, false)
                            }
                        }
                    }
                });
                return {}
            } else {
                return res.respMessage
            }
        });
    }
    data = await getUrgentDutyById(id);
    if(data){
        editCreator = data.creator
        $('#urgentDutyModal').modal("show");
        $('.modal-title').text('Edit Urgent Duty');
        $('#purposeType').val(data.purpose);
        if(data.hub){
            if(data.hub != '-') {
                $('.hub-select').val(data.hub);
                $('.hub-select').trigger('change')
                $('.node-select').val(data.node ? data.node : 'null');
                $('.node-select').trigger('change')
            }
        } 
        $('.hub-select').css('background-color', 'rgb(233, 236, 239)')
        $('.hub-select').prop('disabled', 'disabled')
        $('.node-select').css('background-color', 'rgb(233, 236, 239)')
        $('.node-select').prop('disabled', 'disabled')
        $('#periodStartDate').val(moment(data.indentStartDate).format('DD/MM/YYYY'));
        $('#periodEndDate').val(moment(data.indentEndDate).format('DD/MM/YYYY'));
        if(data.endDate) $('#periodEndDate').trigger('change')
        initVehicleDriverPage()
        // data.vehicleType == 'Ford Everest OUV' ? $("input[name='resource-type'][value='Ford Everest OUV']").prop('checked', true) : $("input[name='resource-type'][value='5 Ton GS (Auto)']").prop('checked', true)
        $('.resourceType').val(data.vehicleType)
        urgentDutyRequect = data.vehicleType
        verifyVehicleTypeAndVehicleAndDriver()
        if(data.vehicleNo) $('#vehicleNo').val(data.vehicleNo);
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
        if(id){;
            confirmUrgentDuty(id)
        }
    
        $('#urgentDutyCancel').off('click').on('click', function () {
            mtAdminId = null;
            clearPageData();
        });
        // $('.hub-select-row').hide()
        // $('.node-select-row').hide()
    }  
}

window.cancelUrgentDutyById = async function(id) {
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
                    return axios.post('/urgent/cancelUrgentDutyById',{ id, cancelledCause: cancelledCause })
                    .then(function (res) {
                        if(res.respCode != 1){
                            $.confirm({
                                title: 'Warn',
                                content: res.respMessage,
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
                            dataTable.ajax.reload(null, false)
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

window.showJustification = function (e) {
    let row = dataTable.row($(e).data('row')).data()
    $.alert({
        title: 'Justification',
        content: row.cancelledCause
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
            url: "/urgent/getUrgentConfig",
            type: "POST",
            data: function (d) {
                let hub = $(".select-hub option:selected").val()
                let node = $(".select-node option:selected").val()
                if(node) node = node.substring(node.lastIndexOf(":")+1, node.length)
                if(hub) hub = hub.toLowerCase() === 'all' ? null : hub;
                if(node) node = node.toLowerCase() === 'all' ? null : node;
                hub = hub ? hub : null;
                node = node ? node : null;
                let idDateOrder
                let endDateOrder
                for (let orderField of d.order) {
                    if(orderField.column == 0) {
                        idDateOrder = orderField.dir;
                    } else if(orderField.column == 5) {
                        endDateOrder = orderField.dir;
                    }
                }    
                let option = { 
                    'resource': $('.selected-request').val(),
                    'createDate': $('.created-date').val() ? $('.created-date').val() : null,
                    'hub': hub,
                    'node': node,
                    "groupId": $('.select-group').val() ? $('.select-group').val() : null,
                    "vehicleNo": $('.screen-vehicleNo').val() ? $('.screen-vehicleNo').val() : null,
                    "driverName": $('.screen-driverName').val() ? $('.screen-driverName').val() : null,
                    "endDateOrder": endDateOrder,
                    "idDateOrder": idDateOrder,
                    "pageNum": d.start, 
                    "pageLength": d.length
                }
                if(option.createDate) {
                    option.createDate = moment(option.createDate, 'DD/MM/YYYY').format('YYYY-MM-DD')
                }
                return option
            }
        },   
        columns: [
            { 
                data: 'id', 
                title: "ID",
                sortable: true 
            },
            { 
                title: 'Purpose', 
                data: 'purpose', 
                sortable: false,
                defaultContent: '-' 
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
                title: "Category",
                data: "category", 
                sortable: false ,
                defaultContent: '-' 
            },
            { 
                title: 'Execution Time', 
                sortable: true ,
                defaultContent: '-'  ,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label><label>${ moment('2023-10-26 '+full.startTime).format('HH:mm') } ${ full.indentStartDate ? moment(full.indentStartDate).format('DD/MM/YYYY') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label><label>${ moment('2023-10-26 '+full.endTime).format('HH:mm') } ${ full.indentEndDate ? moment(full.indentEndDate).format('DD/MM/YYYY') : '-' }</label>
                    `
                }
            },
            { 
                title: 'Vehicle No <br/> Resource', 
                data: 'vehicleNo', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNo ?? '-' }<br>${ full.vehicleType ?? '-' }`
                } 
            },
            { 
                title: 'Driver Name <br/> (Mobile No.)', 
                data: 'driverName', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    return `${ data ?? '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
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
                            <label class="fw-bold">Amended by:</label> <label>${ full.cancelledName ? full.cancelledName : '' }</label><br>
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
                data: 'id', 
                sortable: false,
                defaultContent: '-' ,
                render: function (data, type, full, meta) {
                        let html = ``
                        let operation = full.operation.toLowerCase().split(',')
                        if(!full.mobileStartTime && !full.cancelledDateTime) {
                            if (operation.includes('edit')) {
                                html += `<button type="button" class="px-2 py-0 btn-assigned btn custom-btn-blue" onclick="updateUrgentDutyById('${data}')">Edit</button> `
                            }
                            if (operation.includes('cancel')) {
                                html += `<button type="button" class="px-2 py-0 btn-assigned btn btn-danger" onclick="cancelUrgentDutyById('${data}')">Cancel</button> `
                            }
                        } else {
                            if((full.status).toLowerCase() == 'waitcheck') {
                                html += `Pending`
                            } else {
                                html += `${ full.status }`
                            }
                        }
                        return html
                }
            },
        ],
    });
}

const initHubAndNode = async function () {
    if(userType.toUpperCase() != 'CUSTOMER') {
        // $('.hub-select-row').show()
        // $(".node-select-row").show()
        const getHubNode = async function () {
            return axios.post('/mtAdmin/getHubNode')
                    .then(function (res) {
                        return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
    
        const initUnitTypePage = function (unitList) {
            $('.hub-select').empty();
            $(".node-select").empty();
           
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

const initVehicleDriverPage = async function () {
    const initVehicleDriver = async function (userId) {
        const vehicleOnFocus = async function (e) {
            if ($('#periodStartDate').val() && $('#periodEndDate').val()) {
                $('.search-select').css("display", "");
                $(".search-select input").css("display", "block");
                $(".search-select input").val("");
                $(e).next().css("display", "block")
                $(e).next().find(".form-search-select").empty()
                let user = await getUnitIdByUserId(userId);
                let unitId = $('.node-select option:selected').attr('data-unitId') ? $('.node-select option:selected').attr('data-unitId') : user.unitId;
                let startDate = `${ moment($('#periodStartDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') } 09:30`;
                let endDate = `${ moment($('#periodEndDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') } 17:00`;
                vehicleList = await getVehicleList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, urgentDutyRequect, startDate, endDate, $('.hub-select option:selected').val(), $('.node-select option:selected').val() ? $('.node-select option:selected').val() : null);
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
            if ($('#periodStartDate').val() && $('#periodEndDate').val()) {
                $('.search-select').css("display", "");
                $(".search-select input").css("display", "block");
                $(".search-select input").val("");
                $(e).next().css("display", "block")
                $(e).next().find(".form-search-select").empty();
                let user = await getUnitIdByUserId(userId);
                let unitId = $('.node-select option:selected').attr('data-unitId') ? $('.node-select option:selected').attr('data-unitId') : user.unitId
                let startDate = `${ moment($('#periodStartDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') } 09:30`;
                let endDate = `${ moment($('#periodEndDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') } 17:00`;
                driverList = await getDriverList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, urgentDutyRequect, startDate, endDate);
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

const initPage = async function () { 
    const initPurposeType = function (purposeTypeList) {
        $('#purposeType').empty()
        let html = `<option>${ purposeTypeList[0] }</option>` ;
        // for(let item of purposeTypeList){
        //     html+= `<option>${ item }</option>`;
        // }
        $('#purposeType').append(html)
    }
    initPurposeType(['Urgent Duty']);

    $("#vehicleNo-shadow .form-search-select").on("mousedown", "li", function () {
        let val = $(this).text()
        let id = $(this).attr("data-unitid")
        $(this).parent().parent().prev().val(val)
        $(this).parent().parent().prev().attr("data-unitid", id)
        $(this).parent().parent().css("display", "none")
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
    
    const initRequestType = function (requestList) {
        $('.selected-request').empty()
        let html =' <option value="">Resource:All</option>';
        for(let item of requestList){
            html += `<option>${ item }</option>`;
        }
        $('.selected-request').append(html)
    }
    initRequestType(['Ford Everest OUV', 'Agilis (Auto)', '5 Ton GS (Auto)' , '6 Ton GS']);
    $('.selected-request').off('change').on('change', function () {
        dataTable.ajax.reload(null, true)
    })

    const initRequest = function (requestList) {
        $('.resourceType').empty()
        let html =' <option value=""></option>';
        for(let item of requestList){
            html += `<option>${ item }</option>`;
        }
        $('.resourceType').append(html)
    }
    initRequest(['Ford Everest OUV', 'Agilis (Auto)', '5 Ton GS (Auto)' , '6 Ton GS']);

    $('.resourceType').off('click').on('click', function(){
        // urgentDutyRequect = $(this).filter(':checked').val()
        urgentDutyRequect = $(this).val()
        $('#vehicleNo').val('');
        $('#vehicleNo').attr('data-unitId', null)
        $('#driver').val('');
        $('#driver').attr('data-unitId', null)
        $('#driver').attr('data-id', null)
        $('#driver').attr('data-value', null)
        $('#driver-shadow .form-search-select').empty()   
        $('#vehicleNo-shadow .form-search-select').empty()
    })
    $('.resourceType').trigger('click')
    initVehicleDriverPage()
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

const verifyVehicleTypeAndVehicleAndDriver = async function () {
    if ($('#periodStartDate').val() && $('#periodEndDate').val()) {
        let user = await getUnitIdByUserId(userId);
        let unitId = $('.node-select option:selected').attr('data-unitId') ? $('.node-select option:selected').attr('data-unitId') : user.unitId
        let startDate = `${ moment($('#periodStartDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') } 09:30`;
        let endDate = `${ moment($('#periodEndDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') } 17:00`;
        let newVehicleList = await getVehicleList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, urgentDutyRequect, startDate, endDate, $('.hub-select').val(), $('.node-select').val());
        let result = newVehicleList.some(item=> item.vehicleNo == $('#vehicleNo').val())
        if(!result) {
            $('#vehicleNo').val('');
            $('#vehicleNo').attr('data-unitId', null)
            $('#vehicleNo-shadow .form-search-select').empty()   
        }
        let newDriverList = null;
        newDriverList = await getDriverList($('#purposeType').val(), mtUnitId ? mtUnitId : unitId, urgentDutyRequect, startDate, endDate);
        let driverResult = newDriverList.some(item=> item.driverId == $('#driver').attr('data-id'))
        if(!driverResult) {
            $('#driver').val('');
            $('#driver').attr('data-unitId', null)
            $('#driver').attr('data-id', null)
            $('#driver').attr('data-value', null)
            $('#driver-shadow .form-search-select').empty()   
        }
    } else {
        clearDriverAndVehicleAndType()
    }
}

const initDateTime = async function(){
    const getForbiddenDate = async function(){
        return axios.get('/urgent/getForbiddenDate')
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data ? res.data.respMessage : [];
        });
    }
    let dateList = await getForbiddenDate();
    //Exclude Saturdays, Sundays and holidays
    const disableDate = () => {
        const tdElements = document.querySelectorAll('.layui-laydate-content td');
		tdElements.forEach(td => {
            let targetDate = td.getAttribute('lay-ymd');
            for(let item of dateList){
                targetDate = moment(targetDate, 'YYYY-MM-DD').format('YYYY-MM-DD')
                if (targetDate == item) {  
                    td.classList.add('laydate-disabled');
                }
            }
        });

        let trElems = $(".layui-laydate-content").find('tr');
        trElems.each(function () {
            $(this).find('td').each(function (tdIndex, tdElem) {
                if (tdIndex === 0 || tdIndex === 6) {
                    $(this).addClass('laydate-disabled');
                }
            });
        });
    }

    layui.use(['laydate'], function () {
        let laydate = layui.laydate;
        let optStr = {
            elem: "#periodStartDate",
            lang: 'en',
            type: 'date',
            trigger: 'click',
            format: 'dd/MM/yyyy',
            btns: ['clear', 'confirm'],
            holidays: [parent.publidHolidays],
            ready: (value) => { 
                disableDate()
                // $('#periodStartDate').text(`09:30-17:00 ${ value }`)
                $('#periodStartDate').text(`${ value }`)
                noSecond()
                DisabledLayDate();
            },
            change: (value) => { 
                disableDate()
                noSecond()
                DisabledLayDate();
            },
            done: (value) => {
                if (value) {
                    value = moment(value, 'DD/MM/YYYY').format('YYYY-MM-DD')
                    let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
                    let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
                    if (moment(value).isSameOrAfter(moment(endTime)) && startTime != endTime) {
                        $.alert({
                            title: 'Warn',
                            content: 'End date should be later than start date!',
                        });
                        $('#periodEndDate').val(null)
                    }
                    verifyVehicleTypeAndVehicleAndDriver()
                } else {
                    clearDriverAndVehicleAndType()
                }
            }
        };
        optStr['min'] = moment().format('YYYY-MM-DD')
        laydate.render(optStr);
    });

    layui.use(['laydate'], function () {
        let laydate = layui.laydate;
        let optStr = {
            elem: '#periodEndDate',
            lang: 'en',
            type: 'date',
            trigger: 'click',
            format: 'dd/MM/yyyy',
            btns: ['clear', 'confirm'],
            holidays: [parent.publidHolidays],
            ready: () => { 
                disableDate()
                noSecond()
                DisabledLayDate();
            },
            change: () => { 
                disableDate()
                noSecond()
                DisabledLayDate();
            },
            done: (value) => {
                if ($('#periodStartDate').val() && value) {
                    let startTime = moment($('#periodStartDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD')
                    let endTime = moment($('#periodEndDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD')
                    value = moment(value, 'DD/MM/YYYY').format('YYYY-MM-DD')
                    if (moment(startTime).isSameOrAfter(moment(value)) && startTime != endTime) {
                        $.alert({
                            title: 'Warn',
                            content: 'End date should be later than start date!',
                        });
                        $('#periodEndDate').val(null)
                    } 
                    verifyVehicleTypeAndVehicleAndDriver()
                }  else {
                    clearDriverAndVehicleAndType()
                }
                
            }
        };
        optStr['min'] = moment().format('YYYY-MM-DD')
        laydate.render(optStr);
    });
}

const initDate = function () {
    layui.use('laydate', function(){
        let laydate = layui.laydate;
        laydate.render({
            elem: '.execution-date',
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
        if(unitId && unitId != '' && startDate && endDate && purpose && vehicleType){
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
        if(startDate && endDate && vehicleType){
            return axios.post('/urgent/getDriverByGroupId', { editUserId, vehicleType, startDate, endDate })
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
            return axios.post('/urgent/getVehicleByGroupId', { editUserId, vehicleType, startDate, endDate })
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

const addUrgentDuty = async function () {
    $('#addUrgentDuty').off('click').on('click', async function () {
        $('.hub-select').removeAttr('disabled')
        $('.hub-select').css('background-color', 'white')
        $('.node-select').css('background-color', 'white')
        $('.node-select').removeAttr('disabled')
        $('.modal-title').text('New Urgent Duty');
        clearPageData();
        initHubAndNode()
        confirmUrgentDuty(null);
    });

    $('#urgentDutyCancel').off('click').on('click', function () {
        $('.hub-select').removeAttr('disabled')
        $('.hub-select').css('background-color', 'white')
        $('.node-select').css('background-color', 'white')
        $('.node-select').removeAttr('disabled')
        clearPageData();
    });
}

const confirmUrgentDuty = function (id) {
    const ValidAssignTask = function (data, id) {
        let errorLabel = {
            purpose: 'purpose',
            hub: 'Hub',
            node: 'Node',
            category: 'category',
            indentStartDate: 'Start Date',
            indentEndDate: 'End Date',
            startTime: 'startTime',
            endTime: 'endTime',
            vehicleType: 'Resource',
            vehicleNo: 'Vehicle No',
            driverId: 'Driver'
        }
        for (let key in data) {
            if(id || userType.toUpperCase() == 'CUSTOMER') {
                if(key == 'hub') {
                    continue
                }
            }
            if(key == 'node' || key == 'category' || key == 'startTime' || key == 'endTime') {
                continue
            }
            if(!data[key]) {
                $.alert({
                    title: 'Warn',
                    content: `${ errorLabel[key] } is required.`,
                });
                return false;
            }
        }
        return true
    }

    const createUrgentConfig = async function (urgentConfig) {
        return axios.post('/urgent/createUrgentConfig',{ urgentConfig })
        .then(function (res) {
           return res
        });
    }
    const updateUrgentDutyById = async function (urgentConfig, id) {
        return axios.post('/urgent/updateUrgentDutyById',{ urgentConfig, id })
        .then(function (res) {
           return res
        });
    }
    $('#urgentDutyConfirm').off('click').on('click', async function () {
        let urgentConfig = {
            purpose: $('#purposeType').val(),
            hub: $('.hub-select').val(),
            node: $('.node-select').val(),
            category: 'MV',
            indentStartDate: $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null,
            indentEndDate: $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null,
            startTime: '09:30',
            endTime: '17:00',
            vehicleType: urgentDutyRequect,
            vehicleNo: $('#vehicleNo').val(),
            driverId: $('#driver').attr('data-id'),
        }
        let state = ValidAssignTask(urgentConfig, id)
        if(!state) return
        $(this).addClass('btn-disabled')
        if(id) {
            await updateUrgentDutyById(urgentConfig, id).then((res) => {
                urgentConfig = null
                $(this).removeClass('btn-disabled')
                if(res.respCode != 1) {
                    $.alert({
                        title: 'Warn',
                        content: res.respMessage,
                    });
                    return
                }
                clearPageData();
                $('#urgentDutyModal').modal('hide')
                dataTable.ajax.reload(null, false) 
                return
            });
        } else {
            await createUrgentConfig(urgentConfig).then((res) => {
                urgentConfig = null
                $(this).removeClass('btn-disabled')
                if(res.respCode != 1) {
                    $.alert({
                        title: 'Warn',
                        content: res.respMessage,
                    });
                    return
                }
                clearPageData();
                $('#urgentDutyModal').modal('hide')
                dataTable.ajax.reload(null, false) 
                return
            });
        }
        $(this).removeClass('btn-disabled')
    })
}

const clearPageData = function () {
    data = ''
    $('.resourceType').val('')
    $('.resourceType').trigger('change')
    // $('#purposeType').val('');
    $('.hub-select').val('');
    $('.hub-select').trigger('change')
    $('.node-select').val('');
    $('#vehicleNo').val('');
    $('#vehicleNo').attr('data-unitId', null)
    $('#driver').val('');
    $('#driver').attr('data-unitId', null)
    $('#driver').attr('data-id', null)
    $('#driver').attr('data-value', null)
    $('#driver-shadow .form-search-select').empty()   
    $('#vehicleNo-shadow .form-search-select').empty()
    $('#periodStartDate').val('');
    $('#periodEndDate').val('');
    driverObj = null
    mtUnitId = null
    editCreator = null
}