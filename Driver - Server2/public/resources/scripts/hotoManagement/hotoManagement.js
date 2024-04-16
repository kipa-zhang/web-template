let dataTable = null;
let tableType = 'current';
$(function () {
    $('.addRequest').off('click').on('click', function() {
        $('.modal-title').text('Create Request')
        $('#requestModal').modal('show');
        initPage() 
    })
    initSelect()
    initTable()
    // setInterval(() => {
    //     if(dataTable) dataTable.ajax.reload(null, false)
    // }, 2000)
    $('.span-hotoTable').off('click').on('click', function () {
        $('.span-hotoTable').removeClass('active');
        $(this).addClass('active');
        if($(this).text().toLowerCase() == 'current') {
            $('.history-div').hide()
            $('.current-div').show()
            tableType = 'current'
        }
        if($(this).text().toLowerCase() == 'history'){
            $('.current-div').hide()
            $('.history-div').show()
            tableType = 'history'
        }
        $('.selected-status').val("")
        initSelectStatus(tableType)
        $('#clearAll').trigger('click')
        if(dataTable) dataTable.ajax.reload(null, true)
    })
    initSelectStatus(tableType)
    $('.selected-status').val("")
    if(dataTable) dataTable.ajax.reload(null, true)
});

const getVehicleTypeList = async function () {
    return await axios.post('/hoto/getVehicleTypeList', { creator: null })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        });
}

const initSelectStatus = function(tableType){
    const initStatus = function(list){
        $('.selected-status').empty();
        $('.selected-status').append(`<option value="">Status: All</option>`)
        for(let item of list){
            $('.selected-status').append(`<option>${ item }</option>`)
        }
    }
    let statusList = null;
    if(tableType == 'current'){
        statusList = [ 'Pending Endorse', 'Pending Assign', 'Pending Approval', 'Approved' ]
    }
    if(tableType == 'history'){
        statusList = [ 'Completed', 'Rejected', 'Cancelled' ]
    }
    initStatus(statusList)
    $('.selected-status').off('change').on('change', function(){
        if(dataTable) dataTable.ajax.reload(null, true)
    })
}

const initSelect = async function(){
    const initDate = function () {
        // layui.use('laydate', function(){
        //     let laydate = layui.laydate;
        //     laydate.render({
        //         elem: '.execution-date',
        //         type: 'date',
        //         lang: 'en',
        //         trigger: 'click',
        //         range: '~',
        //         btns: ['clear', 'confirm'],
        //         done: function () {
        //             dataTable.ajax.reload(null, true)
        //         }
        //     });
        // });
    
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
    initDate()

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
        purposeTypeList.push({id: 111, purposeName: 'Maintenance'})
        $('.selected-Purpose').empty()
        let html2 =' <option value="">Purpose:All</option>';
        for(let purposeType of purposeTypeList){
            html2+= `<option data-id="${ purposeType.id }">${ purposeType.purposeName }</option>`;
        }
        $('.selected-Purpose').append(html2)
    }
    initPurposeType(await getPurposeModeType());

    const initUnit = function (unitList) {
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
            $(".select-node").off('change').on('change' , function () {
                dataTable.ajax.reload(null, true)
            })
        }
    }
    const getHubNode = async function() {
        return await axios.post('/mtAdmin/getHubNode')
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        });
    }
    let unitList = await getHubNode();
    initUnit(unitList); 

    const initType = function(typeList){
        $('.selected-type').empty();
        $('.selected-type').append(`<option value="">Type: All</option>`)
        for(let item of typeList){
            $('.selected-type').append(`<option>${ item }</option>`)
        }
    }
    initType(await getVehicleTypeList())

    const initResource = function(list){
        $('.selected-resource').empty();
        $('.selected-resource').append(`<option value="">Resource: All</option>`)
        for(let item of list){
            $('.selected-resource').append(`<option>${ item }</option>`)
        }
    }
    initResource(['Driver', 'Vehicle'])

    $('.selected-Purpose').off('change').on('change', function(){
        if(dataTable) dataTable.ajax.reload(null, true)
    })

    $(".selected-type").off('change').on('change', function(){
        if(dataTable) dataTable.ajax.reload(null, true)
    })

    $(".selected-resource").off('change').on('change', function(){
        if(dataTable) dataTable.ajax.reload(null, true)
    })

    $('#clearAll').off('click').on('click', function () {
        $('.selected-Purpose').val("")
        $(".selected-type").val("")
        $(".selected-resource").val("")
        $('.selected-status').val("")
        // $('.execution-date').val("")
        $(".created-date").val("")
        $(".select-hub").val("all")
        $('.select-hub').trigger('change')
        if(dataTable) dataTable.ajax.reload(null, true)
    });
}

const initPage = async function () {
    const initTypeList = function (typeList) {
        $('#platformType').empty()
        let html =' <option></option>';
        for(let item of typeList){
            html+= `<option>${ item }</option>`;
        }
        $('#platformType').append(html)
    }
    initTypeList(await getVehicleTypeList());

    const initDateTime = function(){
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
                        if (moment(value).isSameOrAfter(moment(endTime))) {
                            $.alert({
                                title: 'Warn',
                                content: 'End time should be later than start time.',
                            });
                            $('#periodEndDate').val(null)
                        }
                    }
                }
            };
            optStr['min'] = moment().format('YYYY-MM-DD HH:mm:ss')
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
                    if ($('#periodStartDate').val() && value) {
                        value = moment(value, 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm')
                        let startTime = moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm')
                        if (moment(startTime).isSameOrAfter(moment(value))) {
                            $.alert({
                                title: 'Warn',
                                content: 'End time should be later than start time.',
                            });
                            $('#periodEndDate').val(null)
                        }
                    }  
                }
            };
            optStr['min'] = moment().format('YYYY-MM-DD HH:mm:ss')
            laydate.render(optStr);
        });
    }
    initDateTime()
    
    const initResource = function() {
        $('.resourceType').empty()
        let resourceTypeList = ['Driver', 'Vehicle']
        for(let item of resourceTypeList) {
            $('.resourceType').append(`<option>${ item }</option>`)
        }
    }
    initResource()

    const initHubNode = async function (){
        const getHubNodeByUser = async function () {
            return axios.post('/mtAdmin/getHubNode')
            .then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        let unitData = await getHubNodeByUser()
        const initUnit = async function (unitList) {
            if(unitList.length > 0) {
                let __unitList = unitList.map(unit => { return unit.unit });
                $('.select-myHub').empty();
                __unitList = Array.from(new Set(__unitList));
                if(__unitList.length > 1){
                    let html = `<option value="">Hub:All</option>`;
                    for (let __unit of __unitList) {
                        html += `<option name="unitType" value="${ __unit }">${ __unit }</option>`
                    }
                    $('.select-myHub').append(html); 
                } else {
                    $('.select-myHub').append(`<option name="unitType">${ __unitList[0] }</option>`); 
                    setTimeout(function(){
                        $('.select-myHub').val(__unitList[0]).trigger('change')
                    }, 200)
                   
                }
               
    
                $('.select-myHub').off('change').on('change' , function () {
                    let selectedUnit = $(this).val();
                    $(".select-myNode").empty();
                    let html2
                    if(unitList.length > 1) {
                        html2 = `<option value="">Node:All</option>`;
                        for (let unit of unitList) {
                            if (unit.unit === selectedUnit && unit.subUnit) {
                                if((unit.subUnit).toLowerCase() === 'null') continue
                                html2 += `<option name="subUnitType">${ unit.subUnit }</option>`
                            } else {
                                continue;
                            }
                        }
                    } else {
                        html2 = `<option name="subUnitType" value='${ unitList[0].subUnit }'>${ unitList[0].subUnit }</option>`;
                    }
                    $(".select-myNode").append(html2);
                })
            } else {
                $('.select-myHub').empty();
                $('.select-myHub').append(`<option value="">Hub:All</option>`);
                $(".select-myNode").empty();
                $(".select-myNode").append(`<option value="">Node:All</option>`);
            }
        }
        initUnit(unitData)
    }
    initHubNode()

    const initPurpose = async function (){
        const getPurposeModelist = async function () {
            return await axios.post('/mtAdmin/getPurposeModelist', { creator: null })
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
                html+= `<option>${ purposeType.purposeName }</option>`;
            }
            $('#purposeType').append(html)
        }
        initPurposeType(await getPurposeModelist());
    }
    await initPurpose()

    const clearPageData = function () {
        $('#activityName').val('')
        $('#purposeType').val('')
        $('.select-myHub').val('')
        $('.select-myNode').val('')
        $('.select-myHub').attr('data-id', null)
        initHubNode();
        $('#periodStartDate').val('')
        $('#periodEndDate').val('')
        $('.resourceType').val('')
        $('.resource-qty-input').val('')
        $('#explanation').val('')
        dataTable.ajax.reload(null, false) 
    }

    $('.requestCancel').off('click').on('click', function(){
        clearPageData()
    })

    $('#requestConfirm').off('click').on('click', async function() {
        const validRequest = function (data) {
            let errorLabel = {
                activityName: 'Activity Name',
                purpose: 'Purpose',
                hub: 'Hub',
                node: 'Node',
                startTime: 'Start Time',
                endTime: 'End Time',
                resource: 'Resource',
                resourceQty: 'Resource Qty',
                explanation: 'Explanation',
                vehicleType: 'Type'
            }
            for (let key in data) {
                // if(key == 'hub' || key == 'node') continue
                if(key == 'node') continue
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
        let myHub = $('.select-myHub').val() && ($('.select-myHub').val() == 'all' || $('.select-myHub').val() == '') ? null : $('.select-myHub').val();
        let myNode = $('.select-myNode').val() && ($('.select-myNode').val() == 'all' || $('.select-myNode').val() == '') ? null : $('.select-myNode').val();
        let hotoRequest = {
            activityName: $('#activityName').val(),
            purpose: $('#purposeType').val(),
            hub: myHub,
            node: myNode,
            startTime: $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null,
            endTime: $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null,
            resource: $('.resourceType').val(),
            resourceQty: $('.resource-qty-input').val(),
            explanation: $('#explanation').val(),
            vehicleType: $('#platformType').val()
        }
        let state = validRequest(hotoRequest)
        if(!state) return
        let requestId = $('.select-myHub').attr('data-id') ? $('.select-myHub').attr('data-id') : null
        const createHotoRequest = async function (hotoRequest) {
            $('#requestConfirm').addClass('btn-hotoConfirm')
            return axios.post('/hoto/createHotoRequest',{ hotoRequest })
            .then(function (res) {
                if (res.respCode === 1) {
                    clearPageData()
                    $('#requestModal').modal('hide');
                    $('#requestConfirm').removeClass('btn-hotoConfirm')
                } else{
                    clearPageData()
                    $('#requestModal').modal('hide');
                    $.alert({
                        title: 'Warn',
                        content: `operation failure.`,
                    });
                    $('#requestConfirm').removeClass('btn-hotoConfirm')
                }
            });
        }
        const editHotoRequest = async function (hotoRequest, requestId) {
            $('#requestConfirm').addClass('btn-hotoConfirm')
            return axios.post('/hoto/editHotoRequest',{ hotoRequest, requestId })
            .then(function (res) {
                if (res.respCode === 1) {
                    clearPageData()
                    $('#requestModal').modal('hide');
                    $('#requestConfirm').removeClass('btn-hotoConfirm')
                } else{
                    clearPageData()
                    $('#requestModal').modal('hide');
                    $.alert({
                        title: 'Warn',
                        content: `${ res.respMessage ? res.respMessage : 'operation failure.' }`,
                    });
                    $('#requestConfirm').removeClass('btn-hotoConfirm')
                }
            });
            
        }
        if(requestId) {
            await editHotoRequest(hotoRequest, requestId)
        } else {
            await createHotoRequest(hotoRequest)
        }
        if(dataTable) dataTable.ajax.reload(null, false)
    })
}

window.operateRequest = async function(operateType, requestId){
    let option = null
    if(operateType.toLowerCase() == 'endorsed'){
        option = 'endorseRequestById'
    } else if(operateType.toLowerCase() == 'rejected'){
        option = 'rejectRequestById'
    } else if(operateType.toLowerCase() == 'cancelled'){
        option = 'cancelRequestById'
    }
    const operateRequestById = async function (operateType, requestId) {
        if(!option) return
        return await axios.post(`/hoto/${ option }`, { operateType, requestId })
            .then(function (res) {
                if (res.respCode === 1) {
                    dataTable.ajax.reload(null, false) 
                    return res.respMessage;
                } else {
                    dataTable.ajax.reload(null, false) 
                    $.alert({
                        title: 'Warn',
                        content: (res.respMessage).join('<br/>'),
                    });
                    return null;
                }
            });
    }
    await operateRequestById(operateType, requestId)
}

window.editRequest = async function(requestId){
    const getHotoRequestById = async function (requestId) {
        return await axios.post('/hoto/getHotoRequestById', { requestId })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage;
                } else {
                    console.error(res.respMessage);
                    return null;
                }
            });
    }
    let hotoRequest = await getHotoRequestById(requestId)
    $('.modal-title').text('Edit Request')
    $('#requestModal').modal('show');
    initPage() 
    setTimeout(function(){
        if(hotoRequest){
            $('.modal-title').text('Edit Request')
            $('.select-myHub').attr('data-id', hotoRequest.id)
            $('#activityName').val(hotoRequest.activityName)
            $('#platformType').val(hotoRequest.vehicleType)
            $('#purposeType').val(hotoRequest.purpose)
            $('.select-myHub').val(hotoRequest.hub)
            $('.select-myHub').trigger('change')
            $('.select-myNode').val(hotoRequest.node)
            $('#periodStartDate').val(hotoRequest.startTime ? moment(hotoRequest.startTime).format('DD/MM/YYYY HH:mm') : '')
            $('#periodEndDate').val(hotoRequest.endTime ? moment(hotoRequest.endTime).format('DD/MM/YYYY HH:mm') : '')
            $('.resourceType').val(hotoRequest.resource)
            $('.resource-qty-input').val(hotoRequest.resourceQty)
            $('#explanation').val(hotoRequest.explanation)
        }
    }, 250)
   
}

window.showExplanation = function (e) {
    let row = dataTable.row($(e).data('row')).data()
    $.alert({
        title: 'Explanation',
        content: row.explanation
    });
}

window.showActivityName = function (e) {
    let row = dataTable.row($(e).data('row')).data()
    $.alert({
        title: 'Activity Name',
        content: row.activityName
    });
}

window.buttonRequest = async function(requestType, requestId){
    $.removeCookie('requestType')
    $.removeCookie('requestId')
    Cookies.set('requestId', requestId, {path: '/'})
    Cookies.set('requestType', requestType, {path: '/'})
    window.location.href = "/hoto/viewManagement";
}

const initTable = async function() {
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
            url: "/hoto/getHotoRequest",
            type: "POST",
            data: function (d) {
                let idOrder
                for (let orderField of d.order) {
                    if(orderField.column == 0) {
                        idOrder = orderField.dir;
                    }
                }    
                let option = { 
                    "idOrder": idOrder,
                    "purpose": $('.selected-Purpose').val(),
                    "type": $(".selected-type").val() && $(".selected-type").val() == '' ? null : $(".selected-type").val(),
                    "resource":  $(".selected-resource").val() && $(".selected-resource").val() == '' ? null : $(".selected-resource").val(),
                    // "execution_date": $(".execution-date").val(),
                    "createDate": $(".created-date").val(),
                    "selectHub": $(".select-hub").val() && $(".select-hub").val() == 'all' ? null :  $(".select-hub").val(),
                    "selectNode": $(".select-node").val() && $(".select-node").val() == 'all' ? null :  $(".select-node").val(),
                    "status":  $(".selected-status").val() && $(".selected-status").val() == '' ? null : $(".selected-status").val(),
                    "tableType": tableType,
                    "pageNum": d.start, 
                    "pageLength": d.length
                }
                if(option.createDate) {
                    option.createDate = moment(option.createDate, 'DD/MM/YYYY').format('YYYY-MM-DD')
                }
                return option
            }
        },   
        "initComplete" : function (settings, json) {
            // $(".data-list thead tr th:first").removeClass('sorting_asc');
        },
        "columns": [
            // { 
            //     data: null, 
            //     title: "S/N",
            //     sortable: false ,
            //     "render": function (data, type, full, meta) {
            //         return meta.row + 1 + meta.settings._iDisplayStart
            //     }
            // },
            { 
                data: 'id', 
                title: "Request ID",
                sortable: true 
            },
            { 
                title: 'Purpose', 
                data: 'purpose', 
                sortable: false,
                defaultContent: '-' 
            },
            { 
                data: 'activityName', 
                title: "Activity",
                sortable: false,
                render: function (data, type, full, meta){
                    if (data) {
                        return `
                        <div>
                            <span class="d-inline-block text-truncate" style="max-width: 90px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showActivityName(this);" role="button" tabindex="0">
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
                title: 'Request For', 
                data: 'hub', 
                sortable: false,
                defaultContent: '-' ,
                render: function (data, type, full, meta){
                    if (data) {
                        return `
                        ${ data } <br/>
                        ${ full.node ? full.node : '' }
                        `
                    } else {
                        return '-'
                    }
                } 
            },
            { 
                title: 'Status', 
                data: 'status', 
                sortable: false,
                defaultContent: '-' ,
                render: function (data, type, full, meta) {
                    if(full.newStatus) data = full.newStatus
                    if(data){
                        let status = null;
                        if(data.toLowerCase() == 'submitted') {
                            status = ` Pending Endorse `
                        } else if(data.toLowerCase() == 'endorsed') {
                            status = ` Pending Assign `
                        } else if(data.toLowerCase() == 'assigned') {
                            // if(full.assignQtyStatus) {
                            //     status = ` Pending Assign `
                            // } else {
                                status = ` Pending Approval `
                            // }
                        } else {
                            status = data
                        }
                        return `<div style="font-weight: bolder;font-size: 1rem;">
                                    ${ status }
                                </div>`
                    }
                }
            },
            { 
                title: 'Execution Time', 
                sortable: false ,
                defaultContent: '-'  ,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label> <label>${ full.startTime ? moment(full.startTime).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.endTime ? moment(full.endTime).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                }
            },
            { 
                title: 'Resource<br/>(Type)', 
                data: 'resource', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    return `
                        <label>${ data ? data : '-' }</label><br>
                        <label>${ full.vehicleType ? full.vehicleType : '-' }</label>
                    `
                }
            },
            { 
                title: 'Qty', 
                data: 'resourceQty', 
                sortable: false ,
                defaultContent: '-'
            },
            { 
                title: 'Explanation', 
                data: 'explanation', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta){
                    if (data) {
                        return `
                        <div>
                            <span class="d-inline-block text-truncate" style="max-width: 90px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showExplanation(this);" role="button" tabindex="0">
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
                title: 'Submit', 
                data: 'createdAt', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    return `<label class="fw-bold">SubmittedBy:</label> ${ full.fullName }<br/>
                    <label class="fw-bold">SubmittedAt:</label> ${ data ? moment(data).format('DD/MM/YYYY HH:mm') : '-' }`
                } 
            },
            { 
                title: 'Action', 
                data: 'id', 
                sortable: false,
                defaultContent: '-' ,
                render: function (data, type, full, meta) {
                    const buttonListObj = {
                        VIEW: ` <button type="button" class="px-2 py-0 btn-view btn-action" onclick="buttonRequest('view', ${ data })">View</button> `,
                        APPROVE: ` <button type="button" class="px-2 py-0 btn-endorse btn-action" onclick="buttonRequest('approve', ${ data })">Approve</button> `,
                        ASSIGN: ` <button type="button" class="px-2 py-0 btn-endorse btn-action" onclick="buttonRequest('assign', ${ data })">Assign</button> `,
                        REPLACE: ` <button type="button" class="px-2 py-0 btn-replace btn-action" onclick="buttonRequest('Replace', ${ data })">Replace</button> `,
                        EDIT: ` <button type="button" class="px-2 py-0 edit-btn btn-action" onclick="editRequest(${ data })">Edit</button> `,
                        ENDORSE: ` <button type="button" class="px-2 py-0 btn-endorse btn-action" onclick="operateRequest('Endorsed', ${ data })">Endorse</button> `,
                        REJECT: ` <button type="button" class="px-2 py-0 btn-reject btn-action" onclick="operateRequest('Rejected', ${ data })">Reject</button> `,
                        CANCEL: ` <button type="button" class="px-2 py-0 btn-cancel btn-action" onclick="operateRequest('Cancelled', ${ data })">Cancel</button> `,
                        RETURN: ` <button type="button" class="px-2 py-0 btn-return btn-action" onclick="buttonRequest('return', ${ data })">Return</button> `,
                    }

                    let operationList = (full.operation).toUpperCase().split(',')

                    if(full.status) {
                        if((full.status).toLowerCase() == 'submitted') {
                            let html = `
                                ${ operationList.includes('EDIT') ? buttonListObj.EDIT : '' }
                                ${ operationList.includes('ENDORSE') ? buttonListObj.ENDORSE : '' }
                                ${ operationList.includes('REJECT') && full.cancelBanStatus ? buttonListObj.REJECT : '' }
                                ${ operationList.includes('CANCEL') ? buttonListObj.CANCEL : '' }
                            `
                            return html.trim() ? html : full.status
                        }
                        if((full.status).toLowerCase() == 'endorsed') {
                            let html = `
                                ${ operationList.includes('VIEW') ? buttonListObj.VIEW : '' }
                                ${ operationList.includes('ASSIGN') ? buttonListObj.ASSIGN : '' }
                                ${ operationList.includes('CANCEL') ? buttonListObj.CANCEL : '' }
                            `
                            return html.trim() ? html : full.status
                        }
                        if((full.status).toLowerCase() == 'assigned') {
                            if(full.assignQtyStatus){
                                let html = `
                                    ${ operationList.includes('VIEW') ? buttonListObj.VIEW : '' }
                                    ${ operationList.includes('ASSIGN') ? buttonListObj.ASSIGN : '' }
                                    ${ operationList.includes('REPLACE') && full.replaceStatus ? buttonListObj.REPLACE : '' }
                                    ${ operationList.includes('CANCEL') ? buttonListObj.CANCEL : '' }
                                `
                                return html.trim() ? html : full.status
                            } else {
                                let html = `
                                    ${ operationList.includes('VIEW') ? buttonListObj.VIEW : '' }
                                    ${ operationList.includes('REPLACE') && full.replaceStatus ? buttonListObj.REPLACE : '' }
                                    ${ operationList.includes('APPROVE') ? buttonListObj.APPROVE : '' }
                                    ${ operationList.includes('REJECT') && full.cancelBanStatus ? buttonListObj.REJECT : '' }
                                `
                                return html.trim() ? html : full.status
                            }
                        }
                        if(full.taskStatus) {
                            if((full.status).toLowerCase() == 'approved') {
                                let html = `
                                    ${ operationList.includes('VIEW') ? buttonListObj.VIEW : '' }
                                    ${ operationList.includes('REPLACE') && full.replaceStatus && !full.newStatus ? buttonListObj.REPLACE : '' }
                                    ${ full.approverExist ? operationList.includes('APPROVE') ? buttonListObj.APPROVE : '' : '' }
                                    ${ full.hotoExist == 1 ? operationList.includes('RETURN') ? buttonListObj.RETURN : '' : '' }
                                `
                                return html.trim() ? html : full.status
                            }
                        } else {
                            if((full.status).toLowerCase() == 'approved') {
                                let html = `
                                    ${ operationList.includes('VIEW') ? buttonListObj.VIEW : '' }
                                    ${ operationList.includes('REPLACE') && full.replaceStatus && !full.newStatus ? buttonListObj.REPLACE : '' }
                                    ${ full.approverExist ? operationList.includes('APPROVE') ? buttonListObj.APPROVE : '' : '' }
                                    ${ full.hotoExist == 1 ? operationList.includes('RETURN') ? buttonListObj.RETURN : '' : '' }
                                    ${ full.cancelBanStatus ? operationList.includes('CANCEL') ? buttonListObj.CANCEL : '' : '' }
                                `
                                return html.trim() ? html : full.status
                            }
                        }
                        if((full.status).toLowerCase() == 'rejected') {
                            let html = `
                                ${ operationList.includes('VIEW') ? buttonListObj.VIEW : '' }
                                ${ full.cancelBanStatus ? operationList.includes('CANCEL') ? buttonListObj.CANCEL : '' : '' }
                            `
                            return html.trim() ? html : full.status
                        }
                        if((full.status).toLowerCase() == 'cancelled') {
                            let html = `
                            ${ operationList.includes('VIEW') ? buttonListObj.VIEW : '' }
                            `
                            return html.trim() ? html : full.status
                        }
                    }
                }
            },
        ],
    });
}