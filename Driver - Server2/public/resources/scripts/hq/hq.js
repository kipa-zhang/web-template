let currentInterval = null;
let Interval_CheckRouteCreate = null;
let currentArea = null;
let currentNode = null;
let layer = null;
let userType = Cookies.get('userType');
let userId = Cookies.get('userId');
let tableHub;
let tableNode;
let tableBySys     
let tableByMb
let tableName = 'sys'

layui.use('layer', function(){
    layer = layui.layer;
});

$( async function () {
    initClickPage()
    taskUploadEventHandler()
    initSelectedAndPage();
    clickSelect();
    initDetail();
    // initDetail2();

    setTimeout(() => {
        $('.assign-menu').children().first().trigger('click')
    }, 100)

});

const tableReload = function () {
    if(tableName == 'mb'){
        tableByMb.ajax.reload(null, true) 
    } else if(tableName == 'sys' || tableName == 'atms') {
        tableBySys.ajax.reload(null, true) 
    }
}

const tableReload2 = function () {
    if(tableName == 'mb'){
        tableByMb.ajax.reload(null, false) 
    } else if(tableName == 'sys' || tableName == 'atms') {
        tableBySys.ajax.reload(null, false) 
    }
}

const initClickPage = function () {
    $('.sys-assign').off('click').on('click', function () {
        tableName = 'sys'
        $('.mb-assign-border').css('display', 'none')
        $('.sys-assign-border').css('display', 'block')
        // $('.mb-task-assign').css('display', 'none')
        // $('.tripId-div').css('display', 'block')
        // $('.sys-assign-table').css('display', 'block')
        // $('.mb-assign-table').css('display', 'none')
        // $('.taskStatus-div').show()
        if(tableBySys) tableBySys.ajax.reload(null, false) 
    })

    $('.mb-assign').off('click').on('click', function () {
        // tableName = 'mb'
        tableName = 'atms'
        $('.mb-assign-border').css('display', 'block')
        $('.sys-assign-border').css('display', 'none')
        // $('.mb-task-assign').css('display', 'block')
        // $('.tripId-div').css('display', 'none')
        // $('.sys-assign-table').css('display', 'none')
        // $('.mb-assign-table').css('display', 'block')
        // $('.taskStatus-div').hide()
        // if(tableByMb) tableByMb.ajax.reload(null, false) 
        if(tableBySys) tableBySys.ajax.reload(null, false) 
    })
    $('.sys-assign').trigger('click')

    $('#clearAll').off('click').on('click', function () {
        $(".selected-vehicleType").val("")
        $(".select-hub").val("")
        $('.select-hub').trigger('change')
        $(".execution-date").val("")
        $(".created-date").val("")
        $(".tripId").val("")
        $('.screen-vehicleNo').val('')
        $('.screen-driverName').val('')
        if(tableName == 'sys' || tableName == 'atms') $('.selected-taskStatus').val('')
        setTimeout(function () {
            tableReload()
        }, 300)
    });
}

let taskCreateIns = null;
let taskUploadIns = null;
const taskUploadEventHandler = function () {
    layui.use('upload', function(){
        let upload = layui.upload;
        if (taskCreateIns) return;
        taskCreateIns = upload.render({
            elem: '#task-Create',
            url: '/uploadMBTask',
            accept: 'file',
            acceptMime: 'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            before: function () {
                $('#loadingModal').modal('show');
            },
            // multiple: true,
            done: function (res) {
                if (res.respCode == 1) {
                    $.alert({
                        title: 'Warn',
                        content: 'Upload success.',
                    });
                } else {
                    $.alert({
                        title: 'Error!',
                        content: res.respMessage,
                    });
                }
                // initDetail2()
                setTimeout(() => {
                    $('#loadingModal').modal('hide');
                }, 500)
            },
            error: function (error) {
                console.error(error);
            }
        });
    });
    layui.use('upload', function(){
        let upload = layui.upload;
        if (taskUploadIns) return;
        taskUploadIns = upload.render({
            elem: '#task-upload',
            url: '/updateMBTask',
            accept: 'file',
            acceptMime: 'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            before: function () {
                $('#loadingModal').modal('show');
            },
            // multiple: true,
            done: function (res) {
                if (res.respCode == 1) {
                    $.alert({
                        title: 'Warn',
                        content: 'Upload success.',
                    });
                } else {
                    $.alert({
                        title: 'Error!',
                        content: res.respMessage,
                    });
                }
                // initDetail2()
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
    $('#task-download').off('click').on('click', function () {
        window.location.href='/downloadMBTask'
    })
}

const initSelectedAndPage = async function () {
    const getVehicleType = async function () {
        return axios.post('/assign/getVehicleType')
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        });
    }

    const initVehicleType = function (vehicleTypeList) {
        $('.selected-vehicleType').empty();
        let html = `<option value=''>Resource:All</option>`;
        for(let vehicleType of vehicleTypeList){
            if(vehicleType.typeOfVehicle) html += `<option value='${ vehicleType.typeOfVehicle }'>${ vehicleType.typeOfVehicle }</option>`;
        }
        $('.selected-vehicleType').append(html);
    }

    let vehicleTypeList = await getVehicleType();
    initVehicleType(vehicleTypeList);

    const initLayDate = function () {
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
                    tableReload()
                }
            });
            laydate.render({
                elem: '.created-date',
                format: 'dd/MM/yyyy',
                type: 'date',
                lang: 'en',
                trigger: 'click',
                btns: ['clear', 'confirm'],
                done: function () {
                    tableReload()
                }
            });
        });
    }
    initLayDate()

    const initUnit = async function (unitList) {
        if(unitList) {
            let __unitList = unitList.map(unit => { return unit.unit });
            $('.select-hub').empty();
            __unitList = Array.from(new Set(__unitList));
            let html = `<option value="">Hub:All</option>`;
            for (let __unit of __unitList) {
                html += `<option name="unitType" value="${ __unit }">${ __unit }</option>`
            }
            $('.select-hub').append(html); 

            $('.select-hub').off('change').on('change' , function () {
                if(unitList.length > 0) {
                    let selectedUnit = $(this).val();
                    $(".select-node").empty();
                    let html2 = ''
                    if(unitList.length > 1) {
                        html2 = `<option value="">Node:All</option>`;
                    } else {
                        html2 = `<option name="subUnitType" value='${ unitList[0].subUnit }'>Node:All</option>`;
                    }
                    for (let unit of unitList) {
                        if (unit.unit === selectedUnit && unit.subUnit) {
                            if((unit.subUnit).toLowerCase() === 'null') continue
                            html2 += `<option name="subUnitType" data-id="${ unit.id }" value="${ unit.subUnit }">${ unit.subUnit }</option>`
                        } else {
                            continue;
                        }
                    }
                    $(".select-node").append(html2);
                } else {
                    $(".select-node").empty();
                    $(".select-node").append(`<option value="">Node:All</option>`);
                }
                tableReload()
            })
        }
    }
    let unitList = await getHubNode('', '');
    initUnit(unitList);
    $('#collapseExample').show();
    $('.data-detail').show();
    $('.data-sector-detail').hide();
}

const getVehicleList = function (purpose, dataType, vehicleType, hub, node, startDate, endDate, taskId, noOfDriver, unitId) {
    return axios.post('/assign/getVehicleList', { purpose, dataType, vehicleType, hub, node, startDate, endDate, taskId, noOfDriver, unitId })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        });
}

const getDriverAndVehicle = async function(taskId, dataType, vehicleData, driverData) {
    const getTaskIdDriverAndVehicle = function (taskId, dataType) {
        return axios.post('/assign/getTaskIdDriverAndVehicle', { taskId, dataType })
                .then(function (res) {
                    if (res.respCode === 1) {
                        return res.respMessage;
                    } else {
                        console.error(res.respMessage);
                        return null;
                    }
                });
    }

    let vehicleNumber;
    let searchDriverId = null;
    let driverAndVehicle = await getTaskIdDriverAndVehicle(taskId, dataType);
    if(driverAndVehicle){
        if(driverAndVehicle.vehicleNumber) {
            vehicleNumber = driverAndVehicle.vehicleNumber ? driverAndVehicle.vehicleNumber : '';
            let vehicelTotalObj = vehicleData.filter(vehicleObj => vehicleObj.vehicleNo == vehicleNumber)
            if(vehicelTotalObj[0]) $('#search-vehicleNumber').val(`${ driverAndVehicle.vehicleNumber }`);
        }
        if(driverAndVehicle.driverId) {
            searchDriverId = driverAndVehicle.driverId ? driverAndVehicle.driverId : null;
            let driverTotalObj = driverData.filter(driverObj => driverObj.driverId == searchDriverId)
            if(driverTotalObj[0]) {
                let contactNumber = driverAndVehicle.contactNumber ? driverAndVehicle.contactNumber : null;
                $('#search-driverName').attr('data-id', searchDriverId)
                $('#search-driverName').val(`${ driverAndVehicle.name }${ contactNumber ? ` (${ contactNumber })` : '' }`)
            }
        }
    } else {
        vehicleNumber = null;
        searchDriverId = null;
        $('#search-vehicleNumber').val('')
        $('#search-driverName').val('')
        $('#search-driverName').attr('data-id', null)
    }
    let driverAndVehicelObj = { vehicleNumber, searchDriverId }
    return driverAndVehicelObj
}

window.CheckListByTaskId = async function(taskId){
    return await axios.post('/assign/CheckListByTaskId', { taskId })
    .then(function (res) {
        if (res.respCode === 1) {
            return res.respMessage;
        } else {
            console.error(res.respMessage);
            return null;
        }
    });
}

const getDriverListByTaskId = function (userId, vehicleType, hub, node, noOfVehicle, startDate, endDate, unitId) {
    return axios.post('/assign/getDriverListByTaskId', { userId, vehicleType, hub, node, noOfVehicle, startDate, endDate, unitId })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        });
}

window.showPersonDetailEventHandler = async function (el, userType, userId, vehicleType, taskId, choiceArea, choiceNode, reAssignHub, reAssignNode, dataType, startDate, endDate, purpose, mtAdminId, unitId) {
    startDate = moment(startDate).format('YYYY-MM-DD HH:mm')
    endDate = endDate && endDate != '' && endDate != 'null' ? moment(endDate).format('YYYY-MM-DD HH:mm') : null
    let noOfVehicle = $(el).closest('tr').find('#table-request-div').attr('value') == -1 ? true : null;
    let noOfDriver = $(el).closest('tr').find('#table-request-div').attr('value');
    let dataTypeByVehicle = null
    // if(tableName == 'mb') {
    //     taskId = `AT-${ taskId }`
    // }

    const clearFormData = function () {
        $('#myModal').modal('hide');
        // taskId = ''
        $("#search-driverName").val('');
        $("#search-driverName").attr("data-id", null);
        $("#search-driverName").attr("data-unitId", null);
        $('.form-driverName-select').css("display", "none");
        $("#search-vehicleNumber").val('');
        $("#search-vehicleNumber").attr('data-unitId', null);
        $('.form-vehicleNumber-select').css("display", "none");
        tableHub = null
        tableNode = null
        $('.form-vehicleNumber-select').empty()
        $(".form-driverName-select").empty()
        // tableReload2()
        tableBySys.ajax.reload(null, false) 
    }


    const loanOutTaskByTaskId = async function(taskId, startDate, endDate){
        return await axios.post('/assign/loanOutTaskByTaskId', { taskId, startDate, endDate })
        .then(function (res) {
            if (res.respCode === 1) {
                return res.respMessage;
            } else {
                console.error(res.respMessage);
                return null;
            }
        });
    }

    let taskState
    if(dataType.toLowerCase() == 'sys') { 
        dataTypeByVehicle = null
        if($(el).closest('tr').find('#table-request-div').attr('value') > 0) {
            taskState = await CheckListByTaskId(taskId) 
        } else {
            taskState = await loanOutTaskByTaskId(taskId, startDate, endDate) 
        }
    } else if(dataType.toLowerCase() == 'atms'){
        dataTypeByVehicle = null
        if($(el).closest('tr').find('#table-request-div').attr('value') > 0) {
            taskState = await CheckListByTaskId('AT-'+taskId) 
        } else {
            taskState = await loanOutTaskByTaskId('AT-'+taskId, startDate, endDate) 
        }
    }
    // else if(dataType.toLowerCase() == 'mb') {
    //     dataTypeByVehicle = 'mb'
    //     if($(el).closest('tr').find('#table-request-div').attr('value') > 0) {
    //        if(taskId) if(taskId != 'null') taskState = await CheckListByTaskId(taskId) 
    //     } else {
    //         taskState = await loanOutTaskByTaskId('AT-'+mtAdminId, startDate, endDate) 
    //     }
    // }
    if(taskState){
        $.confirm({
            title: 'Warn',
            content: 'The task has started disabling operations.',
            buttons: {
                ok: {
                    btnClass: 'btn-green',
                    action: function () {
                        if(tableName == 'mb'){
                            tableByMb.ajax.reload(null, false) 
                        } else if(tableName == 'sys' || tableName == 'atms') {
                            tableBySys.ajax.reload(null, false) 
                        }
                    }
                }
            }
        });
        return
    }

    $('#myModal').modal('show');
    choiceArea = choiceArea ? choiceArea : '';
    choiceNode = choiceNode ? choiceNode : '';
    tableHub = $(el).closest('tr').find('#task-assign-hub').val();
    tableNode = $(el).closest('tr').find('#task-assign-node').val();
    tableHub = tableHub && tableHub == 'undefined' ? null : tableHub
    tableNode = tableNode && tableNode == 'undefined' ? null : tableNode
    reAssignHub = reAssignHub && reAssignHub == 'undefined' ? null : reAssignHub
    reAssignNode = reAssignNode && reAssignNode == 'undefined' ? null : reAssignNode
    if(reAssignHub || tableHub) {
        if(((tableNode || tableNode == '-') || (reAssignNode || reAssignNode == '-'))) {
            let vehicleDriverId = '';
            let vehicleNumber = vehicleDriverId.vehicleNumber ? vehicleDriverId.vehicleNumber : '';
            let searchDriverId = vehicleDriverId.searchDriverId ? vehicleDriverId.searchDriverId : null;
            let searchDriverNumber = null;
            let start = true;
            let start2 = true;
            if(reAssignNode != "null" || tableNode != "") {
                if($(el).closest('tr').find('#table-request-div').attr('value') > 0) { // driver and vehicle
                    $("#search-driverName").attr("disabled", false);
                    $("#search-vehicleNumber").attr("disabled", false);  
                } else if($(el).closest('tr').find('#table-request-div').attr('value') == 0){ // not driver
                    $("#search-driverName").attr("disabled", true);  
                    $("#search-vehicleNumber").attr("disabled", false); 
                } else if($(el).closest('tr').find('#table-request-div').attr('value') == -1){ // not vehicle
                    $("#search-driverName").attr("disabled", false); 
                    $("#search-vehicleNumber").attr("disabled", true);  
                }

                const initDriverNumber = function (driverId) {
                    if(driverData) {
                        if(driverData.length > 0){
                            for(let driverList of driverData){
                                if(driverId == driverList.driverId){
                                    searchDriverNumber = driverList.contactNumber;
                                }
                            }
                        }
                    }
                } 
                let vehicleData = null
                if($(el).closest('tr').find('#table-request-div').attr('value') != -1) {
                    vehicleData = await getVehicleList(purpose, dataTypeByVehicle, vehicleType, tableHub ? tableHub : reAssignHub, tableNode ? tableNode : reAssignNode, startDate, endDate, taskId, noOfDriver);
                    const initVehicleOption = function () {
                        $("#search-vehicleNumber").off('click').on("click", function () {
                            $('.form-vehicleNumber-select').css("display", "block");
                            initVehicleNumber(this)
                        });

                        $("#search-vehicleNumber").on("keyup", function () {
                            let val = $(this).val()
                            let filterUnits = vehicleData.filter(vehicleList => vehicleList.vehicleNo.toLowerCase().indexOf(val.toLowerCase()) != -1)
                            InsertFilterOption2('.form-vehicleNumber-select', filterUnits)
                            vehicleNumber = '';
                        })
                    
                        $('.form-vehicleNumber-select').on("mousedown", "li", async function () {
                            $("#search-vehicleNumber").html('');
                            let val = $(this).html();
                            let unitId = $(this).attr('data-unitId');
                            vehicleNumber = $(this).text();
                            $("#search-vehicleNumber").val(val);
                            $("#search-vehicleNumber").attr('data-unitId', unitId);
                            $('.form-vehicleNumber-select').css("display", "none");
                        });
                    
                        const initVehicleNumber = async function (e) {
                            $(e).next().css("display", "")
                            $(e).next().find("input").val("");
                            $(e).next().find("input").attr('data-unitId', '');
                            $(e).next().css("display", "block")
                            $('.form-vehicleNumber-select').empty()
                            vehicleData = await getVehicleList(purpose, dataTypeByVehicle, vehicleType, tableHub ? tableHub : reAssignHub, tableNode ? tableNode : reAssignNode, startDate, endDate, taskId, noOfDriver);
                            if(vehicleData) {
                                for(let vehicleList of vehicleData) {
                                    $('.form-vehicleNumber-select').append(`<li data-unitId="${ vehicleList.unitId }">${ vehicleList.vehicleNo }</li>`)
                                    if(!vehicleList.vehicleNo) $('.form-vehicleNumber-select').append(`<li data-unitId="${ vehicleList.unitId }">-</li>`)
                                }
                            }
                        }
                    
                        const InsertFilterOption2 = function (element, filterUnits) {
                            if(filterUnits.length === 0) {
                                start2 = false
                            } else {
                                start2 = true
                            }
                            $(element).css("display", "block");
                            $(element).empty();
                            for (let vehicleList of filterUnits) {
                                $(element).append(`<li data-unitId="${ vehicleList.unitId }">${ vehicleList.vehicleNo }</li>`)
                                if(!vehicleList.vehicleNo) $(element).append(`<li data-unitId="${ vehicleList.unitId }">-</li>`)
                            }
                        }

                        $("#search-vehicleNumber").on('blur', function () {
                            if(taskId) {
                                if(!vehicleNumber) $("#search-vehicleNumber").val('')
                                $('.form-vehicleNumber-select').css("display", "none");
                            }
                        });
                    }
                    initVehicleOption()
                }
                let driverData = null
                // let newUnitIdByMbDriver = $(el).closest('tr').find('#task-assign-node option:selected').attr('data-id') ? $(el).closest('tr').find('#task-assign-node option:selected').attr('data-id') : null;
                if(noOfDriver != 0) {
                    // if(dataType.toLowerCase() == 'mb') { 
                    //     //mb type task and mtAdmin task driver the same
                    //     driverData = await getDriverList(purpose, newUnitIdByMbDriver ? newUnitIdByMbDriver : unitId, vehicleType, startDate, endDate, 'mb');
                    // } else {
                        driverData = await getDriverListByTaskId(userId, vehicleType, tableHub ? tableHub : reAssignHub, tableNode ? tableNode : reAssignNode, noOfVehicle, startDate, endDate);
                    // }
                    const initDriverOption = function () {
                        $("#search-driverName").off('click').on("click", function () {
                            driverNameOnFocus(this)
                            if($(".li-driverName").text() != '-') $('.form-driverName-select').css("display", "block");
                        })
                        
                        $("#search-driverName").on("keyup", function () {
                            let val = $(this).val()
                            let filterUnits = driverData.filter(driverList => driverList.driverName.toLowerCase().indexOf(val.toLowerCase()) != -1)
                            InsertFilterOption('.form-driverName-select', filterUnits)
                            searchDriverId = ''
                        })
                        $('.form-driverName-select').on("mousedown", "li", async function () {
                            $("#search-driverName").html('');
                            let val = $(this).html();
                            searchDriverId = $(this).attr("data-id");
                            let driverId = $(this).attr("data-id");
                            let unitId = $(this).attr("data-unitId");
                            $("#search-driverName").val(val);
                            $("#search-driverName").attr("data-id", driverId);
                            $("#search-driverName").attr("data-unitId", unitId);
                            $('.form-driverName-select').css("display", "none");
                            if($("#search-driverName").val()){
                                let newDriverId = $("#search-driverName").attr("data-id");
                                initDriverNumber(newDriverId);
                            }
                        });
                        
                        const driverNameOnFocus = async function (e) {
                            $(e).next().css("display", "")
                            $(e).next().find("input").val("");
                            $(e).next().css("display", "block");
                            $(".form-driverName-select").empty()
                            // if(purpose) {
                            //     //mb type task and mtAdmin task driver the same
                            //     driverData = await getDriverList(purpose, newUnitIdByMbDriver ? newUnitIdByMbDriver : unitId, vehicleType, startDate, endDate, 'mb');
                            // } else {
                                driverData = await getDriverListByTaskId(userId, vehicleType, tableHub ? tableHub : reAssignHub, tableNode ? tableNode : reAssignNode, noOfVehicle, startDate, endDate);
                            // }
                            if(driverData) {
                                for (let driverList of driverData) {
                                    $(".form-driverName-select").append(`<li class="li-driverName" data-id="${ driverList.driverId }" data-unitId="${ driverList.unitId }">${ driverList.driverName } (${ driverList.contactNumber ? driverList.contactNumber : '' })</li>`)
                                    if(driverList.driverName === null || driverList.driverName === '') $(".form-driverName-select").append(`<li class="li-driverName" data-unitId="${ driverList.unitId }">-</li>`)
                                }
                            }
                            
                        }
                    
                        const InsertFilterOption = function (element, filterUnits) {
                            if(filterUnits.length === 0) {
                                start = false
                            } else {
                                start = true
                            }
                            $(element).css("display", "block");
                            $(element).empty();
                            if(filterUnits) {
                                for (let driverList of filterUnits) {
                                    $(element).append(`<li data-id="${ driverList.driverId }" data-unitId="${ driverList.unitId }">${ driverList.driverName }(${ driverList.contactNumber ? driverList.contactNumber : '' })</li>`)
                                    if(!driverList.driverName) $(element).append(`<li data-unitId="${ driverList.unitId }">-</li>`)
                                }
                            }
                            
                        }

                        $("#search-driverName").on('blur', async function () {
                            if(taskId) {
                                if(!searchDriverId){
                                    $("#search-driverName").val('')
                                    $('#search-driverName').attr("data-id", null)
                                    $('#search-driverName').attr("data-unitId", null)
                                } 
                                $('.form-driverName-select').css("display", "none");
                            }
                        });
                    }
                    initDriverOption()
                }

                //Reproduced data
                let driverAndVehicelObj = await getDriverAndVehicle(taskId, dataType, vehicleData, driverData)
                if(driverAndVehicelObj){
                    if(driverAndVehicelObj.vehicleNumber) vehicleNumber = driverAndVehicelObj.vehicleNumber
                    if(driverAndVehicelObj.searchDriverId) searchDriverId = driverAndVehicelObj.searchDriverId
                }

                const ValidAssignTask = function (data, vehicleNO, driverNO) {
                    let errorLabel = {
                        vehicleNumber: 'Vehicle Number', 
                        searchDriverId: 'Driver Name(Mobile Number)'
                    }
                    for (let key in data) {
                        if(key == 'searchDriverId'){
                            if(driverNO){
                                continue
                            }
                        }
                        if(key == 'vehicleNumber'){
                            if(vehicleNO){
                                continue
                            }
                        }
                       if (data[key] == null || data[key] == "" || data[key].trim() == "") {
                            $.alert({
                                title: 'Warn',
                                content: errorLabel[key] + " is required.",
                            });
                            return false
                        }
                    }
                    return true;
                }

                $('#driverConfirm').off('click').on('click', function () {
                    let ValidAssignTaskObj = {
                        vehicleNumber: $('#search-vehicleNumber').val(),
                        searchDriverId: $('#search-driverName').attr("data-id")
                    }
                    let vehicleNO = $(el).closest('tr').find('#table-request-div').attr('value') == -1 ? true : false;
                    let driverNO = $(el).closest('tr').find('#table-request-div').attr('value') == 0 ? true : false;
                    let state = ValidAssignTask(ValidAssignTaskObj, vehicleNO, driverNO)
                    if(state) {
                        $(this).addClass('btn-taskAssignConfirm')
                        if(dataType.toLowerCase() == 'sys' || dataType.toLowerCase() == 'atms') {
                            assignTask(dataType, tableHub ? tableHub : reAssignHub,  tableNode ? tableNode : reAssignNode, taskId, searchDriverId, vehicleNumber).then((res) => {
                                if(res.respCode === 1) {
                                    // if (data.askRoute) {
                                    //     $('#modal-waiting').modal('show');
                                    //     checkRouteCreate();
                                    // }
                                    clearFormData()
                                }  else {
                                    $.alert({ title: 'Warn', content: res.respMessage });
                                    clearFormData()
                                }
                                $(this).removeClass('btn-taskAssignConfirm')
                            }); 
                        } else if(dataType.toLowerCase() == 'mb') {
                            assignMbTask(tableHub ? tableHub : reAssignHub,  tableNode ? tableNode : reAssignNode, searchDriverId, vehicleNumber, mtAdminId).then((res) => {
                                if(res.respCode === 1) {
                                    // if (data.askRoute) {
                                    //     $('#modal-waiting').modal('show');
                                    //     checkRouteCreate();
                                    // }
                                    clearFormData()
                                }  else {
                                    vehicleData = []
                                    driverData = []
                                    $.alert({ title: 'Warn', content: res.respMessage });
                                    clearFormData()
                                }
                                $(this).removeClass('btn-taskAssignConfirm')
                            });
                        }                                     
                    }  
                });
            } else {
                $.alert({ title: 'Warn',content:'The format of hub and node is incorrect.' })
                clearFormData()
            }
        } else {
            $.alert({ title: 'Warn',content:'The hub and node cannot be empty.' })
            clearFormData()
        }
    } else {
        $.alert({ title: 'Warn',content:'The hub and node cannot be empty.' })
        clearFormData()
    }

    $('#driverCancel').off('click').on('click', function () {
        clearFormData()
    });
}

window.hubChangeEventHandler = function (el) {
    if($(el).val()) {
        tableHub = $(el).val()
        $(el).val(tableHub)
    }
}

window.nodeChangeEventHandler = function (el, taskId, driverId, vehicleNumber, hub) {
    if($(el).val()){
        tableNode = $(el).val();
        if(driverId || vehicleNumber) {
            $(el).closest('tr').find('.btn-assigned').trigger('click');
        } else {
            axios.post('/assign/preAssign', { taskId, hub, tableNode })
            .then(function (res) {
                if (res.respCode === 1) {
                    $.confirm({
                        title: 'Warn',
                        content: 'hub and node update successfully.',
                        buttons: {
                            ok: {
                                btnClass: 'btn-green',
                                action: function () {
                                    if(tableName == 'mb'){
                                        tableByMb.ajax.reload(null, false) 
                                    } else if(tableName == 'sys' || tableName == 'atms') {
                                        tableBySys.ajax.reload(null, false) 
                                    }
    
                                    tableHub = ''
                                    tableNode = ''
                                }
                            }
                        }
                    });
                } else {
                    $.confirm({
                        title: 'Warn',
                        content: 'hub and node update failed.',
                        buttons: {
                            ok: {
                                btnClass: 'btn-green',
                                action: function () {
                                    tableHub = ''
                                    tableNode = ''
                                }
                            }
                        }
                    });
                    console.error(res.respMessage);
                    return null;
                }
            })
        }
        setInterval(function () {
            $(el).val(tableNode)
        },3000)
    }
}

window.nodeChangeEventHandlerByMb = function (el, taskId, driverId, vehicleNumber, hub) {
    if($(el).val()){
        tableNode = $(el).val();
        if(driverId || vehicleNumber) {
            $(el).closest('tr').find('.btn-assigned').trigger('click');
        } else {
            axios.post('/assign/preMbAssign', { taskId, hub, tableNode })
            .then(function (res) {
                if (res.respCode === 1) {
                    $.confirm({
                        title: 'Warn',
                        content: 'hub and node update successfully.',
                        buttons: {
                            ok: {
                                btnClass: 'btn-green',
                                action: function () {
                                    if(tableName == 'mb'){
                                        tableByMb.ajax.reload(null, false) 
                                    } else if(tableName == 'sys' || tableName == 'atms') {
                                        tableBySys.ajax.reload(null, false) 
                                    }
                                    tableHub = ''
                                    tableNode = ''
                                }
                            }
                        }
                    });
                } else {
                    $.confirm({
                        title: 'Warn',
                        content: 'hub and node update failed.',
                        buttons: {
                            ok: {
                                btnClass: 'btn-green',
                                action: function () {
                                    tableHub = ''
                                    tableNode = ''
                                }
                            }
                        }
                    });
                    console.error(res.respMessage);
                    return null;
                }
            })
        }
        setInterval(function () {
            $(el).val(tableNode)
        },3000)
    }
}

window.initDetail = async function () {
    const initDataTableBySys = function (unitList) {
        tableBySys = $('.sys-data-list').DataTable({
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
                url: "/assign/getAssignableTaskList2",
                type: "POST",
                data: function (d) {
                    let unit = [];
                    let vehicleType = $(".selected-vehicleType option:selected").val() ? $(".selected-vehicleType option:selected").val() : '';
                    let tripNo = $(".tripId").val() ? $(".tripId").val() : '';
                    let execution_date = $(".execution-date").val() ? $(".execution-date").val() : '';
                    if (execution_date) {
                        if (execution_date.indexOf('~') != -1) {
                            const dates = execution_date.split(' ~ ')
                            if(dates.length > 0) {
                                dates[0] = moment(dates[0], 'DD/MM/YYYY').format('YYYY-MM-DD')
                                dates[1] = moment(dates[1], 'DD/MM/YYYY').format('YYYY-MM-DD')
                                execution_date = dates.join(' ~ ')
                            }
                        } else {
                            execution_date = moment(execution_date, 'DD/MM/YYYY').format('YYYY-MM-DD')
                        }
                    }
                    let created_date = $(".created-date").val() ? moment($(".created-date").val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
                    currentArea = $(".select-hub option:selected").val()
                    currentNode = $(".select-node option:selected").val()
                    let endDateOrder = null
                    for (let orderField of d.order) {
                        if(orderField.column == 7) endDateOrder = orderField.dir;
                    }    
                    let option = { 
                        "taskType": tableName == 'sys' ? 'sys' : 'atms',
                        "userType": userType, 
                        "userId": userId, 
                        "node": currentNode, 
                        "unit": unit ? unit : "", 
                        "vehicleType": vehicleType ? vehicleType : "", 
                        "tripNo": tripNo ? tripNo : "", 
                        "execution_date": execution_date ? execution_date : "", 
                        "created_date": created_date ? created_date : "", 
                        "hub": currentArea ,
                        "vehicleNo": $('.screen-vehicleNo').val() ? $('.screen-vehicleNo').val() : null,
                        "driverName": $('.screen-driverName').val() ? $('.screen-driverName').val() : null,
                        // "taskStatus": tableName == 'sys' ? $('.selected-taskStatus').val() ?? null : null,
                        "taskStatus": $('.selected-taskStatus').val(),
                        "endDateOrder": endDateOrder,
                        "pageNum": d.start, 
                        "pageLength": d.length
                    }
                    return option
                },
            },   
            "initComplete" : function (settings, json) {
                // row margin-top: fill by div height;
                // $('.saf-driver-table tbody tr').before('<div style="width: 100%;height: 10px;"/>');
                $(".saf-driver-table thead tr th:first").removeClass('sorting_asc');
            },
            "columns": [
                { 
                    data: null, 
                    title: "S/N",
                    sortable: false ,
                    "render": function (data, type, full, meta) {
                        return meta.row + 1 + meta.settings._iDisplayStart
                    }
                },
                { 
                    class: 'task-assign-hub',
                    title: 'Hub', 
                    data: 'hub',
                    sortable: false,
                    defaultContent: ''
                },
                {   
                    class: 'task-assign-node',
                    title: 'Node', 
                    data: 'node',
                    sortable: false,
                    defaultContent: '' ,
                    render: function (data, type, full, meta) {
                        if(data){
                            if(data.toLowerCase() == 'null') {
                                return ''
                            } else {
                                return data
                            }
                        } else {
                            return ''
                        }
                    } 
                },
                { 
                    title: 'Task ID', 
                    data: 'tripNo', 
                    sortable: false,
                    defaultContent: '' , 
                    render: function(data, type, full, meta) {
                        let taskId = full.referenceId ? `AT-${ full.taskId }` : full.taskId;
                        return `${ data ?? '-' }<br/>${ taskId ?? '-' }`
                    } 
                },
                { 
                    title: 'Created Date', 
                    data: 'createdAt', 
                    sortable: false ,
                    defaultContent: '', 
                    render: function(data, type, full, meta) {
                        return `${ data ? moment(data).format('DD/MM/YYYY') : '' }`
                    } 
                },
                { 
                    title: 'Request', 
                    data: 'preParkDate', 
                    sortable: false ,
                    defaultContent: '' ,
                    render: function (data, type, full, meta) {
                        if(full.vehicleType != '-'){
                            if(!full.instanceId){
                                if(full.noOfDriver >= full.driverNo) {
                                    return `<div value="1" id='table-request-div'>Pre-Park</div>`
                                } else {
                                    return `<div value="0" id='table-request-div'>Pre-Park</div>`
                                }
                            } else {
                                if(full.noOfDriver >= full.driverNo) {
                                    return `<div value="1" id='table-request-div'>Both</div>`
                                } else {
                                    return `<div value="0" id='table-request-div'>Vehicle Only</div>`
                                }
                            }
                        } else {
                            return `<div value="-1" id='table-request-div'>TO Only</div>`
                        }
                        
                    } 
                },
                { 
                    title: 'Resource', 
                    data: 'vehicleType', 
                    sortable: false ,
                    defaultContent: '' 
                },
                { 
                    class: 'dataTable-executionTime',
                    title: 'Execution Time', 
                    data: 'endDate', 
                    sortable: true ,
                    defaultContent: '',
                    render: function (data, type, full, meta) {
                        if(full.startDate && full.endDate){
                            if(full.preParkDate){
                                return `
                                <div style="margin:0 auto;text-align: left">
                                    <div>${ full.startDate ? moment(full.startDate).format('DD/MM/YYYY HH:mm') : '' } to</div>
                                    <div>${ full.endDate ? moment(full.endDate).format('DD/MM/YYYY HH:mm') : '' }</div>
                                    <div><span style="font-weight: bold;">Pre-Park:</span> ${ full.preParkDate ? moment(full.preParkDate).format('DD/MM/YYYY HH:mm') : '' } </div>
                                </div>
                                ` 
                            } else {
                                return `
                                <div style="margin:0 auto;text-align: left">
                                    <div>${ full.startDate ? moment(full.startDate).format('DD/MM/YYYY HH:mm') : '' } to</div>
                                    <div>${ full.endDate ? moment(full.endDate).format('DD/MM/YYYY HH:mm') : '' }</div>
                                </div>
                                ` 
                            }
                            
                        } else {
                            if(full.startDate) return `<div>${ full.startDate ? moment(full.startDate).format('DD/MM/YYYY HH:mm') : '' }</div>`
                            if(full.endDate) return `<div>${ full.endDate ? moment(full.endDate).format('DD/MM/YYYY HH:mm') : '' }</div>`
                            if(full.preParkDate) return `<div><span style="font-weight: bold;">Pre-Park:</span> ${ full.preParkDate ? moment(full.preParkDate).format('DD/MM/YYYY HH:mm') : '' } </div>` 
                        }
                    }  
                },
                { 
                    title: 'Task Status', 
                    data: 'taskStatus', 
                    sortable: false ,
                    defaultContent: '',
                    render: function (data, type, full, meta) {
                        if(!data) return ''
                        if(data.toLowerCase() === 'assigned' || data.toLowerCase() === 'assigned (system)'){
                            return `<div style="color: #1B9063;font-weight: bold;">${ data }</div>`
                        } else if(data.toLowerCase() === 'unassigned') {
                            return `<div style="color: #1D308E;font-weight: bold;">${ data }</div>`
                        } else if(data.toLowerCase() === 'declined') {
                            return `<div style="color: #E76D70;font-weight: bold;">${ data }</div>`
                        } else {
                            return ''
                        }
                    } 
                },
                {
                    title: "Location",
                    data: "pickupDestination", 
                    sortable: false ,
                    defaultContent: '' ,
                    render: function (data, type, full, meta) {
                        if (!data) {
                            return "";
                        }
                        return `<div>
                            <div class="color-pickup-destination">${ full.pickupDestination }</div>
                            <div class="icon-down-div"><span class="iconfont icon-down"></span></div>
                            <div class="color-dropoff-destination">${ full.dropoffDestination }</div>
                        </div>`
                    }
                },
                { 
                    title: 'POC Details', 
                    data: 'poc', 
                    sortable: false ,
                    defaultContent: '',
                    render: function (data, type, full, meta) {
                        return `<div>${ data ? data : '' }</div>
                            <div>${ full.pocNumber ? full.pocNumber : '' }</div>`
                    } 
                },
                { 
                    title: 'Driver Details', 
                    data: 'name', 
                    sortable: false , 
                    defaultContent: '' ,
                    render: function (data, type, full, meta) {
                        let nric = full.nric ? full.nric : '';
                        let newNric;
                        if(nric) newNric = nric.slice((nric.length - 4), nric.length);
                        let html = '';
                        if(data){
                            html += `<div>${ data ? data : '' }(${ newNric ? newNric : nric })</div>
                            <div>${ full.contactNumber ? full.contactNumber : '' }</div>`
                        }
                        if (full.reassignedDriverName) {
                            return `${html}</div><div>Pending Approve: ${ full.reassignedDriverName }</div>`
                        } else {
                            return html;
                        }
                    }
                },
                { 
                    title: 'Vehicle Details', 
                    data: 'vehicleNumber', 
                    sortable: false ,
                    defaultContent: '' ,
                    render: function (data, type, full, meta) {
                        if (full.reassignedVehicleNumber) {
                            return `<div>${ data ? data : '' }</div><div>Pending Approve: ${ full.reassignedVehicleNumber }</div>`
                        } else {
                            return `<div>${ data ? data : '' }</div>`
                        }
                    }
                },
                { 
                    title: 'Reassigned By', 
                    data: 'reassignedUserName', 
                    sortable: false ,
                    defaultContent: '' ,
                    render: function (data, type, full, meta) {
                        return `<div>${ data ? data : '' }</div>`
                    }
                },
                { 
                    title: 'Action', 
                    data: 'vehicleType', 
                    sortable: false,
                    defaultContent: '' ,
                    render: function (data, type, full, meta) {
                        let dataType = tableName;
                        let operationList = (full.operation).toUpperCase().split(',')
                        if(full.driverId || full.vehicleNumber) {
                            if(full.checkResult) {
                                return ``
                            } else {
                                if (full.autoMatchResourceTask == 1 && full.reassignApproveStatus == 'Pending Approve') {
                                    if (operationList.includes('REASSIGN APPROVE')) {
                                        return `<button type="button" class="btn-reassign-approve custom-btn-green" onclick="reassignApproveSystemTask('${ dataType }', '${ full.taskId }', 'pass')">Re-Assign Approve</button>
                                            <button type="button" class="btn-reassign-approve custom-Reject" onclick="reassignApproveSystemTask('${ dataType }', '${ full.taskId }', 'reject')">Re-Assign Reject</button>
                                        ` 
                                    }
                                    return '';
                                } else if (operationList.includes('ASSIGN')) {
                                    return `<button type="button" class="btn-assigned custom-Reassign" onclick="showPersonDetailEventHandler(this,'${ userType }', '${ userId }', '${ data }', '${ full.taskId }', '${ currentArea }', '${ currentNode }', '${full.hub}', '${full.node}', '${ dataType }', '${ full.startDate }', '${ full.endDate }', '${ full.purposeType }')">Re-Assign</button>` 
                                } 
                                return ``
                            }
                        } else {
                            if (operationList.includes('ASSIGN')) {
                                return `<button type="button" class="btn-assigned custom-assign" onclick="showPersonDetailEventHandler(this,'${ userType }', '${ userId }', '${ data }', '${ full.taskId }', '${ currentArea }', '${ currentNode }', '${full.hub}', '${full.node}', '${ dataType }', '${ full.startDate }', '${ full.endDate }', '${ full.purposeType }')">Assign</button>`
                            }
                        }
                    }
                }
            ],
            fnCreatedRow: async function(nRow, aData, iDataIndex) {
                if(!aData.checkResult) {
                    if(unitList) {
                        let __unitList = unitList.map(unit => { return unit.unit });
                        __unitList = Array.from(new Set(__unitList));
                        let html = '<option></option>'
                        for (let __unit of __unitList) {
                            if(__unit) html += `<option name="unitType" value="${ __unit }">${ __unit }</option>`
                        }
                        if (aData.checkResult || (aData.autoMatchResourceTask == 1 && aData.reassignApproveStatus == 'Pending Approve')) {
                            $('td:eq(1)', nRow).empty()
                            $('td:eq(1)', nRow).prepend(`<label>${aData.hub}</label>`);
                        } else {
                            $('td:eq(1)', nRow).empty()
                            $('td:eq(1)', nRow).prepend(`<select onchange="hubChangeEventHandler(this)" class="form-select" id="task-assign-hub">${html}</select>`);
                        }
                        
                        $('#task-assign-hub', nRow).off('change').on('change' , function () {
                            let selectedUnit = $('#task-assign-hub', nRow).val();
                            $("#task-assign-node", nRow).remove();
                            let html2 = `<option></option>`;
                            for (let unit of unitList) {
                                if (unit.unit === selectedUnit) {
                                    // if((unit.subUnit).toLowerCase() === 'null') continue
                                    html2 += `<option name="subUnitType" id="mtAdmin-subUnitType" data-id="${ unit.id }" value="${ unit.subUnit ? unit.subUnit : '-'}">${ unit.subUnit ? unit.subUnit : '-'}</option>`
                                }
                            }
                            
                            if (aData.checkResult || (aData.autoMatchResourceTask == 1 && aData.reassignApproveStatus == 'Pending Approve')) {
                                $('td:eq(2)', nRow).empty()
                                $('td:eq(2)', nRow).prepend(`<label>${aData.node}</label>`);
                            } else {
                                $('td:eq(2)', nRow).empty()
                                $('td:eq(2)', nRow).prepend(`<select  onchange="nodeChangeEventHandler(this, '${ aData.taskId }',${ aData.driverId ? `'${ aData.driverId }'` : null },${ aData.vehicleNumber ? `'${ aData.vehicleNumber }'` : null }, '${ $('#task-assign-hub', nRow).val() }')" class="form-select" id="task-assign-node">${html2}</select>`);
                            }
                        })
                        $('#task-assign-hub', nRow).val(aData.hub)
                        $('#task-assign-hub', nRow).trigger('change')
                        $('#task-assign-node', nRow).val(aData.node ? aData.node : '-')
                        if(Cookies.get('userType') == 'UNIT') {
                            $('td:eq(1)', nRow).empty()
                            $('td:eq(1)', nRow).prepend(`<label>${aData.hub}</label>`);
                        }
                        if(Cookies.get('node')) {
                            $('td:eq(2)', nRow).empty()
                            $('td:eq(2)', nRow).prepend(`<label>${aData.node}</label>`);
                        }
                    }
                }
                
            }
        });
    }
    let globalUnitList = await getHubNode(userType, userId);
    initDataTableBySys(globalUnitList)
}

window.showJustification = function (e) {
    let row = tableByMb.row($(e).data('row')).data()
    $.alert({
        title: 'Justification',
        content: row.cancelledCause
    });
}

window.cancalTaskByMb = async function (mtAdminId) {
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
                    return axios.post('/assign/cancalTaskByMb',{ mtAdminId, cancelledCause: cancelledCause })
                    .then(function (res) {
                        if(res.respMessage === false){
                            $.confirm({
                                title: 'Warn',
                                content: 'The task has already started and cannot be operated.',
                                buttons: {
                                    ok: {
                                        btnClass: 'btn-green',
                                        action: function () {
                                            tableByMb.ajax.reload(null, false)
                                        }
                                    }
                                }
                            });
                        } else {
                            if (res.respCode === 1) {
                                tableByMb.ajax.reload(null, false)
                                return res.respMessage;
                            } else {
                                $.alert({
                                    title: 'Warn',
                                    content: res.respMessage
                                });
                                tableByMb.ajax.reload(null, false)
                                return null;
                            }
                        }
                    });
                }
            },
            no: {
                btnClass: 'btn-outline-secondary', 
                action: function(){
                    tableByMb.ajax.reload(null, false)
                }
            }
        }
    });
} 

window.editTaskByMb = async function(mtAdminId, taskId, unitId, driverNum, el) {
    let onlyStatus = $(el).closest('tr').find('#table-request-div').attr('value')
    let noOfVehicle = $(el).closest('tr').find('#table-request-div').attr('value') == -1 ? true : null;
    if(onlyStatus > 0) { // driver and vehicle
        $("#driver").attr("disabled", false);
        $("#vehicleNo").attr("disabled", false);  
    } else if(onlyStatus == 0){ // not driver
        $("#driver").attr("disabled", true);  
        $("#vehicleNo").attr("disabled", false); 
    } else if(onlyStatus == -1){ // not vehicle
        $("#driver").attr("disabled", false); 
        $("#vehicleNo").attr("disabled", true);  
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
    let taskState = await CheckListByTaskId(taskId) 
    if(taskState){
        $.confirm({
            title: 'Warn',
            content: 'The task has started disabling operations.',
            buttons: {
                ok: function (){
                    tableByMb.ajax.reload(null, false)
                }
            }
        });
        return
    }

    $('#mtAdminTaskModal').modal('show')
    $('#mtAdminTaskModal .modal-title').text('Edit Task')

    const clearPageData = function () {
        $('#purposeType').val('');
        $('#purposeType').trigger('change')
        $('#additionalRemarks').val('');
        $('#remarks').val('');
        $('#serviceMode').val('');
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
    }
    const initModalPage = async function(){
        $('.requestType-select').off('change').on('change', function(){
            if($('.requestType-select').val()){
                if($('.requestType-select').val() == 2) {
                    driverNum = 1;
                    // noOfDriver = 1;
                    noOfVehicle = false;
                    $('#driver').css('background-color', 'white')
                    $('#driver').removeAttr('disabled', 'disabled')
                    $('#vehicleNo').css('background-color', 'white')
                    $('#vehicleNo').removeAttr('disabled', 'disabled')
                }
                if($('.requestType-select').val() == 1){
                    driverNum = 0
                    // noOfDriver = 0
                    onlyStatus = 0
                    $('#vehicleNo').val("");
                    $('#vehicleNo').attr("data-unitid", null)
                    $('#driver').attr('data-value', null);
                    $('#driver').val("");
                    $('#driver').attr("data-unitid", null);
                    $('#driver').attr("data-id", null);
                    $('#vehicleNo').css('background-color', 'white')
                    $('#vehicleNo').removeAttr('disabled', 'disabled')
                    $('#driver').css('background-color', 'rgb(233, 236, 239)')
                    $('#driver').prop('disabled', 'disabled')
                }
                if($('.requestType-select').val() == 0){
                    noOfVehicle = true
                    onlyStatus = -1
                    $('#vehicleNo').val("");
                    $('#vehicleNo').attr("data-unitid", null)
                    $('#driver').attr('data-value', null);
                    $('#driver').val("");
                    $('#driver').attr("data-unitid", null);
                    $('#driver').attr("data-id", null);
                    $('#vehicleNo').css('background-color', 'rgb(233, 236, 239)')
                    $('#vehicleNo').prop('disabled', 'disabled')
                    $('#driver').css('background-color', 'white')
                    $('#driver').removeAttr('disabled', 'disabled')
                }
            }
        })
        const verifyVehicleTypeAndVehicleAndDriver = async function () {
            const verifyVehicleType = async function(startDate, endDate, vehicleType, hub, node, unitId){
                return await axios.post('/mtAdmin/verifyVehicleType', { startDate, endDate, vehicleType, hub, node, unitId })
                .then(function (res) {
                    if (res.respCode === 1) {
                        return res.respMessage;
                    } else {
                        console.error(res.respMessage);
                        return null;
                    }
                });
            }
            if ($('#periodStartDate').val() && $('#periodEndDate').val() && $('#typeOfVehicle').val()) {
                // let newVehicleType = await verifyVehicleType($('#periodStartDate').val(), $('#periodEndDate').val(), $('#typeOfVehicle').val(), null, null, unitId);
                let newStartTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                let newEndTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                let newVehicleType = await verifyVehicleType(newStartTime, newEndTime, $('#typeOfVehicle').val(), null, null, unitId);
                if(newVehicleType) {
                    if(onlyStatus == 0 || onlyStatus > 0) {
                        // let newVehicleList = await getVehicleList($('#purposeType').val(), 'mb', $('#typeOfVehicle').val(), null, null, $('#periodStartDate').val(), $('#periodEndDate').val(), taskId, driverNum, unitId);
                        let newVehicleList = await getVehicleList($('#purposeType').val(), 'mb', $('#typeOfVehicle').val(), null, null, newStartTime, newEndTime, taskId, driverNum, unitId);
                        let result = newVehicleList.some(item=> item.vehicleNo == $('#vehicleNo').val())
                        if(!result) {
                            $('#vehicleNo').val('');
                            $('#vehicleNo').attr('data-unitId', null)
                            $('#vehicleNo-shadow .form-search-select').empty()   
                        }
                    }
                   
                    if(onlyStatus == -1 || onlyStatus > 0) {
                        // let newDriverList = await getDriverListByTaskId(userId, $('#typeOfVehicle').val(), null, null, noOfVehicle, $('#periodStartDate').val(), $('#periodEndDate').val(), unitId);
                        let newDriverList = await getDriverListByTaskId(userId, $('#typeOfVehicle').val(), null, null, noOfVehicle, newStartTime, newEndTime, unitId);
                        let driverResult = newDriverList.some(item=> item.driverId == $('#driver').attr('data-id'))
                        if(!driverResult) {
                            $('#driver').val('');
                            $('#driver').attr('data-unitId', null)
                            $('#driver').attr('data-id', null)
                            $('#driver').attr('data-value', null)
                            $('#driver-shadow .form-search-select').empty()   
                        }
                    }
                } else {
                    clearDriverAndVehicleAndType()
                }
            } else {
                clearDriverAndVehicleAndType()
            }
        }
        const initDateTime = async function(){
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
                            let endTime = moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm')
                            if (moment(value).isSameOrAfter(moment(endTime))) {
                                $.alert({
                                    title: 'Warn',
                                    content: 'End time should be later than start time!',
                                });
                                $('#periodEndDate').val(null)
                            }
                            verifyVehicleTypeAndVehicleAndDriver()
                        } else {
                            clearDriverAndVehicleAndType()
                        }
                    }
                };
                optStr['min'] = moment().format('YYYY-MM-DD HH:mm:ss')
                if($('#purposeType').val() == 'WPT') {
                    let newDate = new Date();
                    let year = newDate.getFullYear();
                    let month = newDate.getMonth();
                    let day = newDate.getDate();
                    let week = newDate.getDay();
                    let endDate = new Date(year, month, day - week + 7)      
                    let weekEndDate = moment(endDate).format('YYYY-MM-DD') + ' 23:59:59'
                    optStr['max'] = moment(weekEndDate).format('YYYY-MM-DD HH:mm:ss')
                }
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
                            let startDate = moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm')
                            if (moment(startDate).isSameOrAfter(moment(value))) {
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
                if($('#purposeType').val() == 'WPT') {
                    let newDate = new Date();
                    let year = newDate.getFullYear();
                    let month = newDate.getMonth();
                    let day = newDate.getDate();
                    let week = newDate.getDay();
                    let endDate = new Date(year, month, day - week + 7)      
                    let weekEndDate = moment(endDate).format('YYYY-MM-DD') + ' 23:59:59'
                    optStr['max'] = moment(weekEndDate).format('YYYY-MM-DD HH:mm:ss')
                }
                laydate.render(optStr);
            });
        }
        initDateTime()
        const getPurposeModelist = async function (unitId) {
            return await axios.post('/mtAdmin/getPurposeModelist', { creator: unitId })
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
            html+= '<option>Training</option>'
            // for(let purposeType of purposeTypeList){
            //     html+= `<option data-id="${ purposeType.id }">${ purposeType.purposeName }</option>`;
            // }
            $('#purposeType').append(html)

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
                    $('#driver').val('');
                    $('#driver').attr('data-unitId', null)
                    $('#driver').attr('data-id', null)
                    $('#driver').attr('data-value', null)
                    $('#driver-shadow .form-search-select').empty()   
                    $('#vehicleNo').val('');
                    $('#vehicleNo').attr('data-unitId', null)
                    $('#vehicleNo-shadow .form-search-select').empty()
                    initDateTime();
                }
            });
        }
        initPurposeType(await getPurposeModelist(unitId));
    
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
        initUnitTypePage(await getHubNode(userType, userId))

        const initServiceMode = function (serviceModeList) {
            $('#serviceMode').empty()
            let html = ``;
            for(let serviceMode of serviceModeList){
                html+= `<option>${ serviceMode }</option>`;
            }
            $('#serviceMode').append(html)
        }
    
        let serviceModeList = ['1-way', 'Disposal'];
        initServiceMode(serviceModeList)

        const initLocation = async function() {
            const GetDestination = async function(){
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

            $('#pickupDestination').off('click').on('click', function () {
                DestinationOnFocus(this)
            });
        
            $('#dropoffDestination').off('click').on('click', function () {
                DestinationOnFocus(this)
            });
            let locationList = null
            const DestinationOnFocus = async function (e) {
                $('.search-select').css("display", "");
                $(".search-select input").css("display", "block");
                $(".search-select input").val("");
                $(e).next().css("display", "block")
                $(e).next().find(".form-search-select").empty()
                locationList = await GetDestination()
                for (let item of locationList) {
                    $(e).next().find(".form-search-select").append(`<li data-id="${item.id}">${item.locationName}</li>`)
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
                } else {
                    let attrDisabled = $("#dropoffDestination").attr("disabled")
                    if (attrDisabled) {
                        $("#dropoffDestination").val(val)
                        $("#dropoffDestination").attr("data-secured", secured)
                        $("#dropoffDestination").attr("data-id", id)
                    }
                }
            })
    
            $(".location-div .search-select input").on("keyup", function () {
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
        }
        initLocation()
        $(document).off('click').on("click", function (e) {
            let target = e.target;
            if (target.id != "search1" && target.id != "search2" && target.id != "pickupDestination" && target.id != "dropoffDestination"
             && target.id != "search3" && target.id != "search4" && target.id != "vehicleNo" && target.id != "driver" && target.id != "search5" && target.id != "typeOfVehicle") {
                $('.search-select').css("display", "");
            }
        });

        const initVehicleType = function () {
            $('#typeOfVehicle').off('click').on('click', function () {
                if ($('#periodStartDate').val() && $('#periodEndDate').val()) {
                    resourceOnFocus(this)
                }
            })

            const getVehicleType = async function(startDate, endDate, unit, subUnit, unitId){
                return await axios.post('/mtAdmin/getVehicleType', { startDate, endDate, unit, subUnit, unitId })
                .then(function (res) {
                    if (res.respCode === 1) {
                        return res.respMessage;
                    } else {
                        console.error(res.respMessage);
                        return null;
                    }
                });                
            }

            let vehicleTypeList = null
            const resourceOnFocus = async function (e) {
                if($('#periodEndDate').val()){
                    $('.search-select').css("display", "");
                    $(".search-select input").css("display", "block");
                    $(".search-select input").val("");
                    $(e).next().css("display", "block")
                    $(e).next().find(".form-search-select").empty()
                    let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                    let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                    // vehicleTypeList = await getVehicleType($('#periodStartDate').val(), $('#periodEndDate').val(), null, null, unitId);
                    vehicleTypeList = await getVehicleType(startTime, endTime, null, null, unitId);
                    if(vehicleTypeList.length > 0) vehicleTypeList = vehicleTypeList.sort()
                    $('#typeOfVehicle-shadow .form-search-select').empty()
                    for(let vehicleType of vehicleTypeList){
                        $(e).next().find(".form-search-select").append(`<li>${ vehicleType }</li>`)
                    } 
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
            })
        }
        initVehicleType()
       
        const initVehicle = async function (taskId, driverNum) {
            $('#vehicleNo').off('click').on('click', function () {
                if($('.requestType-select').val() == 2) {
                    driverNum = 1;
                    noOfVehicle = false;
                }
                if($('.requestType-select').val() == 1){
                    driverNum = 0
                    onlyStatus = 0
                }
                if($('.requestType-select').val() == 0){
                    noOfVehicle = true
                    onlyStatus = -1
                }
                vehicleOnFocus(this, taskId, driverNum)
            });
            let vehicleList = null
            const vehicleOnFocus = async function (e, taskId, driverNum) {
                if($('#typeOfVehicle').val()){
                    $('.search-select').css("display", "");
                    $(".search-select input").css("display", "block");
                    $(".search-select input").val("");
                    $(e).next().css("display", "block")
                    $(e).next().find(".form-search-select").empty()
                    let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                    let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                    // vehicleList = await getVehicleList($('#purposeType').val(), 'mb', $('#typeOfVehicle').val(), null, null, $('#periodStartDate').val(), $('#periodEndDate').val(), taskId, driverNum, unitId);
                    vehicleList = await getVehicleList($('#purposeType').val(), 'mb', $('#typeOfVehicle').val(), null, null, startTime, endTime, taskId, driverNum, unitId);
                    $('#vehicleNo-shadow .form-search-select').empty()
                    for(let vehicle of vehicleList){
                        $(e).next().find(".form-search-select").append(`<li data-unitId="${ vehicle.unitId }">${vehicle.vehicleNo}</li>`)
                    } 
                } 
            }

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

            $("#vehicleNo-shadow .form-search-select").on("mousedown", "li", function () {
                let val = $(this).text()
                let id = $(this).attr("data-unitid")
                $(this).parent().parent().prev().val(val)
                $(this).parent().parent().prev().attr("data-unitid", id)
                $(this).parent().parent().css("display", "none")
            })
        }
        initVehicle(taskId, driverNum)

        const initDriver = async function () {
            $('#driver').off('click').on('click', function() {
                driverOnFocus(this, unitId)
            } )
            
            let driverList = null 
            const driverOnFocus = async function (e, unitId) {
                $('.search-select').css("display", "");
                $(".search-select input").css("display", "block");
                $(".search-select input").val("");
                $(e).next().css("display", "block")
                $(e).next().find(".form-search-select").empty();
                if(!$('#purposeType').val()){ $.alert({ title: 'warn', content: 'Purpose can not be empty.' }); return } 
                let startTime = $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
                // driverList = await getDriverListByTaskId(userId, $('#typeOfVehicle').val(), null, null, noOfVehicle, $('#periodStartDate').val(), $('#periodEndDate').val(), unitId);
                driverList = await getDriverListByTaskId(userId, $('#typeOfVehicle').val(), null, null, noOfVehicle, startTime, endTime, unitId);
                if(driverList.length > 0) driverList = driverList.sort((a, b) => a.driverName.localeCompare(b.driverName));
                $('#driver-shadow .form-search-select').empty()
                for(let driver of driverList){
                    $(e).next().find(".form-search-select").append(`<li data-id="${driver.driverId}" data-unitId="${ driver.unitId }" value="${driver.driverName}">${driver.driverName}(${ driver.contactNumber ? driver.contactNumber : '-'})</li>`)
                } 
            }

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

        $('#mtAdminCancel').off('click').on('click', function () {
            // mtAdminId = null;
            clearPageData();
        });

        const initPage = async function (mtAdminId) {
            const getMBTaskById = async function(mtAdminId){
                return await axios.post('/assign/getMBTaskById', { mtAdminId })
                .then(function (res) {
                    if (res.respCode === 1) {
                        return res.respMessage;
                    } else {
                        console.error(res.respMessage);
                        return null;
                    }
                });                
            }
            let taskMtAdminObj = await getMBTaskById(mtAdminId)
            $('#purposeType').val(taskMtAdminObj.purpose)
            $('#purposeType').trigger('change')
            $('#additionalRemarks').val(taskMtAdminObj.activityName)
            let requestType = 0;
            if(taskMtAdminObj.driverNum > 0 && taskMtAdminObj.needVehicle > 0) {
                requestType = 2
            } else {
                if(taskMtAdminObj.driverNum == 0) {
                    requestType = 1
                } 
                if(taskMtAdminObj.needVehicle == 0 ) {
                    requestType = 0
                } 
            }
            $(".requestType-select").val(requestType);
            $(".requestType-select").trigger('change')
            $('#remarks').val(taskMtAdminObj.remarks)
            $('.hub-select').val(taskMtAdminObj.hub)
            $('.hub-select').trigger('change')
            $('.node-select').val(taskMtAdminObj.node)
            $('#periodStartDate').val(taskMtAdminObj.startDate ? moment(taskMtAdminObj.startDate).format('DD/MM/YYYY HH:mm') : '');
            $('#periodEndDate').val(taskMtAdminObj.endDate ? moment(taskMtAdminObj.endDate).format('DD/MM/YYYY HH:mm') : '');
            if( $('#purposeType').val()){
                $('#typeOfVehicle').val(taskMtAdminObj.vehicleType)
                verifyVehicleTypeAndVehicleAndDriver()
                if(taskMtAdminObj.vehicleNumber) $('#vehicleNo').val(taskMtAdminObj.vehicleNumber);
                $('#vehicleNo').attr("data-unitid", taskMtAdminObj.unitId)
                if(taskMtAdminObj.driverId) {
                    $('#driver').attr('data-value', taskMtAdminObj.driverName)
                    $('#driver').val(`${ taskMtAdminObj.driverName }(${ taskMtAdminObj.contactNumber ? taskMtAdminObj.contactNumber : '-' })`)
                    $('#driver').attr("data-unitid", taskMtAdminObj.unitId)
                    $('#driver').attr("data-id", taskMtAdminObj.driverId)
                }
            }
            $('#pickupDestination').val(taskMtAdminObj.reportingLocation);
            $('#dropoffDestination').val(taskMtAdminObj.destination);
        }
        initPage(mtAdminId)
    }
    initModalPage()

    $('#mtAdminTaskConfirm').off('click').on('click', function () {
        let endTime = $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
        let endDateTime = endTime ? new Date(Date.parse((moment(endTime)))) : null;
        let nowDateTime = new Date(Date.parse((moment().format('YYYY-MM-DD HH:mm'))));
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
                endDate: $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null
            }
            const validAssignByMB = function (data) {
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
                    mobileNumber: 'Mobile Number',
                    startDate: 'Start Date',
                    endDate: 'End Date',
                }
                for (let key in data) {
                    if(!data[key]) {
                        if(onlyStatus == -1) if(key == 'vehicleNumber') continue
                        if(onlyStatus == 0) if(key == 'driverName') continue
                        if($('#purposeType').val() != 'Others') {
                            if(key == 'remarks') continue
                        }  
                        $.alert({
                            title: 'Warn',
                            content: `${ errorLabel[key] } is required.`,
                        });
                        return false;
                    }
                }
                return true
            }
            let state = validAssignByMB(ValidAssignTaskObj)
            if(!state) return
            $('#mtAdminTaskConfirm').addClass('btn-disabled')
            let mtAdmin = {
                id: mtAdminId,
                activityName: $('#additionalRemarks').val(),
                purpose: $('#purposeType').val(),
                unitId: unitId,
                startDate: $('#periodStartDate').val() ? moment($('#periodStartDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null,
                endDate: $('#periodEndDate').val() ? moment($('#periodEndDate').val(), 'DD/MM/YYYY HH:mm').format('YYYY-MM-DD HH:mm') : null,
                vehicleNumber: $('#vehicleNo').val(),
                vehicleType: $('#typeOfVehicle').val() ? $('#typeOfVehicle').val() : '',
                driverName: $('#driver').attr('data-value') ? $('#driver').attr('data-value') : null,
                driverId:  $('#driver').attr('data-id') ? $('#driver').attr('data-id') : null,
                remarks: $('#remarks').val() ? $('#remarks').val() : '',
                category: $("input[name='category']:checked").val() ? 'MV' : null,
                serviceMode: $('#serviceMode').val(),
                reportingLocation: $('#pickupDestination').val(),
                destination: $('#dropoffDestination').val(),
                dataType: 'mb'
            }
            if($('.requestType-select').val()){
                if($('.requestType-select').val() == 2) {
                    mtAdmin.driverNum = 1;
                    mtAdmin.needVehicle = 1;
                }
                if($('.requestType-select').val() == 1){
                    mtAdmin.driverNum = 0;
                    mtAdmin.needVehicle = 1;
                }
                if($('.requestType-select').val() == 0){
                    mtAdmin.driverNum = 1;
                    mtAdmin.needVehicle = 0;
                }
            }
            if(mtAdminId) {
                axios.post('/mtAdmin/updateMtAdminByMtAdminId',{ mtAdmin, taskId, businessType: 'atms task assign' })
                .then(function (res) {
                    if (res.respCode === 1) {
                        $('#mtAdminTaskConfirm').removeClass('btn-disabled')
                        $('#mtAdminTaskModal').modal('hide');
                        tableByMb.ajax.reload(null, false)
                        mtAdmin = null
                        clearPageData(); 
                        return
                    } else {
                        $('#mtAdminTaskConfirm').removeClass('btn-disabled')
                        $('#mtAdminTaskModal').modal('hide');
                        $.alert({
                            title: 'Warn',
                            content: res.respMessage,
                        });
                        mtAdmin = null
                        clearPageData();
                        return
                    }
                });
            }
            tableByMb.ajax.reload(null, false) 
        } else {
            $.alert({
                title: 'Warn',
                content: `The task time has expired. Please select a new one.`,
            });
        }
    })
}

// window.initDetail2 = async function () {
//     const initDataTable = function (unitList) {
//         // tableByMb = $('.mb-data-list').DataTable({
//         //     "ordering": true,
//         //     "searching": false,
//         //     "paging": true,
//         //     "autoWidth": false,
//         //     "fixedHeader": true,
//         //     "scrollX": "auto",
//         //     // "scrollY": "700px",
//         //     "scrollCollapse": true,
//         //     "language": PageHelper.language(),
//         //     "lengthMenu": PageHelper.lengthMenu(),
//         //     "dom": PageHelper.dom(),
//         //     "pageLength": PageHelper.pageLength(),
//         //     "processing": false,
//         //     "serverSide": true,
//         //     "destroy": true,
//         //     "sAjaxDataProp": "data",
//         //     "ajax": {
//         //         url: "/assign/getAssignableTaskListByMtAdmin",
//         //         type: "POST",
//         //         data: function (d) {
//         //             let vehicleType = $(".selected-vehicleType option:selected").val() ? $(".selected-vehicleType option:selected").val() : '';
//         //             let execution_date = $(".execution-date").val() ? $(".execution-date").val() : null;
//         //             if (execution_date) {
//         //                 if (execution_date.indexOf('~') != -1) {
//         //                     const dates = execution_date.split(' ~ ')
//         //                     if(dates.length > 0) {
//         //                         dates[0] = moment(dates[0], 'DD/MM/YYYY').format('YYYY-MM-DD')
//         //                         dates[1] = moment(dates[1], 'DD/MM/YYYY').format('YYYY-MM-DD')
//         //                         execution_date = dates.join(' ~ ')
//         //                     }
//         //                 } else {
//         //                     execution_date = moment(execution_date, 'DD/MM/YYYY').format('YYYY-MM-DD')
//         //                 }
//         //             }
//         //             currentArea = $(".select-hub option:selected").val()
//         //             currentNode = $(".select-node option:selected").val()
//         //             let endDateOrder = null
//         //             let taskIdOrder = null
//         //             let order = d.order;
//         //             for (let orderField of order) {
//         //                 if(orderField.column == 4) {
//         //                     taskIdOrder = orderField.dir;
//         //                 } else if(orderField.column == 7){
//         //                     endDateOrder = orderField.dir;
//         //                 }
//         //             }    
//         //             taskIdOrder = taskIdOrder ? taskIdOrder : order[0].dir;
//         //             let option = { 
//         //                 "userId": userId, 
//         //                 "node": currentNode, 
//         //                 "vehicleType": vehicleType ? vehicleType : "", 
//         //                 "execution_date": execution_date ? execution_date : null,
//         //                 "created_date": $('.created-date').val() ? moment($(".created-date").val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null,
//         //                 "hub": currentArea ,
//         //                 "vehicleNo": $('.screen-vehicleNo').val() ? $('.screen-vehicleNo').val() : null,
//         //                 "driverName": $('.screen-driverName').val() ? $('.screen-driverName').val() : null,
//         //                 "endDateOrder": endDateOrder,
//         //                 "taskIdOrder": taskIdOrder,
//         //                 "pageNum": d.start, 
//         //                 "pageLength": d.length
//         //             }

//         //             return option
//         //         },
//         //     },   
//         //     "columns": [
//         //         { 
//         //             data: null, 
//         //             title: "S/N",
//         //             sortable: false ,
//         //             "render": function (data, type, full, meta) {
//         //                 return meta.row + 1 + meta.settings._iDisplayStart
//         //             }
//         //         },
//         //         { 
//         //             class: 'task-assign-hub',
//         //             title: 'Hub', 
//         //             data: 'hub',
//         //             sortable: false,
//         //             defaultContent: ''
//         //         },
//         //         {   
//         //             class: 'task-assign-node',
//         //             title: 'Node', 
//         //             data: 'node',
//         //             sortable: false,
//         //             defaultContent: '-' ,
//         //             render: function (data, type, full, meta) {
//         //                 if(data){
//         //                     if(data.toLowerCase() == 'null') {
//         //                         return ''
//         //                     } else {
//         //                         return data
//         //                     }
//         //                 } else {
//         //                     return ''
//         //                 }
//         //             } 
//         //         },
//         //         { 
//         //             title: 'Task ID', 
//         //             data: 'taskId', 
//         //             sortable: false,
//         //             defaultContent: '' 
//         //         },
//         //         { 
//         //             title: 'AT ID', 
//         //             data: 'id', 
//         //             sortable: true,
//         //             defaultContent: '' 
//         //         },
//         //         { 
//         //             title: 'Request', 
//         //             data: '', 
//         //             sortable: false ,
//         //             defaultContent: '' ,
//         //             render: function (data, type, full, meta) {
//         //                 if(full.driverNum > 0 && full.needVehicle > 0){
//         //                     return `<div value="1" id='table-request-div'>Both</div>`
//         //                 } else {
//         //                     if(full.driverNum <= 0){
//         //                         return `<div value="0" id='table-request-div'>Vehicle Only</div>`
//         //                     }
//         //                     if(full.needVehicle <= 0){
//         //                         return `<div value="-1" id='table-request-div'>TO Only</div>`
//         //                     }
//         //                 }
//         //             } 
//         //         },
//         //         { 
//         //             title: 'Resource', 
//         //             data: 'vehicleType', 
//         //             sortable: false ,
//         //             defaultContent: '' 
//         //         },
//         //         { 
//         //             class: 'dataTable-executionTime',
//         //             title: 'Execution Time', 
//         //             data: 'endDate', 
//         //             sortable: true ,
//         //             defaultContent: '',
//         //             render: function (data, type, full, meta) {
//         //                 if(full.startDate && full.endDate){
//         //                     return `
//         //                     <div style="margin:0 auto;text-align: left">
//         //                         <div>${ full.startDate ? moment(full.startDate).format('DD/MM/YYYY HH:mm') : '' } to</div>
//         //                         <div>${ full.endDate ? moment(full.endDate).format('DD/MM/YYYY HH:mm') : '' }</div>
//         //                     </div>
//         //                     ` 
//         //                 } else {
//         //                     if(full.startDate) return `<div>${ full.startDate ? moment(full.startDate).format('DD/MM/YYYY HH:mm') : '' }</div>`
//         //                     if(full.endDate) return `<div>${ full.endDate ? moment(full.endDate).format('DD/MM/YYYY HH:mm') : '' }</div>`
//         //                 }
//         //             } 
//         //         }, 
//         //         {
//         //             title: "Location",
//         //             data: "reportingLocation", 
//         //             sortable: false ,
//         //             defaultContent: '' ,
//         //             render: function (data, type, full, meta) {
//         //                 if (!data) {
//         //                     return "";
//         //                 }
//         //                 return `<div>
//         //                     <div class="color-pickup-destination">${ full.reportingLocation }</div>
//         //                     <div class="icon-down-div"><span class="iconfont icon-down"></span></div>
//         //                     <div class="color-dropoff-destination">${ full.destination }</div>
//         //                 </div>`
//         //             }
//         //         },
//         //         { 
//         //             title: 'POC Details', 
//         //             data: 'poc', 
//         //             sortable: false ,
//         //             defaultContent: '',
//         //             render: function (data, type, full, meta) {
//         //                 return `<div>${ data ? data : '' }</div>
//         //                     <div>${ full.mobileNumber ? full.mobileNumber : '' }</div>`
//         //             } 
//         //         },
//         //         { 
//         //             title: 'Driver Details', 
//         //             data: 'driverName', 
//         //             sortable: false , 
//         //             defaultContent: '' ,
//         //             render: function (data, type, full, meta) {
//         //                 let nric = full.nric ? full.nric : '';
//         //                 let newNric;
//         //                 if(nric) newNric = nric.slice((nric.length - 4), nric.length);
//         //                 if(data){
//         //                     return `<div>${ data ? data : '' }(${ newNric ? newNric : nric })</div>
//         //                     <div>${ full.contactNumber ? full.contactNumber : '' }</div>`
//         //                 }
//         //             }
//         //         },
//         //         { 
//         //             title: 'Vehicle Details', 
//         //             data: 'vehicleNumber', 
//         //             sortable: false ,
//         //             defaultContent: '' ,
//         //             render: function (data, type, full, meta) {
//         //                 return `<div>${ data ? data : '' }</div>`
//         //             }
//         //         },
//         //         { 
//         //             title: 'Justification', 	
//         //             data: 'cancelledCause', 
//         //             sortable: false ,
//         //             defaultContent: '-',
//         //             render: function (data, type, full, meta) {
//         //                 if (full.cancelledDateTime) {
//         //                     return `
//         //                     <div>
//         //                         <span class="d-inline-block text-truncate" style="max-width: 90px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this);">
//         //                             ${ data ? data : '' }
//         //                         </span><br>
//         //                         <label class="fw-bold">Amended by:</label> <label>${ full.amendedByUsername ? full.amendedByUsername : '' }</label><br>
//         //                         <label class="fw-bold">Date Time:</label> <label>${ moment(full.cancelledDateTime).format('DD/MM/YYYY HH:mm:ss') }</label>
//         //                     </div>
//         //                     `
//         //                 } else {
//         //                     return '-'
//         //                 }
//         //             }
//         //         },
//         //         { 
//         //             title: 'Action', 
//         //             data: 'vehicleType', 
//         //             sortable: false,
//         //             defaultContent: '' ,
//         //             render: function (data, type, full, meta) {
//         //                 let operationList = (full.operation).toUpperCase().split(',')

//         //                 if(full.cancelledDateTime) {
//         //                     // if (operationList.includes('CANCEL')) {
//         //                         return `<div style="font-weight: bold;">${full.vehicleStatus ?  _.capitalize(full.vehicleStatus) : 'Cancelled' }</div>`
//         //                     // }
//         //                 } else {
//         //                     if(full.driverId || full.vehicleNumber) {
//         //                         if(full.checkResult) {
//         //                             return `<div style="font-weight: bold;">${ _.capitalize(full.vehicleStatus) }</div>`
//         //                         } else {
//         //                             let html = `` 
//         //                             if (operationList.includes('ASSIGN')) {
//         //                                 html += ` <button type="button" class="btn-assigned custom-Reassign" onclick="showPersonDetailEventHandler(this, '${ userType }','${ userId }', '${ data }', '${ full.taskId }', '${ full.driverNum }', '${ currentArea }', '${ currentNode }', '${full.hub}', '${full.node}', 'mb', '${ full.startDate }', '${ full.endDate }', '${ full.purpose }', ${ full.id }, ${ full.unitId })">Re-Assign</button> `
//         //                             }
//         //                             if (operationList.includes('EDIT')) {
//         //                                 html += ` <button type="button" class="btn-edit-task" onclick="editTaskByMb(${ full.id }, '${ full.taskId }', ${ full.unitId }, ${ full.driverNum }, this)">Edit</button> `
//         //                             }

//         //                             if (operationList.includes('CANCEL')) {
//         //                                 html += ` <button type="button" class="btn-cancal" onclick="cancalTaskByMb(${ full.id })">Cancel</button> `
//         //                             }
                                    
//         //                             return html
//         //                         }
//         //                     } else {
//         //                         let html = ``
//         //                         if (operationList.includes('ASSIGN')) {
//         //                             html += ` <button type="button" class="btn-assigned custom-assign" onclick="showPersonDetailEventHandler(this, '${ userType }','${ userId }', '${ data }', '${ full.taskId }', '${ full.driverNum }', '${ currentArea }', '${ currentNode }', '${full.hub}', '${full.node}', 'mb', '${ full.startDate }', '${ full.endDate }', '${ full.purpose }', ${ full.id }, ${ full.unitId })">Assign</button> `
//         //                         }
//         //                         if (operationList.includes('EDIT')) {
//         //                             html += ` <button type="button" class="btn-edit-task" onclick="editTaskByMb(${ full.id }, '${ full.taskId }', ${ full.unitId }, ${ full.driverNum }, this)">Edit</button> `
//         //                         }
//         //                         if (operationList.includes('CANCEL')) {
//         //                             html += ` <button type="button" class="btn-cancal" onclick="cancalTaskByMb(${ full.id })">Cancel</button> `
//         //                         }
                              
//         //                         return html                                
//         //                     }
//         //                 }
//         //             }
//         //         }
//         //     ],
//         //     fnCreatedRow: async function(nRow, aData, iDataIndex) {
//         //         if(!aData.checkResult && !aData.cancelledDateTime) {
//         //             if(unitList) {
//         //                 let __unitList = unitList.map(unit => { return unit.unit });
//         //                 __unitList = Array.from(new Set(__unitList));
//         //                 let html = '<option></option>'
//         //                 for (let __unit of __unitList) {
//         //                     if(__unit) html += `<option name="unitType" value="${ __unit }">${ __unit }</option>`
//         //                 }
//         //                 if (aData.checkResult) {
//         //                     $('td:eq(1)', nRow).empty()
//         //                     $('td:eq(1)', nRow).prepend(`<label>${aData.hub}</label>`);
//         //                 } else {
//         //                     $('td:eq(1)', nRow).empty()
//         //                     $('td:eq(1)', nRow).prepend(`<select onchange="hubChangeEventHandler(this)" class="form-select" id="task-assign-hub">${html}</select>`);
//         //                 }
                        
//         //                 $('#task-assign-hub', nRow).off('change').on('change' , function () {
//         //                     let selectedUnit = $('#task-assign-hub', nRow).val();
//         //                     $("#task-assign-node", nRow).remove();
//         //                     let html2 = `<option></option>`;
//         //                     for (let unit of unitList) {
//         //                         if (unit.unit === selectedUnit) {
//         //                             html2 += `<option name="subUnitType" id="mtAdmin-subUnitType" data-id="${ unit.id }" value="${ unit.subUnit ? unit.subUnit : '-'}">${ unit.subUnit ? unit.subUnit : '-'}</option>`
//         //                         }
//         //                     }
                            
//         //                     if (aData.checkResult) {
//         //                         $('td:eq(2)', nRow).empty()
//         //                         $('td:eq(2)', nRow).prepend(`<label>${aData.node}</label>`);
//         //                     } else {
//         //                         $('td:eq(2)', nRow).empty()
//         //                         $('td:eq(2)', nRow).prepend(`<select  onchange="nodeChangeEventHandlerByMb(this, '${ aData.id }',${ aData.driverId ? `'${ aData.driverId }'` : null }, ${ aData.vehicleNumber ? `'${ aData.vehicleNumber }'` : null }, '${ $('#task-assign-hub', nRow).val() }')" class="form-select" id="task-assign-node">${html2}</select>`);
//         //                     }
//         //                 })
//         //                 $('#task-assign-hub', nRow).val(aData.hub)
//         //                 $('#task-assign-hub', nRow).trigger('change')
//         //                 $('#task-assign-node', nRow).val(aData.node ? aData.node : '-')
//         //                 if(Cookies.get('userType') == 'UNIT') {
//         //                     $('td:eq(1)', nRow).empty()
//         //                     $('td:eq(1)', nRow).prepend(`<label>${aData.hub}</label>`);
//         //                 }
//         //                 if(Cookies.get('node')) {
//         //                     $('td:eq(2)', nRow).empty()
//         //                     $('td:eq(2)', nRow).prepend(`<label>${aData.node}</label>`);
//         //                 }
//         //             }
//         //         }
//         //     }
//         // });
//     }

//     let globalUnitList = await getHubNode(userType, userId);
//     initDataTable(globalUnitList)
// }

window.reassignApproveSystemTask = async function (dataType, taskId, optType) {
    $.confirm({
        title: 'Info',
        content: `Are you sure to ${optType} the reassign apply?`,
        buttons: {
            cancel: function() {},
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    axios.post('/assign/reassignMvTaskApprove', { dataType, taskId, optType }).then(function (res) {
                        let respCode = res.respCode ? res.respCode : res.data.respCode;
                        let respMessage = res.respMessage ? res.respMessage : res.data.respMessage;
                        if (respCode == 1) {
                            $.alert({
                                title: 'Info',
                                content: 'Operation Success.'
                            });
                            tableBySys.ajax.reload(null, false) 
                        } else {
                            $.alert({
                                title: 'Error',
                                content: 'Operation fail:' + respMessage
                            });
                        }
                    });
                }
            }
        }
    });
}

const assignTask = async function (dataType, hub, node, taskId, driverId, vehicleNo) {
    return axios.post('/assign/reassignMvTask', { dataType, hub, node, taskId, driverId, vehicleNo })
        .then(function (res) {
            return res;
        });
}

const assignMbTask = async function (hub, node, driverId, vehicleNo, mtAdminId) {
    return axios.post('/assign/assignMbTask', { hub, node, driverId, vehicleNo, mtAdminId })
        .then(function (res) {
            return res;
        });
}

window.getHubNode = async function(userType, userId) {
    return axios.post('/mtAdmin/getHubNode').then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const clickSelect = function (){
   if(tableName == 'sys' || tableName == 'atms')  {
        $('.selected-taskStatus').off("change").on("change", function () {
                tableReload()
            }
        )
    }

    $(".tripId").on("keyup", function () {
            tableReload()
        }
     )
     $(".selected-vehicleType").off("change").on("change", function () {
            tableReload()
        }
    )
    $(".select-hub").off("change").on("change", function () {
            tableReload()
        }
    )
    $(".select-node").off("change").on("change", function () {
            tableReload()
        }
    )
    $('.screen-vehicleNo').on('keyup', function () {
        let number = ($(this).val()).split("")
        if(number.length > 2 || number.length == 0) {
            tableReload()
        }
    })
    $('.screen-driverName').on('keyup', function () {
        let number = ($(this).val()).split("")
        if(number.length > 3 || number.length == 0) {
            tableReload()
        }
    })
}