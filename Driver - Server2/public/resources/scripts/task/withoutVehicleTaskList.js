let table
jconfirm.defaults = {
    animation: 'top',
    closeAnimation: 'top',
    animationSpeed: 400,
    typeAnimated: true,
}

$(function () {
    initTaskTable();
    initPage();

    setInterval(() => {
        table.ajax.reload(null, false);
    }, 60000)
})

const getHubNode = async function() {
    return await axios.post('/unit/getHubNodeList')
    .then(function (res) {
        if (res.respCode == 1 || res.data.respCode === 1) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        } else {
            console.error(res.respMessage ? res.respMessage : res.data.respMessage);
            return null;
        }
    });
}
const getGroupList = async function() {
    return await axios.post('/getGroupList')
    .then(function (res) {
        if (res.data) {
            if (res.data.respCode === 1) {
                return res.data.respMessage;
            } else {
                console.error(res.data.respMessage);
                return null;
            }
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
const initGroup = async function (groupList) {
    $('.select-group').empty();
    let html = `<option value="">Group: All</option>`;
    if (Cookies.get('userType').toLowerCase() == 'customer') {
        html = ''
    }
    for (let group of groupList) {
        html += `<option name="group" value="${ group.id }">${ group.groupName }</option>`
    }
    $('.select-group').append(html);

    $('.select-group').off('change').on('change', function () {
        table.ajax.reload(null, true)
    })
}
const initUnit = async function (hubNodeList) {
    if(hubNodeList) {
        if (Cookies.get('userType') !== 'HQ' && Cookies.get('node')) {
            // While node user, directly show hub/node
            $('.select-hub').html(`<option name="unitType" value="${ hubNodeList[0].hub }">${ hubNodeList[0].hub }</option>`); 
            $('.select-node').html(`<option name="unitType" value="${ hubNodeList[0].nodeList }">${ hubNodeList[0].nodeList }</option>`); 
        } else {
            let hubList = hubNodeList.map(hubNode => hubNode.hub);
            $('.select-hub').empty();
            let html = `<option value="">Hub:All</option>`;
            for (let hub of hubList) {
                html += `<option name="unitType" value="${ hub }">${ hub }</option>`
            }
            $('.select-hub').append(html); 

            $('.select-hub').off('change').on('change', function () {
                let selectedUnit = $(this).val();
                $(".select-node").empty();
                let html2 = `<option value="">Node:All</option>`;
                for (let hubNode of hubNodeList) {
                    if (hubNode.hub === selectedUnit) {
                        let nodeList = hubNode.nodeList ? hubNode.nodeList.split(',') : []
                        for (let node of nodeList) {
                            html2 += `<option name="subUnitType" value="${ node }">${ node }</option>`
                        }
                    } else {
                        continue;
                    }
                }
                $(".select-node").append(html2);
                table.ajax.reload(null, true)
            })

            $(".select-hub").trigger('change')
        }

        $('.select-node,.select-riskLevel').off('change').on('change', () => {
            table.ajax.reload(null, true)
        })
    }
}

const initPage = async function () {
    const initPurpose = async function () {
        // let purposeList = await axios.get('/mtAdmin/getPurposeModeType')
        //     .then(function (res) {
        //         if (res.respCode === 1) {
        //             return res.respMessage;
        //         } else {
        //             console.error(res.respMessage);
        //             return [];
        //         }
        //     });

        let purposeList = await axios.post('/vehicle/getPurpose').then(result => {
            if (result.respCode == 1) {
                return [].concat(result.respMessage.mtPurposeList, result.respMessage.systemPurposeList)
            } else {
                console.error(result.respMessage)
                return [];
            }
        })

        $('.select-taskPurpose').empty();
        let html = '<option value="">Purpose: All</option>'
        for (let purpose of purposeList) {
            html += `
                <option value="${ purpose.purposeName }">${ purpose.purposeName }</option>
            `
        }
        $('.select-taskPurpose').append(html);
    }

    $('#clearAll').on('click', function () {
        $(".select-group").find("option").eq(0).prop("selected",true)
        $(".select-hub").find("option").eq(0).prop("selected",true)
        $('.select-hub').trigger('change')
        $(".driverName").val("")
        $(".select-taskPurpose").val("")
        $(".taskActivity").val("")
        
        table.ajax.reload(null, true)
    });

    $('.driverName').on('keyup', _.debounce(() => {
        if ($('.driverName').val().length >= 4 || $('.driverName').val().length == 0) {
            table.ajax.reload(null, true)
        }
    }, 500))
    $('.taskActivity').on('keyup', _.debounce(() => {
        table.ajax.reload(null, true)
    }, 500))
    
    $('.select-taskPurpose').on('change', () => {
        table.ajax.reload(null, true)
    })

    if (Cookies.get('userType').toLowerCase() != 'customer') {
        let hubNodeList = await getHubNode();
        initUnit(hubNodeList);
    } else {
        let groupList = await getGroupList();
        initGroup(groupList);
    }

    initPurpose();
}

const initTaskTable = function () {
    // $(".user-hub-params").show();
    // $(".user-node-params").show();
    table = $('.task-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
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
            url: "/dashboard/getWithoutVehicleTaskList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length
                
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.driverName = $('.driverName').val().trim()
                
                if ($('.select-group').val()) {
                    params.group = Number($('.select-group').val())
                }
                params.purpose = $('.select-taskPurpose').val()
                params.activity = $('.taskActivity').val()
                return params
            },
        },
        columns: [
            {
                "title": "Hub/Node",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ full.hub ?? '-' }<br>${ full.node ?? '-' }`
                }
            },
            {
                "data": "taskId",
                "title": "Task ID",
                sortable: false,
                render: function (data, type, full, meta) {
                    if(full.dataFrom.toUpperCase() == 'SYSTEM'){
                        return `${ full.indentId ?? '-' }<br>${ full.taskId ?? '-' }`
                    } else {
                        return `${ full.taskId ?? '-' }`
                    }
                }
            },
            {
                "data": "driverName",
                "title": "Driver Name",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ data ? data : '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
                }
            },
            {
                // "data": "vehicleType",
                "title": "Vehicle No/Resource",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNumber ?? '-'  }<br>${ full.vehicleType ?? '-' }`
                }
            },
            {
                "data": "driverStatus",
                "title": "Status",
                sortable: false,
                render: function (data) {
                    return `  
                        <div style="background-color:#693E3B; color: white; border-radius: 3px; padding: 3px 3px; font-weight: bolder;">
                            ${ _.capitalize('pending') }
                        </div>
                    `
                }
            },
            {
                "data": "activity",
                "title": "Activity",
                defaultContent: '-',
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) return '-'
                    if (data.length < 20) {
                        return data
                    } else {
                        return `
                            <span class="d-inline-block text-truncate" style="max-width: 250px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showActivity(this);" role="button" tabindex="0">
                                ${ data ? data : '' }
                            </span>
                        `
                    }
                }
            },
            {
                "data": "purpose",
                "title": "Purpose",
                defaultContent: '-',
                sortable: false,
            },
            {
                "title": "Execution Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label> <label>${ full.indentStartTime ? moment(full.indentStartTime).format('YYYY-MM-DD HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.indentEndTime ? moment(full.indentEndTime).format('YYYY-MM-DD HH:mm') : '-' }</label>
                    `
                }
            },
            {
                "title": "Actual Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    let timeHtml = `
                        <label class="fw-bold">Start:</label> <label>${ full.mobileStartTime ? moment(full.mobileStartTime).format('YYYY-MM-DD HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.mobileEndTime ? moment(full.mobileEndTime).format('YYYY-MM-DD HH:mm') : '-' }</label>
                    `
                    return timeHtml;
                }
            },
            {
                title: "Location",
                sortable: false ,
                defaultContent: '' ,
                render: function (data, type, full, meta) {
                    return `<div>
                        <div class="color-pickup-destination">${ full.pickupDestination }</div>
                        <div class="icon-down-div"><span class="iconfont icon-down"></span></div>
                        <div class="color-dropoff-destination">${ full.dropoffDestination }</div>
                    </div>`
                }
            },
            {
                title: "Action",
                sortable: false ,
                defaultContent: '' ,
                render: function (data, type, full, meta) {
                    return `<button class="btn btn-sm custom-btn-green" onclick="assignTaskVehicle('${full.taskId}')" title="Assign">Assign</button>`
                }
            }
        ],
    });
}

let assignedVehicleNo = '';
let canSelectVehicleList = [];
const assignTaskVehicle = function(taskId) {
    assignedVehicleNo = '';
    canSelectVehicleList = [];
    $(".current-assign-taskid").text(taskId);
    $("#without-vehicle-task-modal").modal('show');

    $("#search-vehicleNumber").off('click').on("click", function () {
        $('.form-vehicleNumber-select').css("display", "block");
        initVehicleNumber(this)
    });

    $("#search-vehicleNumber").off("keyup").on("keyup", function () {
        let val = $(this).val()
        let filterUnits = canSelectVehicleList.filter(vehicleInfo => vehicleInfo.vehicleNo.toLowerCase().indexOf(val.toLowerCase()) != -1)
        InsertFilterOption2('.form-vehicleNumber-select', filterUnits)
        assignedVehicleNo = '';
    })

    $('.form-vehicleNumber-select').off("mousedown").on("mousedown", "li", async function () {
        $("#search-vehicleNumber").html('');
        let val = $(this).html();
        let unitId = $(this).attr('data-unitId');
        assignedVehicleNo = $(this).text();
        $("#search-vehicleNumber").val(val);
        $("#search-vehicleNumber").attr('data-unitId', unitId);
        $('.form-vehicleNumber-select').css("display", "none");
    });

    $("#search-vehicleNumber").off("blur").on('blur', function () {
        if(taskId) {
            if(!assignedVehicleNo) $("#search-vehicleNumber").val('')
            $('.form-vehicleNumber-select').css("display", "none");
        }
    });

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

    $("#vehicleAssignConfirm").off('click').on('click', async function() {
        if (!assignedVehicleNo) {
            return;
        }
        let taskId = $(".current-assign-taskid").text();
        await axios.post('/assign/reassignTaskVehicle', { taskId, vehicleNo: assignedVehicleNo }).then(function (res) {
            if (res.respCode == 1) {
                table.ajax.reload(null, false);

                $("#search-vehicleNumber").off("keyup");
                $("#search-vehicleNumber").off('click');
                $("#search-vehicleNumber").off("blur");
                $('.form-vehicleNumber-select').off("mousedown")
        
                $("#vehicleAssignConfirm").off('click');
                $("#vehicleAssignCancel").off('click');
                $(".current-assign-taskid").text('');
                $("#without-vehicle-task-modal").modal('hide');
                $("#search-vehicleNumber").val('');
                assignedVehicleNo = '';
            } else {
                $.alert(res.respMessage);
            }
        });
    });

    $("#vehicleAssignCancel").off('click').on('click', function() {
        $("#search-vehicleNumber").off("keyup");
        $("#search-vehicleNumber").off('click');
        $("#search-vehicleNumber").off("blur");
        $('.form-vehicleNumber-select').off("mousedown")

        $("#vehicleAssignConfirm").off('click');
        $("#vehicleAssignCancel").off('click');
        $(".current-assign-taskid").text('');
        $("#without-vehicle-task-modal").modal('hide');
    });
}

const initVehicleNumber = async function (e) {
    $(e).next().css("display", "")
    $(e).next().find("input").val("");
    $(e).next().find("input").attr('data-unitId', '');
    $(e).next().css("display", "block")
    $('.form-vehicleNumber-select').empty()

    let taskId = $(".current-assign-taskid").text();

    await axios.post('/mtAdmin/getVehicleListByTaskId', { taskId }).then(function (res) {
        if (res.respCode === 1) {
            canSelectVehicleList = res.respMessage;

            if(canSelectVehicleList) {
                for(let vehicleInfo of canSelectVehicleList) {
                    $('.form-vehicleNumber-select').append(`<li data-unitId="${ vehicleInfo.unitId }">${ vehicleInfo.vehicleNo }</li>`)
                    if(!vehicleInfo.vehicleNo) {
                        $('.form-vehicleNumber-select').append(`<li data-unitId="${ vehicleInfo.unitId }">-</li>`)
                    }
                }
            }
        } else {
            console.error(res.respMessage);
        }
    });
}