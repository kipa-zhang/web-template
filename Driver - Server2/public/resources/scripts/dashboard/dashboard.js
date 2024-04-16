let table
jconfirm.defaults = {
    animation: 'top',
    closeAnimation: 'top',
    animationSpeed: 400,
    typeAnimated: true,
}

$(function () {
    // $('.search-input').on('keyup', _.debounce( FilterOnChange, 500 ))
    // initMTRacTable();
    initTaskTable();
    initPage();
    // InitFilter();

    setInterval(() => {
        table.ajax.reload(null, false)
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

    layui.use('laydate', function(){
        let laydate = layui.laydate;
        laydate.render({
            elem: '.selectedDate',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            format: 'dd/MM/yyyy',
            btns: ['clear', 'confirm'],
            done: (value) => {
                table.ajax.reload(null, true)
            }
        });
    })

    $('#select-customer').on('change', async function () {
        if ($(this).prop('checked')) {
            $('.show-group-select').removeClass('d-none').show()
            $('.hub-node-btn').hide()

            let groupList = await getGroupList();
            initGroup(groupList);
        } else {
            $('.show-group-select').hide()
            $('.hub-node-btn').show()
            
            let hubNodeList = await getHubNode();
            initUnit(hubNodeList);
        }

        table.ajax.reload(null, true)
    })

    $('.btn-dashboard').off('click').on('click', function () {
        $('.btn-dashboard').removeClass('active')
        $(this).addClass('active')
        let item = $(this).data('item')

        $('.select-taskStatus').parent().hide()
        if (item == 'task') {
            $('.table-common.active').hide().removeClass('active')
            $('.task-table-container').show().addClass('active')
            initTaskTable();
            $('.select-riskLevel').parent().hide()
            $('.taskId').parent().show()
            $('.vehicleNo').parent().show()
            $('.select-taskStatus').parent().show()
            // $('.select-group').parent().show()

            $('.select-taskPurpose').parent().show()
            $('.taskActivity').parent().show()

            $('.customer-button').show();
            if ($('.customer-button input').prop('checked')) {
            } else {
                $('.hub-node-btn').show();
            }
            $('.taskId').show();
        } else if(item == 'urgent-duty') {
            $('.table-common.active').hide().removeClass('active')
            $('.urgent-duty-table-container').show().addClass('active')
            initUrgentDutyTable()

            $('.customer-button').hide();
            $('.hub-node-btn').show();
            $('.taskId').hide();
            $('.user-group-params').hide();
            $('.vehicleNo').parent().show()
            $('.select-taskStatus').parent().show()
            $('.select-taskPurpose').parent().hide()
            $('.taskActivity').parent().hide();
            $('.select-riskLevel').parent().hide()
        } else if (item == 'urgent-task') {
            $('.table-common.active').hide().removeClass('active')
            $('.urgent-task-table-container').show().addClass('active')
            initUrgentTaskTable();
            $('.taskId').show();
            $('.customer-button').show();
            $('.hub-node-btn').show();

            $('.user-group-params').show();
            $('.vehicleNo').parent().show()
            $('.select-taskStatus').parent().show()
            $('.select-taskPurpose').parent().hide()
            $('.taskActivity').parent().hide();
            $('.select-riskLevel').parent().hide()

            if ($('.customer-button input').prop('checked')) {
                $('.hub-node-btn').hide();
            } else {
                $('.hub-node-btn').show();
            }
        
        } else if (item == 'driver-task') {
            $('.table-common.active').hide().removeClass('active')
            $('.driver-task-table-container').show().addClass('active')
            initDriverTaskTable();
            $('.taskId').show();
            $('.customer-button').hide();
            $('.hub-node-btn').hide();

            $('.user-group-params').show();
            $('.vehicleNo').parent().show()
            $('.select-taskPurpose').parent().show()
            $('.taskActivity').parent().hide();
            $('.select-riskLevel').parent().hide()
        } else if (item == 'mtRac') {
            $('.table-common.active').hide().removeClass('active')
            $('.mtRac-table-container').show().addClass('active')
            initMTRacTable();
            $('.select-riskLevel').parent().show()
            $('.taskId').parent().show()
            $('.vehicleNo').parent().show()
            // $('.select-group').parent().show()
            $('.taskId').show();
            $('.select-taskPurpose').parent().hide()
            $('.taskActivity').parent().hide()
            
            $('.customer-button').show();
            if ($('.customer-button input').prop('checked')) {
                $('.hub-node-btn').hide();
            } else {
                $('.hub-node-btn').show();
            }
        } else if (item == 'odd') {
            $('.table-common.active').hide().removeClass('active')
            $('.odd-table-container').show().addClass('active')
            initODDTable();
            $('.select-riskLevel').parent().hide()
            $('.taskId').parent().show()
            $('.vehicleNo').parent().show()
            // $('.select-group').parent().show()
            $('.taskId').show();
            $('.select-taskPurpose').parent().hide()
            $('.taskActivity').parent().hide()
            
            $('.customer-button').show();
            if ($('.customer-button input').prop('checked')) {
                $('.hub-node-btn').hide();
            } else {
                $('.hub-node-btn').show();
            }
        } else if (item == 'survey') {
            $('.table-common.active').hide().removeClass('active')
            $('.survey-table-container').show().addClass('active')
            initSurveyTable();
            $('.select-riskLevel').parent().hide()
            $('.taskId').parent().show()
            $('.vehicleNo').parent().show()
            // $('.select-group').parent().show()
            $('.taskId').show();
            $('.select-taskPurpose').parent().hide()
            $('.taskActivity').parent().hide()
            
            $('.customer-button').show();
            if ($('.customer-button input').prop('checked')) {
                $('.hub-node-btn').hide();
            } else {
                $('.hub-node-btn').show();
            }
        } else if (item == 'cv-loan') {
            $('.table-common.active').hide().removeClass('active')
            $('.cv-table-container').show().addClass('active')
            initCVTable();
            $('.select-riskLevel').parent().hide()
            $('.taskId').parent().show()
            $('.vehicleNo').parent().show()
            $('.select-taskStatus').parent().show()
            // $('.select-group').parent().show()
            $('.taskId').show();
            $('.select-taskPurpose').parent().show()
            $('.taskActivity').parent().show()
            
            $('.customer-button').show();
            if ($('.customer-button input').prop('checked')) {
                $('.hub-node-btn').hide();
            } else {
                $('.hub-node-btn').show();
            }
        } else if (item == 'atms-loan') {
            $('.table-common.active').hide().removeClass('active')
            $('.atms-table-container').show().addClass('active')
            initATMSTable();
            $('.select-riskLevel').parent().hide()
            $('.taskId').parent().show()
            $('.vehicleNo').parent().show()
            $('.select-taskStatus').parent().show()
            // $('.select-group').parent().hide()
            $('.taskId').show();
            $('.select-taskPurpose').parent().show()
            $('.taskActivity').parent().show()

            $('.customer-button').show();
            if ($('.customer-button input').prop('checked')) {
                $('.hub-node-btn').hide();
            } else {
                $('.hub-node-btn').show();
            }
        } else if (item == 'incident') {
            $('.table-common.active').hide().removeClass('active')
            $('.incident-table-container').show().addClass('active')
            initIncidentTable();
            $('.select-riskLevel').parent().hide()
            $('.taskId').parent().hide()
            $('.vehicleNo').parent().hide()
            $('.taskId').show();
            $('.select-taskPurpose').parent().hide()
            $('.select-group').parent().hide()
            $('.taskActivity').parent().hide()
            

            $('.customer-button').show();
            if ($('.customer-button input').prop('checked')) {
                $('.hub-node-btn').hide();
            } else {
                $('.hub-node-btn').show();
            }
        }

    })

    $('#clearAll').on('click', function () {
        $(".select-group").find("option").eq(0).prop("selected",true)
        $(".select-hub").find("option").eq(0).prop("selected",true)
        $('.select-hub').trigger('change')

        $(".select-riskLevel").val("")
        $(".selectedDate").val("")
        $(".driverName").val("")
        $(".vehicleNo").val("")
        $(".taskId").val("")
        $(".select-taskStatus").val("")

        $(".select-taskPurpose").val("")
        $(".taskActivity").val("")
        
        table.ajax.reload(null, true)
    });

    $('.driverName').on('keyup', _.debounce(() => {
        if ($('.driverName').val().length >= 4 || $('.driverName').val().length == 0) {
            table.ajax.reload(null, true)
        }
    }, 500))
    $('.vehicleNo').on('keyup', _.debounce(() => {
        if ($('.vehicleNo').val().length >= 3 || $('.vehicleNo').val().length == 0) {
            table.ajax.reload(null, true)
        }
    }, 500))
    $('.taskId,.taskActivity').on('keyup', _.debounce(() => {
        table.ajax.reload(null, true)
    }, 500))
    
    $('.select-taskStatus,.select-taskPurpose,.select-riskLevel').on('change', () => {
        table.ajax.reload(null, true)
    })

    if (Cookies.get('userType').toLowerCase() != 'customer') {
        let hubNodeList = await getHubNode();
        initUnit(hubNodeList);
    }

    if (['hq', 'customer', 'administrator'].indexOf(Cookies.get('userType').toLowerCase()) > -1) {
        let groupList = await getGroupList();
        initGroup(groupList);
    }
    initPurpose();
}

window.showJustification = function (e, target) {
    let row = table.row($(e).data('row')).data()
    $.alert({
        title: 'Remarks',
        type: 'green',
        content: target ? row[target] : row.cancelledCause
    });
}
window.showActivity = function (e) {
    let row = table.row($(e).data('row')).data()
    $.alert({
        title: 'Activity',
        type: 'green',
        content: row.activity
    });
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
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/dashboard/getTaskList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length

                // console.log(d.order)
                // if (d.order[0].column == 3) {
                //     params.sortBy = 'driverName'
                //     params.sort = d.order[0].dir
                // }
                
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.driverName = $('.driverName').val().trim()
                params.vehicleNo = $('.vehicleNo').val().trim()
                params.taskId = $('.taskId').val()
                params.taskStatus = $('.select-taskStatus').val()
                
                if ($('#select-customer').prop('checked')) {
                    params.hub = null
                    params.node = null
                    if ($('.select-group').val()) {
                        params.group = Number($('.select-group').val())
                    } else {
                        params.group = 0
                    }
                }

                params.purpose = $('.select-taskPurpose').val()
                params.activity = $('.taskActivity').val()
                return params
            },
        },
        columns: [
            // {
            //     "data": "hub",
            //     "title": "Hub",
            //     sortable: false,
            //     render: function (data, type, full, meta) {
            //         if (!data) {
            //             return '-'
            //         } else {
            //             return data
            //         }
            //     }
            // },
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
                    if (!data) {
                        // console.warn(`Driver Name is empty!`)
                        // console.warn(full)
                    }
                    return `${ data ? data : '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
                }
            },
            // {
            //     "data": "vehicleNumber",
            //     "title": "Vehicle Number",
            //     sortable: false,
            // },
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
                    if (!data) return '-'
                    data = data.toString().toLowerCase();
                    if (data == 'waitcheck') data = 'pending';

                    let bgColor = '#CE5018'
                    if (data == 'waitcheck' || data == 'pending') {
                        bgColor = '#1B9063'
                    } else if (data == 'ready') {
                        bgColor = '#1C9600'
                    } else if (data == 'started') {
                        bgColor = '#0d6efd'
                    } else if (data == 'completed') {
                        bgColor = '#afaf0c'
                    } else if (data == 'cancelled') {
                        bgColor = '#5A33DD'
                    }
                    return `  
                        <div style="color: ${ bgColor }; border-radius: 3px; padding: 3px 3px; font-weight: 800;">
                            ${ _.capitalize(data) }
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
                            <span class="d-inline-block text-truncate" style="max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showActivity(this);" role="button" tabindex="0"> 
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
                "data": "cancelledCause",
                "title": "Remarks",
                defaultContent: '-',
                sortable: false,
                render: function (data, type, full, meta) {
                    if (full.cancelledDateTime) {
                        return `
                        <div>
                            <span class="d-inline-block text-truncate" style="max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this);" role="button" tabindex="0">
                                ${ data ? data : '-' }
                            </span><br>
                            <label class="fw-bold">Amended by:</label> <label>${ full.amendedByUsername ?? '-' }</label><br>
                            <label class="fw-bold">Date Time:</label> <label>${ moment(full.cancelledDateTime).format('DD/MM/YYYY HH:mm:ss') }</label>
                        </div>
                        `
                    } else {
                        return '-'
                    }
                }
            },
            {
                "title": "Execution Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label> <label>${ full.indentStartTime ? moment(full.indentStartTime).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.indentEndTime ? moment(full.indentEndTime).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                }
            },
            {
                "title": "Actual Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    let timeHtml = `
                        <label class="fw-bold">Start:</label> <label>${ full.mobileStartTime ? moment(full.mobileStartTime).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.mobileEndTime ? moment(full.mobileEndTime).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                    if (full.withdrawKeyTime) {
                        timeHtml += `<br><label class="fw-bold">&nbsp;&nbsp;Key Wit Time:</label> <label>${ full.withdrawKeyTime ? moment(full.withdrawKeyTime).format('DD/MM/YYYY HH:mm') : '-' }</label>`
                    }
                    if (full.returnKeyTime) {
                        timeHtml += `<br><label class="fw-bold">&nbsp;&nbsp;Key Ret Time:</label> <label>${ full.returnKeyTime ? moment(full.returnKeyTime).format('DD/MM/YYYY HH:mm') : '-' }</label>`
                    }
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
            }
        ],
        // order: [2, 'asc']
    });
}

const initDriverTaskTable = function () {
    table = $('.driver-task-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/dashboard/getDriverMobileTaskList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length

                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.driverName = $('.driverName').val().trim()
                params.vehicleNo = $('.vehicleNo').val().trim()
                params.taskId = $('.taskId').val()
                params.taskStatus = $('.select-taskStatus').val()
                if ($('.select-group').val()) {
                    params.group = Number($('.select-group').val())
                }
                params.purpose = $('.select-taskPurpose').val()
                return params
            },
        },
        columns: [
            {
                "data": "taskId",
                "title": "Task ID",
                sortable: false,
                render: function (data, type, full, meta) {
                    return full.taskId ?? `CU-M-${full.tripId}`
                }
            },
            {
                "data": "driverName",
                "title": "Driver Name",
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) {
                        // console.warn(`Driver Name is empty!`)
                        // console.warn(full)
                    }
                    return `${ data ? data : '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
                }
            },
            {
                "title": "Vehicle No/Resource",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNumber ?? '-' }<br>${ full.vehicleType ?? '-' }`
                }
            },
            {
                "data": "driverStatus",
                "title": "Status",
                sortable: false,
                render: function (data, type, full, meta) {
                    if (full.approveStatus == 'Pending Approval') {
                        data = 'Pending Approval';
                    } else if (full.approveStatus == 'Cancelled') {
                        data = 'Cancelled';
                    }
                    if (!data) return '-'
                    data = data.toString().toLowerCase();
                    if (data == 'waitcheck') data = 'pending';

                    let bgColor = '#CE5018'
                    if (data == 'pending' || data == 'pending approval') {
                        bgColor = '#1B9063'
                    } else if (data == 'ready') {
                        bgColor = '#1C9600'
                    } else if (data == 'started') {
                        bgColor = '#0d6efd'
                    } else if (data == 'completed') {
                        bgColor = '#afaf0c'
                    } else if (data == 'cancelled') {
                        bgColor = '#5A33DD'
                    }
                    return `  
                        <div style="color: ${ bgColor }; border-radius: 3px; padding: 3px 3px; font-weight: bolder;">
                            ${ _.capitalize(data) }
                        </div>
                    `
                }
            },
            {
                "data": "purpose",
                "title": "Purpose",
                defaultContent: '-',
                sortable: false,
            },
            {
                "data": "cancelledCause",
                "title": "Remarks",
                defaultContent: '-',
                sortable: false,
                render: function (data, type, full, meta) {
                    if (full.cancelledDateTime) {
                        return `
                        <div>
                            <span class="d-inline-block text-truncate" style=" max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this);" role="button" tabindex="0">
                                ${ data ? data : '-' }
                            </span><br>
                            <label class="fw-bold">Amended by:</label> <label>${ full.cancelledBy ?? '-' }</label><br>
                            <label class="fw-bold">Date Time:</label> <label>${ moment(full.cancelledDateTime).format('DD/MM/YYYY HH:mm:ss') }</label>
                        </div>
                        `
                    } else {
                        return '-'
                    }
                }
            },
            {
                "title": "Execution Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label> <label>${ full.indentStartTime ? moment(full.indentStartTime).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.indentEndTime ? moment(full.indentEndTime).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                }
            },
            {
                "title": "Actual Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    let timeHtml = `
                        <label class="fw-bold">Start:</label> <label>${ full.mobileStartTime ? moment(full.mobileStartTime).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.mobileEndTime ? moment(full.mobileEndTime).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                    if (full.withdrawKeyTime) {
                        timeHtml += `<br><label class="fw-bold">&nbsp;&nbsp;Key Wit Time:</label> <label>${ full.withdrawKeyTime ? moment(full.withdrawKeyTime).format('DD/MM/YYYY HH:mm') : '-' }</label>`
                    }
                    if (full.returnKeyTime) {
                        timeHtml += `<br><label class="fw-bold">&nbsp;&nbsp;Key Ret Time:</label> <label>${ full.returnKeyTime ? moment(full.returnKeyTime).format('DD/MM/YYYY HH:mm') : '-' }</label>`
                    }
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
                    let actionHtml = '';
                    if (full.approveStatus == 'Pending Approval') {
                        actionHtml += `<button class="btn btn-sm custom-btn-green" onclick="approveDVTask('${full.tripId}')" title="Approve">Approve</button>`
                    }
                    if (full.driverRole == 'DV' && full.approveStatus != 'Cancelled' && !full.mobileStartTime && full.driverStatus != 'Cancelled') {
                        actionHtml += `
                            <button class="btn btn-sm custom-btn-blue" onclick="editDVTask('${full.tripId}', '${full.purpose}', '${full.indentStartTime}', '${full.indentEndTime}', '${full.vehicleNumber}', '${full.pickupDestination}', '${full.dropoffDestination}')" title="Edit">Edit</button>
                            <button class="btn btn-sm custom-btn-gray" onclick="cancelDVTask('${full.tripId}')" title="Cancel">Cancel</button>
                        `
                    }

                    return actionHtml ? actionHtml : '-';
                }
            }
        ],
    });
}

const editDVTask = function(tripId, purpose, startTime, endTime, vehicleNo, reportingLocation, destination) {
    initEditMobileTaskPage(tripId, purpose, startTime, endTime, vehicleNo, reportingLocation, destination);
}

const approveDVTask = function(tripId) {
    $.confirm({
        title: 'Confirm Approve',
        content: 'Are you sure approve to mobile trip:' + tripId,
        buttons: {
            cancel: function () {
                
            },
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    axios.post("/approveMobileTaskById", { tripId }).then(async res => {
                        if (res.respCode != 1) {
                            $.alert({
                                title: 'Error',
                                type: 'red',
                                content: res.respMessage,
                            });
                        } else {
                            table.ajax.reload(null, true);
                        }
                    });
                }
            }
        }
    });
}

const cancelDVTask = function(tripId) {
    $.confirm({
        title: 'Confirm Cancel',
        content: 'Are you sure to cancel mobile trip:' + tripId,
        buttons: {
            cancel: function () {
                
            },
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    axios.post("/cancelMobileTaskById", { tripId }).then(async res => {
                        if (res.respCode != 1) {
                            $.alert({
                                title: 'Error',
                                type: 'red',
                                content: res.respMessage,
                            });
                        } else {
                            table.ajax.reload(null, true);
                        }
                    });
                }
            }
        }
    });
}

window.showMTRacMitigation = function (e) {
    let row = table.row($(e).data('row')).data()
    $.alert({
        title: 'Mitigation',
        type: 'green',
        content: row.mitigation
    });
}
const initMTRacTable = function () {
    // $(".user-hub-params").show();
    // $(".user-node-params").show();
    table = $('.mtRac-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/dashboard/getMT_RACList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length
                
                params.riskLevel = $(".select-riskLevel").val()
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.driverName = $('.driverName').val().trim()
                params.vehicleNo = $('.vehicleNo').val().trim()
                params.taskId = $('.taskId').val().trim()

                if ($('#select-customer').prop('checked')) {
                    params.hub = null
                    params.node = null
                    if ($('.select-group').val()) {
                        params.group = Number($('.select-group').val())
                    } else {
                        params.group = 0
                    }
                }

                return params
            },
        },
        "columns": [
            // {
            //     "data": "hub",
            //     "title": "Hub",
            // },
            {
                // "data": "node",
                "title": "Hub/Node",
                render: function (data, type, full, meta) {
                    return `${ full.hub ?? '-' }<br>${ full.node ?? '-' }`
                }
            },
            // {
            //     "data": "taskId",
            //     "title": "Task ID",
            // },
            {
                "data": "taskId",
                "title": "Task ID",
                render: function (data, type, full, meta) {
                    if(full.dataFrom.toUpperCase() == 'SYSTEM'){
                        if (full.taskId.startsWith('DUTY')) {
                            return full.taskId
                        } else {
                            return `${ full.indentId ?? '-' }<br>${ full.taskId ?? '-' }`
                        }
                    } else {
                        return `${ full.taskId ?? '-' }`
                    }
                }
            },
            {
                "data": "driverName",
                "title": "Driver Name",
            },
            // {
            //     "data": "vehicleNumber",
            //     "title": "Vehicle Number",
            // },
            {
                // "data": "vehicleType",
                "title": "Vehicle No/Resource",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNumber ?? '-' }<br>${ full.vehicleType ?? '-' }`
                }
            },
            {
                "data": "riskLevel",
                "title": "Risk Level",
                "render": function (data, type, full, meta) {
                    let style = ''
                    if (data == 'HIGH') {
                        // style = 'font-weight: bold; color: red;'
                        style = 'alert-danger'
                    } else if (data == 'MEDIUM') {
                        // style = 'font-weight: bold;'
                        style = 'alert-warn'
                    } else {
                        style = 'alert-primary'
                    }
                    // return `<label style="${ style }">${ data }</label>`

                    return `  
                        <div class=${ style } style="padding: 4px 8px; border-radius: 5px;">
                            ${ data }
                        </div>
                    `

                }
            },
            {
                "data": "officer",
                "title": "Officer",
                "render": function (data, type, full, meta) {
                    if (data) {
                        return data + '<br>' + moment(full.officerSignatureDateTime).format('DD/MM/YYYY HH:mm:ss')
                    } else {
                        return '-';
                    }
                }
            },
            {
                "data": "mitigation",
                "title": "Mitigation",
                render: function (data, type, full, meta) {
                    let dataStr = data;
                    if (dataStr && dataStr.length > 50) {
                        dataStr = dataStr.substring(0, 50) + '...';
                        return `<span style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showMTRacMitigation(this)" role="button" tabindex="0">${ dataStr }</span>`
                    } else {
                        return data ? data : '-';
                    }
                    
                }
            },
            {
                "data": "createdAt",
                "title": "Date",
                "render": function (data, type, full, meta) {
                    return moment(data).format('DD/MM/YYYY HH:mm:ss')
                }
            },
            {
                "data": null,
                "title": "View",
                "render": function (data, type, full, meta) {
                    return `
                        <button type="button" class="btn btn-sm custom-btn-green" style="padding: 2px 5px;" data-row="${ meta.row }" onclick="showMTRacDetail(this)">Detail</button>
                    `
                }
            },
        ]
    });
}
const showMTRacDetail = function (e) {
    const generateHtml = function (object) {
        const generateRiskAssessmentHtml = function (riskAssessmentList) {
            let html = ``
            for (let riskAssessment of riskAssessmentList) {
                html += `<label class="fw-bold">${ riskAssessment.riskType }</label> - ${ riskAssessment.assessment }<br>`
            }
            return html
        }
        const generateDriverDeclarationHtml = function (driverDeclarationList) {
            let html = ``
            let index = 1;
            for (let driverDeclaration of driverDeclarationList) {
                html += `<label class="fw-bold">${ index }</label>. ${ driverDeclaration.content }<br>`
                index++;
            }
            return html
        }
        let waitVerifyHtml = '<span style="color: #acacac; font-size: smaller; font-style: italic;">(wait for verify)</span>'
        let html = `
            <div class="container-fluid">
                <div class="row">
                    <div class="col-4 text-end fw-bold">Task ID :</div>
                    <div class="col-8">${ object.taskId }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Risk Level :</div>
                    <div class="col-8">${ object.riskLevel }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Submitted By :</div>
                    <div class="col-8">${ object.submittedBy }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Need Commander :</div>
                    <div class="col-8">${ object.needCommander ? 'True' : 'False' }</div>
                </div>
                ${ object.needCommander ? `
                    <div class="row">
                        <div class="col-4 text-end fw-bold">Commander :</div>
                        <div class="col-8">${ object.commander ? object.commander : waitVerifyHtml }</div>
                    </div>
                    <div class="row">
                        <div class="col-4 text-end fw-bold">Commander Contact Number :</div>
                        <div class="col-8">${ object.commanderContactNumber ? object.commanderContactNumber : waitVerifyHtml }</div>
                    </div>
                    <div class="row">
                        <div class="col-4 text-end fw-bold">Commander Signature DateTime :</div>
                        <div class="col-8">${ object.commanderSignatureDateTime ? moment(object.commanderSignatureDateTime).format('DD/MM/YYYY HH:mm:ss') : waitVerifyHtml }</div>
                    </div>
                ` : '' }
                <div class="row">
                    <div class="col-4 text-end fw-bold">Officer :</div>
                    <div class="col-8">${ object.officer ? object.officer : waitVerifyHtml }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Officer Signature DateTime :</div>
                    <div class="col-8">${ object.officerSignatureDateTime ? moment(object.officerSignatureDateTime).format('DD/MM/YYYY HH:mm:ss') : waitVerifyHtml }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Mitigation :</div>
                    <div class="col-8">${ object.mitigation ? object.mitigation : waitVerifyHtml }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Risk Assessment :</div>
                    <div class="col-8 my-2 py-2" style="line-height: 1.8; background-color: #f1f1f1; border-radius: 5px;">
                        ${ generateRiskAssessmentHtml(object.riskAssessmentList) }
                    </div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Driver Declaration :</div>
                    <div class="col-8 my-2 py-2" style="line-height: 1.8; background-color: #f1f1f1; border-radius: 5px;">
                        ${ generateDriverDeclarationHtml(object.driverDeclarationList) }
                    </div>
                </div>
            </div>
        `;
        
        return html;
    }
    
    let row = table.row($(e).data('row')).data()
    $.confirm({
        title: 'MT RAC Detail',
        boxWidth: '800px',
        useBootstrap: false,
        content: generateHtml(row),
        type: 'green',
        animation: 'top',
        closeAnimation: 'top',
        animationSpeed: 400,
        typeAnimated: true,
        buttons: {
            close: function () {
            }
        }
    });
}

window.showOddDes = function (e) {
    let row = table.row($(e).data('row')).data()
    $.alert({
        title: 'Description',
        type: 'green',
        content: row.content
    });
}

const initODDTable = function () {
    // $(".user-hub-params").show();
    // $(".user-node-params").show();
    let columns = [
        {
            "data": "driverName",
            "title": "Driver Name",
            "width": "15%",
            render: function (data, type, full, meta) {
                return data ?? '-'
            }
        },
        {
            "data": "content",
            "title": "Description",
            "width": "15%",
            render: function (data, type, full, meta) {
                let dataStr = data;
                if (dataStr && dataStr.length > 50) {
                    dataStr = dataStr.substring(0, 50) + '...';
                    return `<span style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showOddDes(this)" role="button" tabindex="0">${ dataStr }</span>`
                } else {
                    return data;
                }
                
            }
        },
        // {
        //     "data": "vehicleNumber",
        //     "title": "Vehicle Number",
        // },
        {
            "data": "vehicleType",
            "title": "Vehicle No/Resource",
            "width": "15%",
            sortable: false,
            render: function (data, type, full, meta) {
                return `${ full.vehicleNumber ?? '-' }<br>${ full.vehicleType ?? '-' }`
            }
        },
        {
            "data": "createdAt",
            "title": "DateTime",
            "width": "15%",
            render: function (data, type, full, meta) {
                return moment(data).format('DD/MM/YYYY HH:mm:ss')
            }
        },
        {
            "data": "oddId",
            "title": "Action",
            "width": "15%",
            render: function (data, type, full, meta) {
                return `<button class="btn btn-sm custom-btn-green" onclick="recfifyOdd('${data}')" title="Rectify">Rectify</button>`
            }
        }
    ];
    table = $('.odd-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/dashboard/getODDList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length
                
                let userNode = Cookies.get('node')
                if (!userNode) {
                    params.hub = $(".select-hub").val()
                    params.node = $(".select-node").val()
                }
                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.driverName = $('.driverName').val().trim()
                params.vehicleNo = $('.vehicleNo').val().trim()
                params.taskId = $('.taskId').val()

                if ($('#select-customer').prop('checked')) {
                    params.hub = null
                    params.node = null
                    if ($('.select-group').val()) {
                        params.group = Number($('.select-group').val())
                    } else {
                        params.group = 0
                    }
                }

                return params
            },
        },
        "columns": columns
    });

    $("#oddRectifyConfirm").off('click').on('click', function() {
        let remarks = $(".odd-remarks-input").val();
        let oddId = $("#oddId").val();

        axios.post("/vehicle/oddRectify", {oddId: oddId, remarks: remarks }).then(async res => {
            if (res.respCode == 1) {
                $("#oddId").val('');
                $(".odd-remarks-input").val('');
                $("#odd-rectify").modal('hide');
            } else {
                $.alert({
                    title: 'Error',
                    type: 'red',
                    content: res.respMessage,
                });
            }

            table.ajax.reload(null, true);
        });

    });
    $("#oddRectifyCancel").off('click').on('click', function() {
        $("#oddId").val('');
        $(".odd-remarks-input").val('');
        $("#odd-rectify").modal('hide');
    });
}

const alertDescription = function(title, content) {
    $.alert({
        title: title,
        type: 'green',
        content: content,
    });
}

const recfifyOdd = function(oddId) {
    $("#oddId").val(oddId);
    $("#odd-rectify").modal('show');
}

window.showSurveyRemark = function (e) {
    let row = table.row($(e).data('row')).data()
    $.alert({
        title: 'Remark',
        type: 'green',
        content: row.remark
    });
}

const initSurveyTable = function () {
    // $(".user-hub-params").show();
    // $(".user-node-params").show();
    table = $('.survey-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/dashboard/getSurveyList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length
                
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.taskId = $('.taskId').val()
                params.driverName = $('.driverName').val().trim()
                params.vehicleNo = $('.vehicleNo').val().trim()

                if ($('#select-customer').prop('checked')) {
                    params.hub = null
                    params.node = null
                    if ($('.select-group').val()) {
                        params.group = Number($('.select-group').val())
                    } else {
                        params.group = 0
                    }
                }

                return params
            },
        },
        "columns": [
            // {
            //     "data": "hub",
            //     "title": "Hub",
            // },
            {
                // "data": "node",
                "title": "Hub/Node",
                "width": "10%",
                render: function (data, type, full, meta) {
                    return `${ full.hub ?? '-' }<br>${ full.node ?? '-' }`
                }
            },
            {
                "data": "taskId",
                "title": "Task ID",
                "width": "10%",
            },
            {
                "data": "driverName",
                "title": "Driver Name",
                "width": "10%",
            },
            // {
            //     "data": "vehicleNumber",
            //     "title": "Vehicle Number",
            // },
            {
                // "data": "vehicleType",
                // "title": "Vehicle Type",
                "title": "Vehicle No/Resource",
                sortable: false,
                "width": "10%",
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNumber ?? '-' }<br>${ full.vehicleType ?? '-' }`
                }
            },
            {
                "data": "starVal",
                "title": "Star",
                "width": "10%",
            },
            // {
            //     "data": "question",
            //     "title": "Question",
            // },
            // {
            //     "data": "options",
            //     "title": "Options",
            // },
            {
                "data": "remark",
                "title": "Remarks",
                "width": "30%",
                render:  function (data, type, full, meta) {
                    let dataStr = data;
                    if (dataStr && dataStr.length > 50) {
                        dataStr = dataStr.substring(0, 50) + '...';
                        return `<span style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showSurveyRemark(this)" role="button" tabindex="0">${dataStr}</span>`
                    } else {
                        return data;
                    }
                    
                }
            },
            {
                "data": "createdAt",
                "title": "Date",
                "width": "10%",
                render: function (data) {
                    return moment(data).format('DD/MM/YYYY HH:mm:ss')
                }
            },
            {
                "data": null,
                "title": "View",
                "width": "10%",
                "render": function (data, type, full, meta) {
                    return `
                        <button type="button" class="btn btn-sm custom-btn-green" data-row="${ meta.row }" style="padding: 2px 5px;" onclick="showSurveyDetail(this)">Detail</button>
                    `
                }
            },
        ]
    });
}
const showSurveyDetail = function (e) {
    const generateOptions = function (list) {
        let html = ``
        for (let option of list) {
            html += `
                <div class="row">
                    <div class="col-8">
                        ${ option.option }
                    </div>
                    <div class="col-4 text-end">
                        <input type="checkbox" ${ option.checked ? 'checked' : '' }/>
                    </div>
                </div>
            `
        }
        return html;
    }
    const generateHtml = function (object) {
        let html = `
            <div class="container-fluid">
                <div class="row">
                    <div class="col-12 fw-bold">${ object.question }</div>
                </div>
                ${ generateOptions(JSON.parse(object.options)) }
                <div class="row mt-2">
                    <div class="col-12">
                        <textarea class="px-2" style="width: 100%; border-color: #d5d5d5; border-radius: 3px;">${ object.remark }</textarea>
                    </div>
                </div>
            </div>
        `;
        return html;
    }
    
    let row = table.row($(e).data('row')).data()
    $.confirm({
        title: 'Survey Detail',
        boxWidth: '500px',
        useBootstrap: false,
        content: generateHtml(row),
        type: 'blue',
        buttons: {
            close: function () {
            }
        },
        onOpen: function () {
            $(".survey-table-container input[type='checkbox']").on('click', function () { 
                this.checked = !this.checked;
                $.alert({
                    title: 'Info',
                    type: 'green',
                    content: 'Can not change here!',
                });
            });
        }
    });
}

window.showIncidentDes = function (e) {
    let row = table.row($(e).data('row')).data()
    $.alert({
        title: 'Description',
        type: 'green',
        content: row.description
    });
}
const initIncidentTable = function () {
    // $(".user-hub-params").show();
    // $(".user-node-params").show();
    table = $('.incident-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/dashboard/getIncidentList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length
                
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.driverName = $('.driverName').val()
                params.taskId = $('.taskId').val()
                return params
            },
        },
        "columns": [
            // {
            //     "data": "hub",
            //     "title": "Hub",
            // },
            {
                // "data": "node",
                "title": "Hub/Node",
                render: function (data, type, full, meta) {
                    return `${ full.hub }<br>${ full.node ?? '-' }`
                }
            },
            {
                "data": "driverName",
                "title": "Submitted By",
            },
            {
                "data": "incidentName",
                "title": "Incident Name",
            },
            {
                "data": "incidentType",
                "title": "Incident Type",
            },
            {
                "data": "description",
                "title": "Description",
                render: function (data, type, full, meta) {
                    let dataStr = data;
                    if (dataStr && dataStr.length > 50) {
                        dataStr = dataStr.substring(0, 50) + '...';
                        return `<span style="border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showIncidentDes(this)" role="button" tabindex="0">${dataStr}</span>`
                    } else {
                        return data;
                    }
                    
                }
            },
            {
                "data": "occTime",
                "title": "OCC Time",
                render: function (data) {
                    return moment(data).format('DD/MM/YYYY HH:mm:ss')
                }
            },
            {
                "data": "createdAt",
                "title": "Create Date",
                render: function (data) {
                    return moment(data).format('DD/MM/YYYY HH:mm:ss')
                }
            }
        ]
    });
}

const initCVTable = function () {
    // $(".user-hub-params").show();
    // $(".user-node-params").show();
    table = $('.cv-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/dashboard/getCVLoanTaskList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length
                
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.driverName = $('.driverName').val().trim()
                params.vehicleNo = $('.vehicleNo').val().trim()
                params.taskId = $('.taskId').val()
                params.taskStatus = $('.select-taskStatus').val()

                params.purpose = $('.select-taskPurpose').val()
                params.activity = $('.taskActivity').val()

                if ($('#select-customer').prop('checked')) {
                    params.hub = null
                    params.node = null
                    if ($('.select-group').val()) {
                        params.group = Number($('.select-group').val())
                    } else {
                        params.group = 0
                    }
                }

                return params
            },
        },
        columns: [
            // {
            //     "title": "Group",
            //     sortable: false,
            //     render: function (data, type, full, meta) {
            //         return `${ full.groupName ?? '-' }`
            //     }
            // },
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
                    return `${ full.indentId ?? '-' }<br>${ full.taskId ?? '-' }`
                }
            },
            {
                "data": "groupName",
                "title": "Group",
                sortable: false,
                render: function (data, type, full, meta) {
                    return data ? data : '-'
                }
            },
            {
                "data": "driverName",
                "title": "Driver Name",
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) {
                        // console.warn(`Driver Name is empty!`)
                        // console.warn(full)
                    }
                    return `${ data ? data : '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
                }
            },
            {
                "title": "Vehicle No/Resource",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNo ?? '-' }<br>${ full.vehicleType ?? '-' }`
                }
            },
            {
                "data": "status",
                "title": "Status",
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) return '-'
                    data = data.toString().toLowerCase();
                    if (data == 'waitcheck') data = 'pending';
                    if (data == 'returned') data = 'completed';

                    let bgColor = '#CE5018'
                    if (data == 'waitcheck' || data == 'pending') {
                        bgColor = '#1B9063'
                    } else if (data == 'ready') {
                        bgColor = '#1C9600'
                    } else if (data == 'started') {
                        bgColor = '#0d6efd'
                    } else if (data == 'completed') {
                        bgColor = '#afaf0c'
                    } else if (data == 'cancelled') {
                        bgColor = '#5A33DD'
                    }
                    
                    return `  
                        <div style="color: ${ bgColor }; border-radius: 3px; padding: 3px 3px; font-weight: bolder;">
                            ${ _.capitalize(data) }
                        </div>
                    `
                }
            },
            {
                "data": "activityName",
                "title": "Activity",
                defaultContent: '-',
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) return '-'
                    if (data.length < 20) {
                        return data
                    } else {
                        return `
                            <span class="d-inline-block text-truncate" style=" max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showActivity(this);" role="button" tabindex="0">
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
                "title": "Remarks",
                defaultContent: '-',
                sortable: false,
                render: function (data, type, full, meta) {
                    if (full.returnDate) {
                        return `
                            <div>
                                <span class="d-inline-block text-truncate" style=" max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this, 'returnRemark');" role="button" tabindex="0">
                                    ${ full.returnRemark ?? '-' }
                                </span><br>
                                <label class="fw-bold">Updated By:</label> <label>${ full.returnUserName ? full.returnUserName : 'System' }</label><br>
                                <label class="fw-bold">Date Time:</label> <label>${ moment(full.returnDate).format('DD/MM/YYYY HH:mm:ss') }</label>
                            </div>
                        `
                    } else if (full.cancelledDateTime) {
                        return `
                            <div>
                                <span class="d-inline-block text-truncate" style=" max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this, 'cancelledCause');" role="button" tabindex="0">
                                    ${ full.cancelledCause ?? '-' }
                                </span><br>
                                <label class="fw-bold">Amended by:</label> <label>${ full.amendedByUsername ?? '-' }</label><br>
                                <label class="fw-bold">Date Time:</label> <label>${ moment(full.cancelledDateTime).format('DD/MM/YYYY HH:mm:ss') }</label>
                            </div>
                        `
                    } else {
                        return '-'
                    }
                }
            },
            {
                "title": "Execution Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label> <label>${ full.startDate ? moment(full.startDate).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.endDate ? moment(full.endDate).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                }
            },
            {
                "title": "Actual Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label> <label>${ full.actualStartTime ? moment(full.actualStartTime).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.actualEndTime ? moment(full.actualEndTime).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                }
            },
            {
                title: "Location",
                sortable: false ,
                defaultContent: '' ,
                render: function (data, type, full, meta) {
                    return `<div>
                        <div class="color-pickup-destination">${ full.pickupDestination ?? '-' }</div>
                        <div class="icon-down-div"><span class="iconfont icon-down"></span></div>
                        <div class="color-dropoff-destination">${ full.dropoffDestination ?? '-' }</div>
                    </div>`
                }
            },
            {
                "data": "status",
                "title": "Action",
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) return '-'
                    let operationList = full.operation.split(',')
                    data = data.toString().toLowerCase();

                    let startBtn = `<button type="button" class="btn btn-sm status-btn custom-btn-blue mx-1" onclick="startLoan(${ full.loanId ?? 0 })">Start</button>`
                    let completeBtn = `<button type="button" class="btn btn-sm status-btn custom-btn-yellow mx-1" onclick="completeLoan(${ full.loanId ?? 0 }, '${ full.actualStartTime }')">Complete</button>`
                    let returnBtn = `<button type="button" class="btn btn-sm status-btn custom-btn-green mx-1" onclick="returnLoan(${ full.loanId ?? 0 })">Return</button>`
                    let cancelBtn = `<button type="button" class="btn btn-sm status-btn custom-btn-gray mx-1" onclick="cancelLoan(${ full.loanId ?? 0 })">Cancel</button>`

                    // if (data == 'pending') {
                    //     return startBtn
                    // } else if (data == 'started') {
                    //     return completeBtn
                    // } else if (data == 'completed') {
                    //     return returnBtn
                    // } else {
                    //     return  _.capitalize(data)
                    // }

                    if (data == 'pending') {
                        if (operationList.includes('Start')) {
                            return startBtn
                        } else {
                            return  ''
                        }
                    } else if (data == 'started') {
                        if (operationList.includes('Complete')) {
                            return completeBtn
                        } else {
                            return ''
                        }
                    } else if (data == 'completed') {
                        if (operationList.includes('Return')) {
                            return returnBtn
                        } else {
                            return ''
                        }
                    } else {
                        return ''
                    }
                }
            },
            
        ],
        // order: [2, 'asc']
    });
}

const initATMSTable = function () {
    // $(".user-hub-params").show();
    // $(".user-node-params").show();
    table = $('.atms-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/dashboard/getATMSLoanTaskList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length
                
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.driverName = $('.driverName').val().trim()
                params.vehicleNo = $('.vehicleNo').val().trim()
                params.taskId = $('.taskId').val()
                params.taskStatus = $('.select-taskStatus').val()

                params.purpose = $('.select-taskPurpose').val()
                params.activity = $('.taskActivity').val()

                if ($('#select-customer').prop('checked')) {
                    params.hub = null
                    params.node = null
                    if ($('.select-group').val()) {
                        params.group = Number($('.select-group').val())
                    } else {
                        params.group = 0
                    }
                }

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
                "data": "id",
                "title": "Task ID",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ full.indentId ?? '-' }<br>${ full.taskId ?? '-' }`
                }
            },
            {
                "data": "groupName",
                "title": "Group",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ data ?? '-' }`
                }
            },
            {
                "data": "driverName",
                "title": "Driver Name",
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) {
                        // console.warn(`Driver Name is empty!`)
                        // console.warn(full)
                    }
                    return `${ data ?? '-' } <br/> (${ full.contactNumber ?? '-' })`
                }
            },
            {
                "title": "Vehicle No/Resource",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNo ? full.vehicleNo : '-' }<br>${ full.vehicleType ?? '-' }`
                }
            },
            {
                "data": "status",
                "title": "Status",
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) return '-'
                    data = data.toString().toLowerCase();
                    if (data == 'waitcheck') data = 'pending';
                    if (data == 'returned') data = 'completed';

                    let bgColor = '#CE5018'
                    if (data == 'waitcheck' || data == 'pending') {
                        bgColor = '#1B9063'
                    } else if (data == 'ready') {
                        bgColor = '#1C9600'
                    } else if (data == 'started') {
                        bgColor = '#0d6efd'
                    } else if (data == 'completed') {
                        bgColor = '#afaf0c'
                    } else if (data == 'cancelled') {
                        bgColor = '#5A33DD'
                    }
                    return `  
                        <div style="color: ${ bgColor }; border-radius: 3px; padding: 3px 3px; font-weight: bolder;">
                            ${ _.capitalize(data) }
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
                            <span class="d-inline-block text-truncate" style=" max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showActivity(this);" role="button" tabindex="0">
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
                "title": "Remarks",
                defaultContent: '-',
                sortable: false,
                render: function (data, type, full, meta) {
                     if (full.cancelledDateTime) {
                        return `
                            <div>
                                <span class="d-inline-block text-truncate" style=" max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this, 'cancelledCause');" role="button" tabindex="0">
                                    ${ full.cancelledCause ?? '-' }
                                </span><br>
                                <label class="fw-bold">Amended by:</label> <label>${ full.amendedByUsername ?? '-' }</label><br>
                                <label class="fw-bold">Date Time:</label> <label>${ moment(full.cancelledDateTime).format('DD/MM/YYYY HH:mm:ss') }</label>
                            </div>
                        `
                    } else if (full.returnDate) {
                        return `
                            <div>
                                <span class="d-inline-block text-truncate" style=" max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this, 'returnRemark');" role="button" tabindex="0">
                                    ${ full.returnRemark ?? '-' }
                                </span><br>
                                <label class="fw-bold">Updated By:</label> <label>${ full.returnUserName ? full.returnUserName : 'System' }</label><br>
                                <label class="fw-bold">Date Time:</label> <label>${ moment(full.returnDate).format('DD/MM/YYYY HH:mm:ss') }</label>
                            </div>
                        `
                    } else {
                        return '-'
                    }
                }
            },
            {
                "title": "Execution Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label> <label>${ full.startDate ? moment(full.startDate).format('DD/MM/YYYY HH:mm') : '-' }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ full.endDate ? moment(full.endDate).format('DD/MM/YYYY HH:mm') : '-' }</label>
                    `
                }
            },
            {
                "title": "Actual Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    let startTime = '-', endTime = '-'
                    if (full.status == 'Cancelled') {
                        startTime = '-'
                        endTime = '-'
                    } else {
                        if (full.actualStartTime) {
                            startTime = moment(full.actualStartTime).format('DD/MM/YYYY HH:mm')
                        }
                        if (full.actualEndTime) {
                            endTime = moment(full.actualEndTime).format('DD/MM/YYYY HH:mm')
                        }
                    }
                    return `
                        <label class="fw-bold">Start:</label> <label>${ startTime }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ endTime }</label>
                    `
                }
            },
            {
                title: "Location",
                sortable: false ,
                defaultContent: '' ,
                render: function (data, type, full, meta) {
                    return `<div>
                        <div class="color-pickup-destination">${ full.reportingLocation ?? '-' }</div>
                        <div class="icon-down-div"><span class="iconfont icon-down"></span></div>
                        <div class="color-dropoff-destination">${ full.destination ?? '-' }</div>
                    </div>`
                }
            },
            {
                "data": "status",
                "title": "Action",
                sortable: false,
                render: function (data, type, full, meta) {
                    if (!data) return '-'
                    let operationList = full.operation.split(',')
                    data = data.toString().toLowerCase();

                    let startBtn = `<button type="button" class="btn btn-sm status-btn  custom-btn-blue mx-1" onclick="startLoan(${ full.loanId ?? 0 })">Start</button>`
                    let completeBtn = `<button type="button" class="btn btn-sm status-btn custom-btn-yellow mx-1" onclick="completeLoan(${ full.loanId ?? 0 }, '${ full.actualStartTime }')">Complete</button>`
                    let returnBtn = `<button type="button" class="btn btn-sm status-btn custom-btn-green mx-1" onclick="returnLoan(${ full.loanId ?? 0 })">Return</button>`
                    let cancelBtn = `<button type="button" class="btn btn-sm status-btn custom-btn-gray mx-1" onclick="cancelLoan(${ full.loanId ?? 0 })">Cancel</button>`

                    if (data == 'pending') {
                        if (operationList.includes('Start')) {
                            return startBtn
                        } else {
                            return ''
                        }
                    } else if (data == 'started') {
                        if (operationList.includes('Complete')) {
                            return completeBtn
                        } else {
                            return ''
                        }
                    } else if (data == 'completed') {
                        if (operationList.includes('Return')) {
                            return returnBtn
                        } else {
                            return ''
                        }
                    } else {
                        return ''
                    }
                }
            },
        ],
        // order: [2, 'asc']
    });
}

window.startLoan = function (loanId) {
    $.confirm({
        title: 'Start Loan Task',
        content: `<div class="row mx-0 px-0">
            <div class="col-5 px-0">Actual Start Date Time:</div>
            <div class="col-7 px-0"><input class="form-control form-control-sm actualStartTime"/></div>
        </div>`,
        onContentReady: function () {
            layui.use('laydate', function(){
                let laydate = layui.laydate;
                laydate.render({
                    elem: '.actualStartTime',
                    type: 'datetime',
                    lang: 'en',
                    trigger: 'click',
                    value: moment().format('YYYY-MM-DD HH:mm:ss'),
                    btns: ['clear', 'confirm'],
                });
            })
        },
        buttons: {
            cancel: function () {

            },
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    if (this.$content.find('.actualStartTime').val()) {
                        axios.post('/dashboard/startLoan', {
                            loanId,
                            actualStartTime: this.$content.find('.actualStartTime').val()
                        }).then(result => {
                            if (result.respCode == 1) {
                                table.ajax.reload(null, true)
                            } else {
                                $.alert({
                                    title: 'Warn',
                                    type: 'green',
                                    content: result.respMessage
                                });
                            }
                        })
                    } else {
                        $.alert({
                            title: 'Warn',
                            type: 'green',
                            content: 'Start time is needed.'
                        });
                        return false
                    }
                    
                }
            }
        }
    });
    
}
window.completeLoan = function (loanId, startTime) {
    $.confirm({
        title: 'Complete Loan Task',
        content: `<div class="row mx-0 px-0">
            <div class="col-6 px-0">Actual End Date Time:</div>
            <div class="col-6 px-0"><input class="form-control form-control-sm actualEndTime"/></div>
        </div>`,
        onContentReady: function () {
            layui.use('laydate', function(){
                let laydate = layui.laydate;
                laydate.render({
                    elem: '.actualEndTime',
                    type: 'datetime',
                    lang: 'en',
                    trigger: 'click',
                    btns: ['clear', 'confirm'],
                    value: moment().format('YYYY-MM-DD HH:mm:ss'),
                    done: function (value) {
                        if (moment(startTime).isSameOrAfter(moment(value))) {
                            $.alert({
                                title: 'Warn!',
                                content: 'EndTime is greater than StartTime.',
                            });
                            $('.actualEndTime').val(null)
                        }
                    }
                });
            })
        },
        buttons: {
            cancel: function () {

            },
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    if (this.$content.find('.actualEndTime').val()) {
                        axios.post('/dashboard/completeLoan', {
                            loanId,
                            actualEndTime: this.$content.find('.actualEndTime').val()
                        }).then(result => {
                            if (result.respCode == 1) {
                                table.ajax.reload(null, true)
                            } else {
                                $.alert({
                                    title: 'Warn',
                                    type: 'green',
                                    content: result.respMessage
                                });
                            }
                        })
                    } else {
                        $.alert({
                            title: 'Warn',
                            type: 'green',
                            content: 'End time is needed.'
                        });
                        return false
                    }
                    
                }
            }
        }
    });
}
window.returnLoan = function (loanId) {
    $.confirm({
        title: 'Return Loan',
        content: `<div class="row mx-0 px-0">
            <div class="col-3 pe-0">Remark</div>
            <div class="col-9 ps-0"><textarea class="form-control form-control-sm returnRemark"></textarea></div>
        </div>`,
        buttons: {
            cancel: function () {

            },
            confirm: {
                btnClass: 'btn-green',
                action: function () {
                    axios.post('/loanOut/returnLoanByLoanId', {
                        loanId,
                        returnRemark: this.$content.find('.returnRemark').val()
                    }).then(result => {
                        if (result.respCode == 1) {
                            table.ajax.reload(null, true)
                        } else {
                            $.alert({
                                title: 'Warn',
                                type: 'green',
                                content: result.respMessage
                            });
                        }
                    })
                }
            }
        }
    });
}


const initUrgentTaskTable = function () {
    const columns = [
        // {
        //     "data": "hub",
        //     "title": "Hub",
        //     sortable: false,
        //     render: function (data, type, full, meta) {
        //         if (!data) {
        //             return '-'
        //         } else {
        //             return data
        //         }
        //     }
        // },
        {
            "title": "Hub/Node",
            sortable: false,
            render: function (data, type, full, meta) {
                return `${ full.hub ?? '-' }<br>${ full.node ?? '-' }`
            }
        },
        {
            "data": "id",
            "title": "Indent ID",
            sortable: false,
            render: function (data, type, full, meta) {
                if(full.dataFrom.toUpperCase() == 'SYSTEM'){
                    return `${ full.requestId ?? '-' }<br>${ full.indentId ?? '-' }`
                } else {
                    return `${ full.indentId ?? '-' }`
                }
            }
        },
        {
            "title": "Duty ID",
            "data": "dutyId",
            sortable: false,
            render: function (data, type, full, meta) {
                return full.configId
            }
        },
        {
            "data": "group",
            "title": "Group",
            sortable: false,
            render: function (data, type, full, meta) {
                return full.group ? full.group : '-'
            }
        },
        {
            "data": "status",
            "title": "Status",
            sortable: false,
            render: function (data, type, full, meta) {
                if (!data) return '-'
                    data = data.toString().toLowerCase();
                    if (data == 'waitcheck') data = 'pending';

                    let bgColor = '#CE5018'
                    if (data == 'waitcheck' || data == 'pending') {
                        bgColor = '#1B9063'
                    } else if (data == 'ready') {
                        bgColor = '#1C9600'
                    } else if (data == 'started') {
                        bgColor = '#0d6efd'
                    } else if (data == 'completed') {
                        bgColor = '#afaf0c'
                    } else if (data == 'cancelled') {
                        bgColor = '#5A33DD'
                    }
                    return `  
                        <div style="color: ${ bgColor }; border-radius: 3px; padding: 3px 3px; font-weight: bolder;">
                            ${ _.capitalize(data) }
                        </div>
                    `
            }
        },
        {
            "data": "driverName",
            "title": "Driver Assigned",
            sortable: false,
            render: function (data, type, full, meta) {
                return `${ data ? data : '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
            }
        },
        {
            "title": "Vehicle No/Resource",
            sortable: false,
            render: function (data, type, full, meta) {
                return `${ full.vehicleNo ?? '-'  }<br>${ full.vehicleType ?? '-' }`
            }
        },
        {
            "title": "Date",
            sortable: false,
            render: function (data, type, full, meta) {
                return full.startTime ? moment(full.startTime).format('DD/MM/YYYY') : '-'
            }
        },
        {
            "title": "Execution Time",
            sortable: false,
            render: function (data, type, full, meta) {
                // return `<div class="fw-bold text-light bg-success rounded-pill py-1 px-1"></div>`
                let baseHtml = `${ moment(full.startTime).format('HHmm') }H - ${ moment(full.endTime).format('HHmm') }H`
                baseHtml += `<br>`
                baseHtml += `<label style="color: #1B9063; margin-top: 10px;">${ full.mobileStartTime ? moment(full.mobileStartTime).format('HHmm') + 'H' : '' } - ${ full.mobileEndTime ? moment(full.mobileEndTime).format('HHmm') + 'H' : '' }</label>`
                
                return baseHtml
            }
        },
        {
            title: "Location",
            sortable: false ,
            defaultContent: '' ,
            render: function (data, type, full, meta) {
                return `<div>
                    <div class="color-pickup-destination">${ full.reportingLocation }</div>
                    <div class="icon-down-div"><span class="iconfont icon-down"></span></div>
                    <div class="color-dropoff-destination">${ full.reportingLocation }</div>
                </div>`
            }
        },
        {
            title: "POC",
            sortable: false ,
            defaultContent: '' ,
            render: function (data, type, full, meta) {
                return `${ full.poc }<br>${ full.mobileNumber }`
            }
        },
        {
            "data": "cancelledCause",
            "title": "Remarks",
            defaultContent: '-',
            sortable: false,
            render: function (data, type, full, meta) {
                if (full.cancelledDateTime) {
                    return `
                    <div>
                        <span class="d-inline-block text-truncate" style=" max-width: 160px; border-bottom: 1px solid gray; cursor: pointer;" data-row="${ meta.row }" onclick="showJustification(this);" role="button" tabindex="0">
                            ${ data ? data : '-' }
                        </span><br>
                        <label class="fw-bold">Amended by:</label> <label>${ full.amendedByUsername ?? '-' }</label><br>
                        <label class="fw-bold">Date Time:</label> <label>${ moment(full.cancelledDateTime).format('DD/MM/YYYY HH:mm:ss') }</label>
                    </div>
                    `
                } else {
                    return '-'
                }
            }
        },
        {
            "title": "Cancelled By",
            "data": "cancelBy",
            sortable: false,
            render: function (data, type, full, meta) {
                return data ?? '-'
            }
        },
        {
            "data": "requestId",
            "title": "Action",
            "width": "15%",
            render: function (data, type, full, meta) {
                let html = ``

                // If after endTime, can do nothing
                if (moment().isBefore(moment(full.endTime))) {
                    if (full.operation.includes('Cancel')) {
                        if (![ 'started', 'completed', 'cancelled' ].includes(full.status.toLowerCase())) {
                            html += `<button type="button" class="btn btn-sm ms-1 custom-btn-gray" onclick="cancelIndent('${ data }', '${ full.id }')" title="Cancel">Cancel</button>`
                            // if (moment(full.startTime).diff(moment()) >= 3600 * 1000) {
                            // } else {
                            //     html += `
                            //     <button type="button" class="btn btn-sm ms-1 custom-btn-gray" style="cursor: not-allowed;" title="Cancel should < 1hr before start time.">Cancel</button>`
                            // }
                        }
                    } 
                    if (full.operation.includes('Re-Assign')) {
                        // while cancelBy has value, can not reassign
                        if (![ 'completed', 'cancelled' ].includes(full.status.toLowerCase()) && !full.cancelBy) {
                            html += `<button type="button" class="btn btn-sm ms-1 custom-btn-blue" onclick="reAssignIndent('${ data }', '${ full.id }', '${ full.dutyId }')" title="Re-Assign">Re-Assign</button>`
                        }
                    }
                }
                
                return html
            }
        }]
    table = $('.urgent-task-table').on('order.dt', function () {
    }).on('page.dt', function () {
    }).DataTable({
        "ordering": false,
        "searching": false,
        "paging": true,
        "pageLength": 10,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "scrollX": "auto",
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": PageHelper.dom(),
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/urgent/getIndentList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length

                // console.log(d.order)
                // if (d.order[0].column == 3) {
                //     params.sortBy = 'driverName'
                //     params.sort = d.order[0].dir
                // }
                
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.selectedDate = $('.selectedDate').val()
                params.selectedDate = params.selectedDate ? moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : ''
                params.driverName = $('.driverName').val().trim()
                params.vehicleNo = $('.vehicleNo').val().trim()
                params.taskId = $('.taskId').val()
                params.taskStatus = $('.select-taskStatus').val()
                
                if ($('#select-customer').prop('checked')) {
                    params.hub = null
                    params.node = null
                    if ($('.select-group').val()) {
                        params.group = Number($('.select-group').val())
                    } else {
                        params.group = 0
                    }
                }

                return params
            },
        },
        columns,
        // order: [2, 'asc']
    });
}

const initUrgentDutyTable = function () {
    table = $('.urgent-duty-table').DataTable({
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
                let endDateOrder
                let idDateOrder
                for (let orderField of d.order) {
                    if(orderField.column == 7) {
                        endDateOrder = orderField.dir;
                    }
                    if(orderField.column == 0){
                        idDateOrder = orderField.dir;
                    }
                }    

                let option = { 
                    'resource': null,
                    'createDate': null,
                    'hub': $(".select-hub").val(),
                    'node': $(".select-node").val(),
                    "groupId": null,
                    "taskStatus": $('.select-taskStatus').val(),
                    "vehicleNo": $('.vehicleNo').val().trim() ?? null,
                    "driverName":  $('.driverName').val().trim() ?? null,
                    "endDateOrder": endDateOrder,
                    "idDateOrder": idDateOrder,
                    "selectedDate": $('.selectedDate').val() ? moment($('.selectedDate').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : '', 
                    "pageNum": d.start, 
                    "pageLength": d.length
                }
                return option
            }
        },   
        columns: [
            {
                title: 'ID', 
                data: 'id', 
                sortable: true,
                defaultContent: '-' 
            },
            { 
                data: 'hub', 
                title: "Hub/Node",
                sortable: false,
                defaultContent: '-' ,
                render: function (data, type, full, meta) {
                    return `${ data ?? '-' }<br/>${ full.node ?? '-' }`
                }
            },
            { 
                title: 'Driver Name', 
                data: 'driverName', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    return `${ data ?? '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
                } 
            },
            { 
                title: 'Vehicle No/Resource', 
                data: 'vehicleNo', 
                sortable: false ,
                defaultContent: '-',
                render: function (data, type, full, meta) {
                    return `${ full.vehicleNo ?? '-' }<br>${ full.vehicleType ?? '-' }`
                } 
            },
            {
                title: 'Status',
                data: 'status',
                sortable: false ,
                defaultContent: '-',
                render: function(data) {
                    if (!data) return '-'
                    data = data.toString().toLowerCase();
                    if (data == 'waitcheck') data = 'pending';

                    let bgColor = '#CE5018'
                    if (data == 'waitcheck' || data == 'pending') {
                        bgColor = '#1B9063'
                    } else if (data == 'ready') {
                        bgColor = '#1C9600'
                    } else if (data == 'started') {
                        bgColor = '#0d6efd'
                    } else if (data == 'completed') {
                        bgColor = '#afaf0c'
                    } else if (data == 'cancelled') {
                        bgColor = '#5A33DD'
                    }
                    return `  
                        <div style="color: ${ bgColor }; border-radius: 3px; padding: 3px 3px; font-weight: bolder;">
                            ${ _.capitalize(data) }
                        </div>
                    `
                }
            },
            { 
                title: 'Purpose', 
                data: 'purpose', 
                sortable: false,
                defaultContent: '-' 
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
                title: 'Execution Time', 
                sortable: true ,
                defaultContent: '-'  ,
                render: function (data, type, full, meta) {
                    return `
                        <label class="fw-bold">Start:</label><label>${ moment(full.indentStartDate).format('DD/MM/YYYY') } ${ moment('2023-10-26 '+full.startTime).format('HH:mm') }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label><label>${ moment(full.indentEndDate).format('DD/MM/YYYY') } ${ moment('2023-10-26 '+full.endTime).format('HH:mm') }</label>
                    `
                }
            },
            {
                "title": "Actual Time",
                sortable: false,
                render: function (data, type, full, meta) {
                    let startTime = '-', endTime = '-'
                    if (full.status == 'Cancelled') {
                        startTime = '-'
                        endTime = '-'
                    } else {
                        if (full.mobileStartTime) {
                            startTime = moment(full.mobileStartTime).format('DD/MM/YYYY HH:mm')
                        }
                        if (full.mobileEndTime) {
                            endTime = moment(full.mobileEndTime).format('DD/MM/YYYY HH:mm')
                        }
                    }
                    return `
                        <label class="fw-bold">Start:</label> <label>${ startTime }</label><br>
                        <label class="fw-bold">&nbsp;&nbsp;End:</label> <label>${ endTime }</label>
                    `
                }
            },
        ],
    });
}

const cancelIndent = async function (requestId, id) {
    $.alert({
        title: null,
        boxWidth: '600px',
        content: `Are you sure to cancel Urgent Indent ${ requestId } ? 
        <br>
        <div class="row mt-1 cancelBy" style="width: 90%; margin-left: 5px;">
            <div class="col-6">
                <div class="form-check">
                    <input class="form-check-input" type="radio" name="flexRadioDefault" value="Cancel by Unit" id="flexRadioDefault1" style="margin-top: 8px;" checked>
                    <label class="form-check-label" for="flexRadioDefault1">
                        Cancel by Unit
                    </label>
                </div>
            </div>
            <div class="col-6">
                <div class="form-check">
                    <input class="form-check-input" type="radio" name="flexRadioDefault" value="Others" id="flexRadioDefault2" style="margin-top: 8px;">
                    <label class="form-check-label" for="flexRadioDefault2">
                        Others
                    </label>
                </div>
            </div>
        </div>
        <textarea id="cancelledCause" rows="3" style="width: 100%;border-color: #ced4da; border-radius: 10px; padding-top: 5px; padding-left: 10px; line-height: 1.5;" placeholder="Please enter the cancellation reason"></textarea>`,
        buttons: {
            confirm: {
                btnClass: 'custom-btn-green',
                action: function () {
                    let cancelledCause = $('#cancelledCause').val();
                    if (!cancelledCause) {
                        $.alert({
                            title: 'INFO',
                            content: `Cancellation reason is needed.`,
                        })
                        return
                    } else {
                        let cancelBy = $('.cancelBy input[type="radio"]:checked').attr('value')
                        axios.post("/urgent/cancelIndent", { id, requestId, cancelledCause, cancelBy }).then(async res => {
                            if (res.respCode != 1) {
                                $.alert({
                                    title: 'Warn',
                                    type: 'red',
                                    content: res.respMessage,
                                });
                            } else {
                                table.ajax.reload(null, true);
                            }
                        });
                    }
                }
            },
            cancel: function () {
                table.ajax.reload(null, true);
            }
        }
    });
}

const reAssignIndent = async function (requestId, id, dutyId) {
    const getAvailableDutyList = async function (id) {
        return await axios.post('/urgent/getAvailableDuty', { id }).then(res => {
            if (res.respCode != 1) {
                return []
            } else {
                return res.respMessage
            }
        })
    }
    const generateHtml = function (dataList) {
        let html = `
            <style>
                .select-duty td {
                    line-height: 40px;
                }
                .select-duty tbody tr:hover {
                    background-color: #f7f3f3;
                }
                .select-duty input {
                    margin-top: 13px;
                }
            </style>
            <table aria-hidden="true" class="select-duty px-2 mt-2" style="width: 90%; text-align: center; margin-left: 5%">
                <thead>
                    <tr class="fw-bold fs-6" style="background-color: #ebe7e7; border-radius: 10px;">
                        <td></td>
                        <td>Duty ID</td>
                        <td>Driver</td>
                        <td>Vehicle Type</td>
                        <td>Vehicle No</td>
                    </tr>
                </thread>
                <tbody>
                    ${  dataList.map(data => {
                            return `
                                <tr>
                                    <td style="float: right;">
                                        <div class="form-check">
                                            <input class="form-check-input" name="select-duty" type="radio" value="${ data.dutyId }">
                                        </div>
                                    </td>
                                    <td>${ data.configId }</td>
                                    <td>${ data.driverName }</td>
                                    <td>${ data.vehicleType }</td>
                                    <td>${ data.vehicleNo }</td>
                                </tr>
                            `
                        }).join('') 
                    }
                </tbody>
            </table>
        `
        return html
    }

    let availableDutyList = await getAvailableDutyList(id);
    let html = generateHtml(availableDutyList);
    $.confirm({
        title: `Re-Assign ${ requestId }`,
        content: html,
        boxWidth: '600px',
        useBootstrap: false,
        buttons: {
            cancel: function () {
                table.ajax.reload(null, true);
            },
            confirm: {
                btnClass: 'custom-btn-green',
                action: function () {
                    let selectedDutyId = $('.select-duty input[type="radio"]:checked').val()
                    if (selectedDutyId) {
                        $.confirm({
                            title: null,
                            content: `Are you sure to re-assign indent ${ requestId }`,
                            buttons: {
                                cancel: function () {
                                    table.ajax.reload(null, true);
                                },
                                confirm: {
                                    btnClass: 'custom-btn-green',
                                    action: function () {
                                        axios.post('/urgent/reAssignIndent', { id, newDutyId: selectedDutyId, oldDutyId: dutyId }).then(res => {
                                            if (res.respCode != 1) {
                                                $.alert(res.respMessage)
                                            } 
                                            table.ajax.reload(null, true);
                                        })
                                    }
                                }
                            }
                        })
                    }
                }
            }
        }
    })
    
}