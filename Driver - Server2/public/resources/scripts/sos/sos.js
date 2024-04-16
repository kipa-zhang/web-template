import { initIncidentPage } from '../detailIncident/detailIncident.js'

let table
jconfirm.defaults = {
    animation: 'top',
    closeAnimation: 'top',
    animationSpeed: 400,
    typeAnimated: true,
}
let sosId = null

$(function () {
    initDataTable();
    initPage();
    // InitFilter();

    setInterval(() => {
        table.ajax.reload(null, false)
    }, 60000)

    $('#select-customer').on('change', async function () {
        if ($(this).prop('checked')) {
            $('.show-group-select').show()
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
})

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
const getHubNode = async function() {
    return await axios.post('/unit/getHubNodeList')
    .then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
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

    $('.btn-dashboard').off('click').on('click', function () {
        $('.btn-dashboard').removeClass('active');
        $(this).addClass('active');
        initDataTable();
    })

    $('#clearAll').on('click', function () {
        $(".select-hub").find("option").eq(0).prop("selected",true)
        $(".select-group").find("option").eq(0).prop("selected",true)
        $('.select-hub').trigger('change')

        $(".selectedDate").val("")
        $(".driverName").val("")
        $(".vehicleNo").val("")
        $(".taskId").val("")
        
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

    if (Cookies.get('userType').toLowerCase() != 'customer') {
        let hubNodeList = await getHubNode();
        initUnit(hubNodeList);
    }

    if (['hq', 'customer', 'administrator'].indexOf(Cookies.get('userType').toLowerCase()) > -1) {
        let groupList = await getGroupList();
        initGroup(groupList);
    }
}

window.showSOSDetail = function (e) {
    const generateHtml = function (object) {

        const generateTOPastIncidentHtml = function (incident) {
            return `
                <div class="container-fluid child-container">
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Type :</div>
                        <div class="col-7">${ incident.type ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Remarks :</div>
                        <div class="col-7">${ incident.remarks ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold" style="line-height: 15px;">Brief Description of Incident :</div>
                        <div class="col-7">${ incident.description ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Follow up actions :</div>
                        <div class="col-7">${ incident.followUpAction ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Location :</div>
                        <div class="col-7">${ incident.location ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Create Date :</div>
                        <div class="col-7">${ moment(incident.createdAt).format('DD/MM/YYYY HH:mm A') }</div>
                    </div>
                </div>
            `
        }
        const generateVehicleDetailHtml = function (incident) {
            return `
                <div class="container-fluid child-container">
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Type :</div>
                        <div class="col-7">${ incident.vehicleType ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Class :</div>
                        <div class="col-7">${ incident.permitType ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">No. :</div>
                        <div class="col-7">${ incident.vehicleNumber ?? '-' }</div>
                    </div>
                </div>
            `
        }
        const generateTaskDetailHtml = function (incident) {
            return `
                <div class="container-fluid child-container">
                    <div class="row">
                        <div class="col-5 text-end fw-bold">ID :</div>
                        <div class="col-7">${ incident.taskId ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold" style="padding-left: 0;">Execution Start/End Time :</div>
                        <div class="col-7">${ incident.indentStartTime ? moment(incident.indentStartTime).format('DD/MM/YYYY HH:mm') : '-' } / ${ incident.indentEndTime ? moment(incident.indentEndTime).format('DD/MM/YYYY HH:mm') : '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Activity :</div>
                        <div class="col-7">${ incident.activity ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Purpose :</div>
                        <div class="col-7">${ incident.purpose ?? '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Actual Start/End Time :</div>
                        <div class="col-7">${ incident.mobileStartTime ? moment(incident.mobileStartTime).format('DD/MM/YYYY HH:mm') : '-' } / ${ incident.mobileEndTime ? moment(incident.mobileEndTime).format('DD/MM/YYYY HH:mm') : '-' }</div>
                    </div>
                    <div class="row">
                        <div class="col-5 text-end fw-bold">Location :</div>
                        <div class="col-7">${ incident.pickupDestination ?? '-' } / ${ incident.dropoffDestination ?? '-' }</div>
                    </div>
                </div>
            `
        }
        const generateVehicleCommanderDetailsHtml = function (commanderList) {
            let html = `
            <div class="container-fluid child-container">
                <div class="row">
                    <div class="col-5 text-end fw-bold">Commander</div>
                    <div class="col-7 text-start fw-bold">ContactNumber</div>
                </div>
            `
            for (let commander of commanderList) {
                html += `
                    <div class="row">
                        <div class="col-5 text-end">${ commander.commander }</div>
                        <div class="col-7 text-start">${ commander.commanderContactNumber }</div>
                    </div>
                `
            }

            html += `</div>`
            return html
        }
        let html = `
            <div class="container-fluid my-1">
                <div class="row">
                    <div class="col-4 text-end fw-bold">Type :</div>
                    <div class="col-8">${ object.type }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Unit Involved :</div>
                    <div class="col-8">${ object.unitInvolved ? object.unitInvolved : '-' }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Hub/Node :</div>
                    <div class="col-8">${ object.hub ?? '-' }/${ object.node ?? '-' }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">TO Name :</div>
                    <div class="col-8">${ object.driverName }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">TO CAT Status :</div>
                    <div class="col-8">${ object.toCATStatus }</div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">TO Past Incident :</div>
                    <div class="col-8">
                        ${ object.toPastIncident ? generateTOPastIncidentHtml(object.toPastIncident) : '-' }
                    </div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Vehicle Commander Details :</div>
                    <div class="col-8">
                        ${ object.vehicleCommanderDetails && object.vehicleCommanderDetails.length ? generateVehicleCommanderDetailsHtml(object.vehicleCommanderDetails) : '-' }
                    </div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Vehicle Details :</div>
                    <div class="col-8">
                        ${ object.vehicleNumber ? generateVehicleDetailHtml(object) : '-' }
                    </div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Task Details :</div>
                    <div class="col-8">
                        ${ object.taskId ? generateTaskDetailHtml(object) : '-' }
                    </div>
                </div>
                <div class="row">
                    <div class="col-4 text-end fw-bold">Location :</div>
                    <div class="col-8">${ object.location }</div>
                </div>
                <!--
                <div class="row mt-2">
                    <div class="col-4 text-end fw-bold">Brief Description of Incident :</div>
                    <div class="col-8">
                        <textarea class="form-control form-control-sm description" >
                        </textarea>
                    </div>
                </div>
                <div class="row mt-2">
                    <div class="col-4 text-end fw-bold">Follow up actions :</div>
                    <div class="col-8">
                        <textarea class="form-control form-control-sm action" >
                        </textarea>
                    </div>
                </div>
                -->
            </div>
        `;
        
        return html;
    }
    
    let row = table.row($(e).data('row')).data()
    let updateConfirm = $.confirm({
        title: 'Incident Info',
        boxWidth: '920px',
        useBootstrap: false,
        content: generateHtml(row),
        type: 'green',
        animation: 'top',
        closeAnimation: 'top',
        animationSpeed: 400,
        typeAnimated: true,
        buttons: {
            // update: {
            //     btnClass: 'btn-green', 
            //     action: function () {
            //         let description = this.$content.find('.description').val();
            //         let followUpAction = this.$content.find('.action').val();
            //         axios.post('/updateSOS', { id: row.id, description, followUpAction }).then(result => {
            //             if (result.respCode == 1) {
            //                 $.alert({
            //                     title: 'Info',
            //                     type: 'green',
            //                     content: 'Update success.'
            //                 });
            //                 table.ajax.reload(null, false);
            //                 updateConfirm.close()
            //             } else {
            //                 $.alert({
            //                     title: 'Warn',
            //                     type: 'red',
            //                     content: 'Update failed, please try again later.'
            //                 });
            //             }
            //         })
            //         return false;
            //     }
            // },
            close: function () {
                return true
            }
        },
        onContentReady: function () {
            this.$content.find('.description').val(row.description ?? '');
            this.$content.find('.action').val(row.followUpAction ?? '');
        }
    });
}

const initDataTable = function () {
    table = $('.incident-table').on('order.dt', function () {
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
        "sAjaxDataProp": "data",
        "ajax": {
            url: "/getSOSList",
            type: "POST",
            data: function (d) {
                let params = {}
                params.pageNum = d.start
                params.pageLength = d.length

                // console.log(d.order)
                if (d.order[0].column == 4) {
                    params.sortBy = 'createdAt'
                    params.sort = d.order[0].dir
                }
                
                params.sosType = $('.btn-dashboard.active').data('item')
                params.hub = $(".select-hub").val()
                params.node = $(".select-node").val()
                params.selectedDate = $('.selectedDate').val()
                if (params.selectedDate) {
                    params.selectedDate = moment(params.selectedDate, 'DD/MM/YYYY').format('YYYY-MM-DD')
                }
                params.driverName = $('.driverName').val().trim()

                // params.group = $(".select-group").val()

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
                title: 'Hub/Node',
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ full.hub ?? '-' }<br>${ full.node ?? '-' }`
                }
            },
            {
                data: 'driverName',
                title: 'Driver Name',
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ data ? data : '-' } <br/> (${ full.contactNumber ? full.contactNumber : '-' })`
                }
            },
            {
                title: 'Type',
                data: 'type',
                sortable: false,
            },
            {
                title: 'Remarks',
                data: 'remarks',
                sortable: false,
                render: function (data, type, full, meta) {
                    return `${ data ? data : '-' }`
                }
            },
            {
                title: 'Create Date',
                data: 'createdAt',
                sortable: true,
                render: function (data, type, full, meta) {
                    return moment(data).format('DD/MM/YYYY HH:mm A')
                }
            },
            {
                data: null,
                title: 'View',
                sortable: false,
                render: function (data, type, full, meta) {
                    let operationList = (full.operation).toUpperCase().split(',')
                    let html = ` <button type="button" class="btn btn-sm custom-btn-green" style="padding: 2px 10px;" data-row="${ meta.row }" onclick="showSOSDetail(this)">Info</button> `
                    if ($('.btn-dashboard.active').data('item').toLowerCase() == 'incident') {
                        if (operationList.includes('EDIT') || operationList.includes('EDIT ISSUE')) {
                            html += ` <button type="button" class="btn btn-sm btn-primary btn-incident-detail" style="padding: 2px 10px; border: 0;" onclick="showIncidentDetail(${ full.id }, '${ full.permitType }')">Detail</button> `
                        }
                        return html
                    } else {
                        return html
                    }
                }
            },
        ],
        order: [4, 'desc']
    });
}


window.showIncidentDetail = async function(sosId, permitType){
    initIncidentPage(sosId, permitType)
    $('#view-incident-new').modal('show')
}
