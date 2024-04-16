let userType = Cookies.get('userType').toUpperCase()
$(function () {
    initPage()
    $('#task-report-create').on('click', async function () {
        await createDriverReport()
    })

    $('.hub-node-div').show()
    // $('.hub-node-check').show()
    if (userType != 'ADMINISTRATOR' && userType != 'HQ') {
        $('.hub-node-div').hide()
    }
    $('.Unit-div').show()
    if(userType == 'UNIT' || userType == 'CUSTOMER'){
        $('.Unit-div').hide()
    }

    $('.report-data-group input').off('click').on('click', function () {
        let val = $(this).val()
        let name = $(this).attr('name')
        if ($(this).prop('checked')) {
            addDataGroupEventHandler(val, name)
        } else {
            removeSelectedReportDataGroup(val)
            removeReportDataFilter(name)
        }
    })

    $("#select-all-checkbox").on('click', async function () {
        let selectAll = $(this).prop('checked')
        $('.report-data-group input[type="checkbox"]').each(function () {
            if ($(this).prop('checked') != selectAll) {
                $(this).trigger('click')
            }
        })
    })
})

const initPage = async function(){
    const initPermitTypeData = function () {
        axios.post("/driver/getPermitTypeList").then(async res => {
            let permitTypeList = res.data.respMessage;
            $("#vehicleClass").empty();
            let optionHtml = `<option value="">All</option>`;
            for (let item of permitTypeList) {
                optionHtml += `<option value="${item.permitType}" >${item.permitType}</option>`
            }
            $("#vehicleClass").append(optionHtml);
        })
    }
    initPermitTypeData();

    const getVehicleTypeList = async function () {
        axios.post('/hoto/getVehicleTypeList')
            .then(function (res) {
                let vehicleTypeList = res.respMessage ? res.respMessage : res.data.respMessage;
                $("#platforms").empty();
                let optionHtml = `<option value="">All</option>`;
                vehicleTypeList = vehicleTypeList.sort()
                for (let item of vehicleTypeList) {
                    optionHtml += `<option value="${item}" >${item}</option>`
                }
                $("#platforms").append(optionHtml);
        });
    }
    getVehicleTypeList()

    const initLayDate = function () {
        layui.use('layer', function(){
            layer = layui.layer;
        });
        layui.use('laydate', function(){
            let laydate = layui.laydate;
    
            laydate.render({
                elem: '#enlistment-Date-Range',
                format: 'dd/MM/yyyy',
                type: 'date',
                lang: 'en',
                range: true,
                trigger: 'click',
                btns: ['clear', 'confirm']
            });
    
            laydate.render({
                elem: '#ORD-date-range',
                format: 'dd/MM/yyyy',
                type: 'date',
                lang: 'en',
                range: true,
                trigger: 'click',
                btns: ['clear', 'confirm']
            })

            laydate.render({
                elem: '#reportDateRange',
                lang: 'en',
                type: 'date',
                trigger: 'click',
                format: 'yyyy-MM-dd',
                btns: ['clear', 'confirm'],
                range: '~',
                done: function () {
                }
            })
        });
    }
    initLayDate();

    const initHubNodeFilter = async function () {
        await axios.post("/unit/getHubNodeList").then(res => {
            let hubNodeList = res.respMessage ?? res.data.respMessage ;

            let list = hubNodeList.map(o => {
                return `<option value="${o.hub}">${o.hub}</option>`
            }).join('')
            $("#hubFilter").append(list)
            $("#hubFilter").off('change').on('change', function(){
                $("#UnitFilter").val('')
                let hub = $("#hubFilter").val()
                if(!hub){
                    $("#nodeFilter").empty()
                    $("#nodeFilter").append(`<option value="">All</option>`)
                    return
                } 
                let item = hubNodeList.find(o => o.hub == hub)
                let nodeList = item.nodeList
                if(!nodeList) return
                if(nodeList.length < 1) return
                let list = nodeList.split(',').map(o => {
                    return `<option value="${o}">${o}</option>`
                }).join('')
                $("#nodeFilter").empty()
                $("#nodeFilter").append(`<option value="">All</option>` + list)
            })
        })
    }
    initHubNodeFilter()
    const initSupportedUnit = async function () {
        await axios.post("/getGroupList").then(res => {
            //console.log(res)
            let groupList = res.respMessage ?? res.data.respMessage;

            let list = groupList.map(o => {
                return `<option value="${o.id}">${o.groupName}</option>`
            }).join('')
            $("#UnitFilter").append(list)
        })
    }
    initSupportedUnit()
    $("#UnitFilter").off('change').on('change', function () {
        $("#hubFilter").val('')
        $("#nodeFilter").val('')
    })


    // const initStatus = async function (){
    //     axios.post("/driver/getTODriverStatusList").then(async res => {
    //         let driverStatusList = res.data.respMessage;
    //         let list = driverStatusList.map(o => {
    //             return `<option value="${item}">${item}</option>`
    //         }).join('')
    //         $("#drivingPermitStatus").append(`<option value="">All</option>` + list)
    //     })
    // }
    // initStatus()

    const getRoleVocation = async function () {
        return axios.post('/getRoleVocation')
                .then(function (res) {
                    return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    let roleVocation = await getRoleVocation();

    let roleData = []
    Object.keys(roleVocation).forEach(function(key){
        roleData.push(key)
    })

    const initRoleSelect = function (data) {
        $('.driver-role-select').empty();
        let html = `<option></option>`;
        for(let item of data){
            html += `<option>${ item }</option>`;
        }
        $('.driver-role-select').append(html)
    }


    initRoleSelect(roleData)

    const initVocationSelect = function (data) {
        $('.driver-vocation-select').empty();
        for(let item of data){
            $('.driver-vocation-select').append(`<option>${ item ? item : '-' }</option>`);
        }
    }

    $('.driver-role-select').off('change').on('change', function () {
        if($('.driver-role-select').val()){
            initVocationSelect(roleVocation[$('.driver-role-select').val()])
        } else {
            $('.driver-vocation-select').empty();
        }
    });
}

const initDate = function(date) {
    if (date) {
        if (date.indexOf('-') != -1) {
            const dates = date.split(' - ')
            if(dates.length > 0) {
                dates[0] = moment(dates[0], 'DD/MM/YYYY').format('YYYY-MM-DD')
                dates[1] = moment(dates[1], 'DD/MM/YYYY').format('YYYY-MM-DD')
                date = dates.join(' - ')
            }
        } else {
            date = moment(date, 'DD/MM/YYYY').format('YYYY-MM-DD')
        }
        return date
    }
}


const createDriverReport = async function () {
    let reportGroupSelectionTitle = []
    $('.drag-list > li').each(function () {
        let val = $(this).attr("data-val")
        let name = $(this).attr("data-name")
        if (name == "hubNode") {
            reportGroupSelectionTitle.push('Hub/Node')
        } else {
            reportGroupSelectionTitle.push(val)
        }
    })

    console.log(reportGroupSelectionTitle)
    if(reportGroupSelectionTitle.length <= 0) {
        $.alert({
            title: 'Warn',
            content: `Select report data.`,
        });
        return
    }
    window.parent.showLoading();
    await axios.post("/reportCreator/getDriverReportList", {
        reportGroupSelectionTitle: reportGroupSelectionTitle,
        reportDateRange: $("#reportDateRange").val(),
        driverStatus: $('#drivingPermitStatus').val(), 
        driverCategory: $('#category').val(), 
        driverClass: $('#vehicleClass').val(), 
        driverType: $('#platforms').val(),
        enDateRange: $('#enlistment-Date-Range').val() ? initDate($('#enlistment-Date-Range').val()) : null, 
        ordRange: $('#ORD-date-range').val() ? initDate($('#ORD-date-range').val()) : null, 
        groupId: $('#UnitFilter').val(),
        hub: $('#hubFilter').val(), 
        node: $('#nodeFilter').val(),
        role: $('.driver-role-select').val(),
        vocation: $('.driver-vocation-select').val(),
        ordStart: $('.driverORDStatus').val()
    }).then(async res => {
        console.log(res)
        window.parent.closeLoading();
        if (res.respCode == 0) {
            $.alert({
                title: 'Warn',
                content: `Create Failed`,
            });
            return
        }
        if (res.respMessage) {
            let filename = res.respMessage
            window.location.href = "/reportCreator/downloadExcel?filename=" + filename
        }
    })
}

const addDataGroupEventHandler = async function (val, name) {
    const dragEventHandler = function () {
        $('.drag-list > li').arrangeable({ dragSelector: '.drag-area' });
        $('.drag-area').off('mousedown');
        $('.drag-area').off('mouseleave');
        $('.drag-area').off('mouseup');
        $('.drag-area').on('mousedown', function () {
        });
        $('.drag-area').on('mouseleave', function () {
            $(this).parent().css('background-color', 'rgba(0, 0, 0, 0)');
        });
        $('.drag-area').on('mousemove', function (e) {
        });

        $('.div-report-group-list').scrollTop($('.drag-list').height());
    }
    const generateListHtml = function () {
        let html = `
        <li data-val="${val}" data-name="${name}">
            <div class="row justify-content-center">
                <div class="col-2">
                    <span class="drag-area"></span>
                </div>
                <div class="col-9">
                    <input type="text" class="form-control" readonly value="${val}">
                </div>
                <div class="col-1">
                    <button type="button" class="btn btn-sm custom-btn btn-primary" style="width: 30px;" onclick="removeReportDataGroup(this)">-</button>
                </div>
            </div>
        </li>
        `
        $('.drag-list').append(html);
        if (name == 'odometer') {
            return
        }
    }
    generateListHtml();
    dragEventHandler();

   
}

const removeSelectedReportDataGroup = function (val) {
    $('.drag-list > li').each(function () {
        if ($(this).attr("data-val") == val) {
            $(this).remove()
        }
    })
}
const removeReportDataGroup = function (e) {
    let val = $(e).closest('li').attr("data-val")
    let name = $(e).closest('li').attr("data-name")
    $(e).closest('li').remove()
    removeReportDataFilter(name)
    $('.report-data-group input[type="checkbox"]').each(function () {
        if ($(this).val() == val) {
            $(this).prop('checked', false)
        }
    })
}
const removeReportDataFilter = function (name) {
    $('.div-report-data-filter div').each(function () {
        if ($(this).attr('data-filter') == name) {
            $(this).remove()
        }
    })
}
