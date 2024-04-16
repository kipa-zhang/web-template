let taskStatusFilterSelect = null
let hubNodeList = []
let userType = Cookies.get('userType').toUpperCase()
$(function () {
    if (userType != 'ADMINISTRATOR' && userType != 'HQ') {
        // $('#hub-checkbox').parent().remove()
        $('#supported-unit-checkbox').parent().remove()
    }
    InitReportDateRange('#reportDateRange')

    $('.report-data-group input').off('click').on('click', function () {
        let val = $(this).val()
        let name = $(this).attr('name')
        if ($(this).prop('checked')) {
            addDataGroupEventHandler(val, name)
        } else {
            removeSelectedReportDataGroup(val)
            // removeReportDataFilter(name)
        }
    })

    $('#task-report-create').on('click', async function () {
        await createTaskReport()
    })

    $("#select-all-checkbox").on('click', async function () {
        let selectAll = $(this).prop('checked')
        $('.report-data-group input[type="checkbox"]').each(function () {
            if ($(this).prop('checked') != selectAll) {
                $(this).trigger('click')
            }
        })
    })

    initFilterHandler()
})

const initFilterHandler = async function () {
    await initHubNodeFilter()
    initTaskStatusFilter()
    InitReportDateRange('#executionTimeFilter')
    InitReportDateRange('#actualTimeFilter')
    initPurposeFilter()
    initSupportedUnit()
    initVehicleTypeFilter()
}

const InitReportDateRange = function (elem) {
    layui.use(['laydate'], function () {
        let laydate = layui.laydate;
        laydate.render({
            elem: elem,
            lang: 'en',
            type: 'date',
            trigger: 'click',
            format: 'dd/MM/yyyy',
            btns: ['clear', 'confirm'],
            range: '~',
            done: function (value) {
                if(value) value = moment(value, 'DD/MM/YYYY').format('YYYY-MM-DD')
            }
        });
    });
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
                <div class="col-8">
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
        // $('.div-report-data-filter').append($(`#${name}Html`).html())
    }
    generateListHtml();
    dragEventHandler();

    // if (name == "hubNode") {
    //     await initHubNodeFilter()
    // } else if (name == 'taskStatus') {
    //     initTaskStatusFilter()
    // } else if (name == 'executionTime') {
    //     InitReportDateRange('#executionTimeFilter')
    // } else if (name == 'actualTime') {
    //     InitReportDateRange('#actualTimeFilter')
    // } else if (name == 'purpose') {
    //     initPurposeFilter()
    // } else if (name == 'supportedUnit') {
    //     initSupportedUnit()
    // } else if (name == 'vehicle') {
    //     initVehicleTypeFilter()
    // }
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
    // removeReportDataFilter(name)
    $('.report-data-group input[type="checkbox"]').each(function () {
        if ($(this).val() == val) {
            $(this).prop('checked', false)
        }
    })
}
// const removeReportDataFilter = function (name) {
//     $('.div-report-data-filter div').each(function () {
//         if ($(this).attr('data-filter') == name) {
//             $(this).remove()
//         }
//     })
// }

const initHubNodeFilter = async function () {
    /*await axios.post("/getUnitList").then(res => {
        //console.log(res)
        let datas = res.respMessage;

        let data = []
        for (let item of datas) {
            data.push({ id: item.id, name: `${item.unit}/${item.subUnit ? item.subUnit : "-"}` })
        }

        hubNodeFilterSelect = $("#hubNodeFilter").multipleSelect({
            dataKey: 'id',
            dataName: 'name',
            searchable: false,
            data: data,
        });
    })*/
    await axios.post("/unit/getHubNodeList").then(res => {
        //console.log(res)
        hubNodeList = res.data ? res.data.respMessage : res.respMessage;

        let list = hubNodeList.map(o => {
            return `<option value="${o.hub}">${o.hub}</option>`
        }).join('')
        $("#hubFilter").append(list)
    })
}

const initSupportedUnit = async function () {
    await axios.post("/getGroupList").then(res => {
        //console.log(res)
        let groupList = res.respMessage;

        let list = groupList.map(o => {
            return `<option value="${o.id}">${o.groupName}</option>`
        }).join('')
        $("#supportedUnitFilter").append(list)
    })
}

const changeHub = function () {
    let hub = $("#hubFilter").val()
    let item = hubNodeList.find(o => o.hub == hub)
    let nodeList = item.nodeList

    let list = nodeList.split(',').map(o => {
        return `<option value="${o}">${o}</option>`
    }).join('')
    $("#nodeFilter").empty()
    $("#nodeFilter").append(`<option value="">All</option>` + list)
}

const initTaskStatusFilter = async function () {
    let data = [
        { id: 'Waitcheck', name: 'Pending' },
        { id: 'System Expired', name: 'System Expired' },
        { id: 'Ready', name: 'Ready' },
        { id: 'Started', name: 'Started' },
        { id: 'Completed', name: 'Completed' },
        { id: 'Cancelled', name: 'Cancel' },
    ]

    taskStatusFilterSelect = $("#taskStatusFilter").multipleSelect({
        dataKey: 'id',
        dataName: 'name',
        searchable: false,
        data: data,
    });
}

const initPurposeFilter = async function () {
    await axios.post("/vehicle/getPurpose").then(res => {
        //console.log(res)
        let datas = res.respMessage;
        let { mtPurposeList, systemPurposeList } = datas
        let purposeList = mtPurposeList.concat(systemPurposeList)
        let purpose = purposeList.map(o => {
            return `<option value="${o.purposeName}">${o.purposeName}</option>`
        }).join('')
        purpose += `<option value="Urgent Duty">Urgent Duty</option>`
        $("#purposeFilter").append(purpose)
    })
}

const initVehicleTypeFilter = async function () {
    await axios.post("/assign/getVehicleType").then(res => {
        //console.log(res)
        let vehicleTypeList = res.respMessage;
        let html = ``;
        for (let vehicleType of vehicleTypeList) {
            if (vehicleType.typeOfVehicle) html += `<option value='${vehicleType.typeOfVehicle}'>${vehicleType.typeOfVehicle}</option>`;
        }
        $("#vehicleTypeFilter").append(html)
    })
}

const initDate = function(date) {
    if (date) {
        if (date.indexOf('~') != -1) {
            const dates = date.split(' ~ ')
            if(dates.length > 0) {
                dates[0] = moment(dates[0], 'DD/MM/YYYY').format('YYYY-MM-DD')
                dates[1] = moment(dates[1], 'DD/MM/YYYY').format('YYYY-MM-DD')
                date = dates.join(' ~ ')
            }
        } else {
            date = moment(date, 'DD/MM/YYYY').format('YYYY-MM-DD')
        }
        return date
    }
}

const createTaskReport = async function () {
    let reportGroupSelectionTitle = []
    $('.drag-list > li').each(function () {
        let val = $(this).attr("data-val")
        let name = $(this).attr("data-name")
        //console.log(name + " -- " + val)
        reportGroupSelectionTitle.push(val)
        // if (name == "hubNode") {
        //     //let val = hubNodeFilterSelect.getValue()
        //     filter['hub'] = $("#hubFilter").val()
        //     filter['node'] = $("#nodeFilter").val()
        // } else if (name == 'taskStatus') {
        //     let val = taskStatusFilterSelect.getValue()
        //     filter[name] = val
        // } else if (name == 'driver') {
        //     filter['driverName'] = $(`#driverNameFilter`).val()
        //     filter['mobileNumber'] = $(`#mobileNumberFilter`).val()
        // } else if (name == 'vehicle') {
        //     filter['vehicleType'] = $(`#vehicleTypeFilter`).val()
        //     filter['vehicleNumber'] = $(`#vehicleNumberFilter`).val()
        // }
        // else if (name == 'odometer') {
        //     filter['startOdometer'] = $(`#startOdometerFilter`).val()
        //     filter['endOdometer'] = $(`#endOdometerFilter`).val()
        // }
        // else {
        //     filter[name] = $(`#${name}Filter`).val()
        // }
    })

    
    let filter = {}
    let reportDateRange = $("#reportDateRange").val() ? initDate($("#reportDateRange").val()) : ''
    filter['reportDateRange'] = reportDateRange;
    $('.div-report-data-filter > div').each(function () {
        let name = $(this).attr("data-filter")
        if (name == "hubNode") {
            //let val = hubNodeFilterSelect.getValue()
            filter['hub'] = $("#hubFilter").val()
            filter['node'] = $("#nodeFilter").val()
        } else if (name == 'taskStatus') {
            let val = taskStatusFilterSelect.getValue()
            if (typeof val == 'object') val = val.join(',')
            filter[name] = val
        } else if (name == 'driver') {
            filter['driverName'] = $(`#driverNameFilter`).val()
            filter['mobileNumber'] = $(`#mobileNumberFilter`).val()
        } else if (name == 'vehicle') {
            filter['vehicleType'] = $(`#vehicleTypeFilter`).val()
            filter['vehicleNumber'] = $(`#vehicleNumberFilter`).val()
        }
        else if (name == 'odometer') {
            filter['startOdometer'] = $(`#startOdometerFilter`).val()
            filter['endOdometer'] = $(`#endOdometerFilter`).val()
        }
        else {
            filter[name] = $(`#${name}Filter`).val()
        }
    })
    
    let actualTime = filter['actualTime'] ? initDate(filter['actualTime']) : null
    filter['actualTime'] = actualTime
    console.log(reportGroupSelectionTitle)
    console.log(filter)
    if (filter.reportDateRange == "") {
        simpleAlert('Please select report date range!')
        return
    }
    if (reportGroupSelectionTitle.length == 0) {
        simpleAlert('Please choose the data to be included in the report!')
        return
    }
    //return
    window.parent.showLoading();
    await axios.post("/reportCreator/getTaskReportList", {
        reportGroupSelectionTitle: reportGroupSelectionTitle,
        filter: filter
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

const downloadExcel = async function (filename) {
    await axios.get('/reportCreator/downloadExcel', { params: { filename: filename }, responseType: 'blob' }).then(async result => {
        const { data, headers } = result
        console.log(result)
        const fileName = headers['Content-Disposition'].replace(/\w+;filename=(.*)/, '$1')
        const blob = new Blob([data], { type: headers['Content-Type'] })
        let dom = document.createElement('a')
        let url = window.URL.createObjectURL(blob)
        dom.href = url
        dom.download = decodeURI(fileName)
        dom.style.display = 'none'
        document.body.appendChild(dom)
        dom.click()
        dom.parentNode.removeChild(dom)
        window.URL.revokeObjectURL(url)
    })
}

const simpleAlert = function (error) {
    $.alert({
        title: 'WARN',
        content: error,
    });
}

const toReportPage = function(url) {
    window.location.href = "/reportCreator/" + url;
}