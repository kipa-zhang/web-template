let taskStatusFilterSelect = null
let hubNodeList = []
let userType = Cookies.get('userType').toUpperCase()
$(function () {
    if (userType != 'ADMINISTRATOR' && userType != 'HQ') {
        // $('#hub-checkbox').parent().remove()
        // $('#supported-unit-checkbox').parent().remove()
    }
    initReportHtml();

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
            if (![ 'transmittedDate'].includes($(this).attr('name')) && $(this).prop('checked') != selectAll) {
                $(this).trigger('click')
            }
        })
    })
})

const initReportHtml = async function () {
    const InitReportDateRange = function (elem) {
        layui.use(['laydate'], function () {
            let laydate = layui.laydate;
            laydate.render({
                elem: elem,
                lang: 'en',
                type: 'datetime',
                trigger: 'click',
                format: 'yyyy-MM-dd HH:mm:ss',
                min: moment().subtract(6, 'month').format('YYYY-MM-DD HH:mm:ss'),
                max: moment().add(1, 'month').format('YYYY-MM-DD HH:mm:ss'),
                btns: ['clear', 'confirm'],
                range: '~',
                done: function () {
                }
            });
        });
    }
    const initHubNodeFilter = async function () {
        let res = await axios.post("/unit/getHubNodeList")
        hubNodeList = res.data ? res.data.respMessage : res.respMessage;

        let list = hubNodeList.map(o => {
            return `<option value="${o.hub}">${o.hub}</option>`
        }).join('')
        $("#hubFilter").append(list)
    }
    const initSupportedUnit = async function () {
        await axios.post("/getGroupList").then(res => {
            let groupList = res.data ? res.data.respMessage : res.respMessage;

            let list = groupList.map(o => {
                return `<option value="${o.id}">${o.groupName}</option>`
            }).join('')
            $("#supportedUnitFilter").append(list)
        })
    }
    
    const initVehicleTypeFilter = async function () {
        await axios.post("/assign/getVehicleType").then(res => {
            let vehicleTypeList = res.data ? res.data.respMessage : res.respMessage;
            let html = ``;
            for (let vehicleType of vehicleTypeList) {
                if (vehicleType.typeOfVehicle) html += `<option value='${vehicleType.typeOfVehicle}'>${vehicleType.typeOfVehicle}</option>`;
            }
            $("#vehicleTypeFilter").append(html)
        })
    }

    // $('.div-report-data-filter').append($(`#violationTypeHtml`).html())
    // $('.div-report-data-filter').append($(`#driverHtml`).html())
    if (userType == 'ADMINISTRATOR' || userType == 'HQ') {
        $('.div-report-data-filter').append($(`#hubNodeHtml`).html())
    }
    $('.div-report-data-filter').append($(`#vehicleHtml`).html())
    $('.div-report-data-filter').append($(`#deviceIdHtml`).html())
    // $('.div-report-data-filter').append($(`#dataFromHtml`).html())
    // $('.div-report-data-filter').append($(`#taskIDHtml`).html())
    $('.div-report-data-filter').append($(`#transmittedDateHtml`).html())
    // $('.div-report-data-filter').append($(`#occTimeRangeHtml`).html())
    
    InitReportDateRange('#occTimeRange')
    if (userType == 'ADMINISTRATOR' || userType == 'HQ') {
        await initHubNodeFilter()
        initSupportedUnit()
    }
    initVehicleTypeFilter()
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
        $('.drag-list').prepend(html);
        // $('.div-report-data-filter').append($(`#${name}Html`).html())
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
    // removeReportDataFilter(name)
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

const createTaskReport = async function () {
    let reportGroupSelectionTitle = []
    $('.drag-list > li').each(function () {
        let val = $(this).attr("data-val")
        let name = $(this).attr("data-name")
        //console.log(name + " -- " + val)
        reportGroupSelectionTitle.push(val)
    })
    
    let filter = {}
    $('.div-report-data-filter > div').each(function () {
        let name = $(this).attr("data-filter")
        if (name == 'occTimeRange') {
            filter['occTimeRange'] = $("#occTimeRange").val()
        } else if (name == "hubNode") {
            //let val = hubNodeFilterSelect.getValue()
            filter['hub'] = $("#hubFilter").val()
            filter['node'] = $("#nodeFilter").val()
        } else if (name == 'driver') {
            filter['driverName'] = $(`#driverNameFilter`).val()
        } else if (name == 'vehicle') {
            filter['vehicleType'] = $(`#vehicleTypeFilter`).val()
            filter['vehicleNumber'] = $(`#vehicleNumberFilter`).val()
        } else if (name == 'dataFrom') {
            filter['dataFrom'] = $(`#dataFromFilter`).val()
        } else if (name == 'taskId') {
            filter['taskId'] = $(`#taskIdFilter`).val()
        } 
        // else if (name == 'transmittedDate') {
        //     filter['transmittedDate'] = $(`#transmittedDateFilter`).val()
        // }  
        else if (name == 'violationType') {
            filter['violationType'] = $(`#violationTypeFilter`).val()
        } else {
            filter[name] = $(`#${name}Filter`).val()
        }
    })

    console.log(reportGroupSelectionTitle)
    console.log(filter)
    // if (!filter.occTimeRange) {
    //     simpleAlert('Please select report date range!')
    //     return
    // }
    if (reportGroupSelectionTitle.length == 0) {
        simpleAlert('Please choose the data to be included in the report!')
        return
    }
    //return
    window.parent.showLoading();
    await axios.post("/reportCreator/getOBDReportList", {
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

const checkTransmittedDate = function (el) {
    let val = $(el).val();
    val = Number.parseInt(val)
    if (val < 0) {
        $(el).val(0)
    } else {
        $(el).val(val)
    }
}