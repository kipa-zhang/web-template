let taskStatusFilterSelect = null
$(function () {
    InitReportDateRange('#reportDateRange')

    $('.report-data-group input').off('click').on('click', function () {
        let val = $(this).val()
        let name = $(this).attr('name')
        if ($(this).prop('checked')) {
            addDataGroupEventHandler(val, name)
        } else {
            removeSelectedReportDataGroup(val)
            //removeReportDataFilter(name)
        }
    })

    $('#keypress-report-create').on('click', async function () {
        await createKeypressReport()
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
            done: function () {
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
    //removeReportDataFilter(name)
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

const createKeypressReport = async function () {
    let reportGroupSelectionTitle = []
    let filter = {}
    filter['reportDateRange'] = $("#reportDateRange").val() ? initDate($("#reportDateRange").val()) : null;
    $('.drag-list > li').each(function () {
        let val = $(this).attr("data-val")
        let name = $(this).attr("data-name")
        //console.log(name + " -- " + val)
        reportGroupSelectionTitle.push(val)
    })

    filter['location'] = $(`#locationFilter`).val();
    filter['boxName'] = $(`#boxNameFilter`).val();
    filter['transactionType'] = $(`#transactionTypeFilter`).val();

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
    await axios.post("/reportCreator/getKeypressReportList", {
        reportGroupSelectionTitle: reportGroupSelectionTitle,
        filter: filter
    }).then(async res => {
        console.log(res)
        window.parent.closeLoading();
        if (res.respCode == 0) {
            let errorMsg = res.respMessage
            $.alert({
                title: 'Warn',
                content: `Create Failed: ${errorMsg}`,
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