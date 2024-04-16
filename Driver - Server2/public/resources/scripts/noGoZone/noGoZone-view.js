import { initNoGoZoneCreatePage } from '../noGoZone/noGoZone-create.js'

$(function () {
    $('.searchNoGoZone').off('keyup').on('keyup', _.debounce( initNoGoZoneViewPage, 500 ))
})

let noGoZoneList = [
];
let noGoZoneViewDataTable = null;

export async function initNoGoZoneList () {
    const getNoGoZoneList = async function () {
        let searchName = $('.searchNoGoZone').val()
        return await axios.post('/zone/getNoGoZoneList', { zoneName: searchName ?? undefined })
            .then(function (res) {
                if (res.respCode === 1) {
                    noGoZoneList = res.respMessage
                } else {
                    console.error(res.respMessage)
                }
            });
    }
    
    await getNoGoZoneList();
    return noGoZoneList;
};

export async function initNoGoZoneViewPage () {
    await initNoGoZoneList();
    $('.offcanvas').offcanvas('hide')
    noGoZoneViewDataTable = $('#noGoZone-list').DataTable({
        destroy: true,
        "ordering": false,
        "autoWidth": false,
        data: noGoZoneList,
        columns: [
            // { title: 'Selected', data: null, render: function (data, type, row, meta) {
            //     return `
            //         <div class="form-check">
            //             <input class="form-check-input" type="checkbox" value="">
            //         </div>
            //     `
            // } },
            // { title: 'Zone ID', data: 'id', sortable: false },
            { title: 'Zone Name', data: 'zoneName', defaultContent: '-', sortable: false },
            { title: 'Alert Days', data: 'selectedWeeks', defaultContent: '-', sortable: false, render: function (data, type, row, meta) {
                if (data) {
                    return generateWeekDays(data)
                } else {
                    return '-'
                }
            } },
            { title: 'Alert Time', data: 'selectedTimes', defaultContent: '-', sortable: false, render: function (data, type, row, meta) {
                if (data) {
                    return generateWeekTimes(data)
                } else {
                    return '-'
                }
            } },
            { title: 'Alert Date', data: null, defaultContent: '-', sortable: false, render: function (data, type, row, meta) {
                if (data) {
                    return `<label style="line-height: 18px">${ row.startDate ? moment(row.startDate, 'YYYY-MM-DD').format('DD/MM/YYYY') : '-' }</label>
                    <br>
                    to
                    <br>
                    <label style="line-height: 18px">${ row.endDate ? moment(row.endDate, 'YYYY-MM-DD').format('DD/MM/YYYY') : '-' }</label>`
                } else {
                    return '-'
                }
            } },
            { title: 'Created By', data: 'creator', defaultContent: '-', sortable: false },
            { title: 'Hub/Node/Group', data: null, defaultContent: '-', sortable: false, render: function (data, type, row, meta) {
                if (row.groupName) {
                    return row.groupName
                } else {
                    if (row.node) {
                        return `${ row.hub }/${ row.node }`
                    } else {
                        return row.hub
                    }
                }
            } },
            { title: 'Edit', data: null, sortable: false, render: function (data, type, row, meta) {
                let operationList = row.operation.split(',')

                let editBtn = `<button type="button" class="btn btn-primary btn-sm custom-btn-green " data-row="${ meta.row }" onclick='editZone(this)'>Edit</button>`
                let deleteBtn = `<button type="button" class="btn btn-primary btn-sm custom-btn-danger " data-row="${ meta.row }" style="margin-left: 5px;" onclick='deleteZone(this)'>Delete</button>`
                let disableBtn = `<button type="button" class="btn btn-primary btn-sm custom-btn-gray " data-row="${ meta.row }" style="margin-left: 5px;" onclick='enableZone(this, ${ row.id }, 0)'>Disable</button>`
                let enableBtn = `<button type="button" class="btn btn-primary btn-sm custom-btn-blue " data-row="${ meta.row }" style="margin-left: 5px;" onclick='enableZone(this, ${ row.id }, 1)'>Enable</button>`
                let html = ``

                if (operationList.includes('Edit')) html += editBtn
                if (operationList.includes('Delete')) html += deleteBtn

                if (row.alertType) {
                    if (row.enable && operationList.includes('Disable')) {
                        html += disableBtn
                    } 
                    if (!row.enable && operationList.includes('Enable')) {
                        html += enableBtn
                    } 
                }

                return html;
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        // searching: true,
        pageLength: 10,
        // scrollY: 200,
        // scrollCollapse: true,
        stateSave: true, // keep page, searching, filter
    });

    // show view noGoZone module
    $('#view-noGoZone').modal('show');
}

window.deleteZone = function (e) {
    let row = noGoZoneViewDataTable.row($(e).data('row')).data()
    axios.post(`/zone/deleteNogoZone`, { nogoZone: { id: row.id } }).then(result => {
        if (result.respCode === 1) {
            $.alert('Delete no go zone success.')
            initNoGoZoneViewPage()
        } else {
            console.error(result.respMessage)
            $.alert('Delete no go zone failed.')
        }
    }) 
}

window.editZone = function (e) {
    let row = noGoZoneViewDataTable.row($(e).data('row')).data()
    $('#view-noGoZone').modal('hide')
    $('.offcanvas').offcanvas('hide')
    $('#create-noGoZone').offcanvas('show')
    initNoGoZoneCreatePage(row)
}

window.enableZone = function (e, id, enable) {
    let row = noGoZoneViewDataTable.row($(e).data('row')).data()

    if (enable == 1) {
        let currentDate = moment().format('YYYY-MM-DD')
        if (!moment(currentDate, 'YYYY-MM-DD').isBetween(moment(row.startDate, 'YYYY-MM-DD'), moment(row.endDate, 'YYYY-MM-DD'), null, [])) {
            // alert(`Require to Edit Start and End Date As The Date Has Passed.`);
            $.alert({
                title: 'Warn',
                content: `Require to Edit Start and End Date As The Date Has Passed.`,
            });
            return;
        }
    }
    axios.post(`/zone/updateNogoZoneStatus`, { nogoZone: { id, enable } }).then(result => {
        if (result.respCode === 1) {
            $.alert('Update no go zone success.')
            initNoGoZoneViewPage()
        } else {
            console.error(result.respMessage)
            $.alert('Update no go zone failed.')
        }
    })
}

const generateWeekDays = function(value) {
    let days = value.split(',')
    days = Array.from(new Set(days))
    let resultDays = []
    for (let day of days) {
        if (day == 1) {
            resultDays.push('Mon')
        } else if (day == 2) {
            resultDays.push('Tue')
        } else if (day == 3) {
            resultDays.push('Wed')
        } else if (day == 4) {
            resultDays.push('Thur')
        } else if (day == 5) {
            resultDays.push('Fri')
        } else if (day == 6) {
            resultDays.push('Sat')
        } else if (day == 7) {
            resultDays.push('Sun')
        }
    }
    let result = `<label style="font-weight: bolder;">${ resultDays.join(', ') }</label>`
    return result
}

const generateWeekTimes = function (value) {
    let times = value.split(',')
    times = Array.from(new Set(times))
    let result = ``
    for (let time of times) {
        result += `<div style="line-height: 1;">${ time }</div>`
    }
    return result
}