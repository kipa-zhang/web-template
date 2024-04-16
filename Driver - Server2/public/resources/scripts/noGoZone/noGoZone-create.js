import * as MapUtil from '../common-map.js'
import { initNoGoZoneViewPage } from '../noGoZone/noGoZone-view.js'

let lineColor = '#F3141D'

let markerList = [], markerPointList = []
let polygon;
let currentZone;

$(function () {
    
})

export async function initNoGoZoneCreatePage (zone) {
    const initColorPickerEventHandler = function (el) {
        $('.edit-noGoZone .color-picker-item').removeClass('active');
        $(el).addClass('active');
        lineColor = $(el).data('color')
        redrawZone()
    }

    // show view noGoZone module
    $('.edit-noGoZone .color-picker-item').off('click').on('click', function () { initColorPickerEventHandler(this) })
    $('input[type="color"]').off('change').on('change', function () { 
        lineColor = $(this).val()
        redrawZone()
    })
    $('input[type="color"]').off('click').on('click', function () { 
        lineColor = $(this).val()
        redrawZone()
    })

    $('.cancel-createZone, .createZone, .btn-close').off('click').on('click', function () { 
        endCreatePoint()
        clearZone()
        $('.zoneName').val(null)
    })
    $('.createZone').off('click').on('click', function () { 
        createNoGoZoneHandler()
    })

    $('.select-alert').off('click').on('click', function () {
        if($(this).prop('checked')) {
            $('.alert-container').addClass('expand')
            $('.alertStartTime').removeAttr('disabled')
            $('.alertEndTime').removeAttr('disabled')
        } else {
            $('.alert-container').removeClass('expand')
            $('.alertStartTime').attr('disabled', 'disabled')
            $('.alertEndTime').attr('disabled', 'disabled')
        }
        $('.alertStartTime').val(null)
        $('.alertEndTime').val(null)
    })
    $('.selectedWeeks .weeks').off('click').on('click', function () {
        if ($(this).hasClass('active')) {
            $(this).removeClass('active')
        } else {
            $(this).addClass('active')
        }
    })

    $('.reDrawPolygon').off('click').on('click', function () {
        if (currentZone) {
            let currentZoneId = currentZone.id
            clearZone()
            currentZone = {}
            currentZone.id = currentZoneId
        } else {
            clearZone()
        }
    })

    layui.use('laydate', function () {
        let laydate = layui.laydate;
        laydate.render({
            elem: '#addTime',
            type: 'time',
            range: true,
            lang: 'en',
            format: 'HH:mm',
            change: (value) => { 
                if (!value) return;
            },
            done: function (value) {
                if (!value) return
                
                console.log(value)
                $('.selectedTimes').append(`
                    <div class="row mx-3 mt-1">
                        <div class="col-10 text-center timezone" style="background-color: #575757; border-radius: 5px;">${ value }</div>
                        <div class="col-2 text-center">
                            <button class="btn btn-success btn-sm minusTime" style="background-color: #1B9063; padding: 2px 8px;">-</button>
                        </div>
                    </div>
                `)
                initMinusBtn();
            }
          });

        laydate.render({
            elem: '#selectedDate',
            lang: 'en',
            format: 'dd/MM/yyyy',
            range: ['#startDate', '#endDate']
        });
    })
    $('.addTime').off('click').on('click', function () {
        $('#addTime').trigger('click')
    })

    endCreatePoint()
    clearZone()
    startCreatePoint(zone);
    initAlertTime(zone)
    if (zone) {
        $('.edit-noGoZone .offcanvas-title').html(`Edit No Go Zone`)
        $('.edit-label').show();

    } else {
        $('.edit-label').hide();
        $('.zoneName').val(null)
        $('.edit-noGoZone .offcanvas-title').html(`Create No Go Zone`)

        $('.select-alert').prop('checked', false)
        $('.alertStartTime').val(null)
        $('.alertEndTime').val(null)

    }

    if (zone) {
        currentZone = zone;
        $('.edit-noGoZone .zoneName').val(zone.zoneName)
        markerPointList = zone.polygon
        lineColor = zone.color
        //draw marker
        markerPointList.forEach(item => {
            markerList.push(MapUtil.drawMarkerTop(item, { iconUrl: "../images/circle.png", iconSize: [15, 15] }))
        })

        // draw polygon
        redrawZone()
    } else {
        
    }
}

const createNoGoZoneHandler = async function () {
    const checkNoGoZoneName = async function (zoneName) {
        let zoneList = await axios.post(`/zone/checkNogoZoneName`, { zoneName, id: currentZone ? currentZone.id : undefined }).then(result => {
            if (result.respCode === 1) {
                return result.respMessage
            } else {
                console.error(result.respMessage)
                return null
            }
        }) 
        if (!zoneList) {
            return false
        } else if (zoneList.length > 1) {
            // at least more than one
            return false
        } else if (zoneList.length == 1) {
            if (currentZone) {
                // if update?
                if (zoneList[0].id !== currentZone.id) {
                    return false
                }
            } else {
                // not update
                return false
            }
        } 
        return true
    }
    const createNoGoZone = async function (zone) {
        return await axios.post(`/zone/createNogoZone`, { nogoZone: zone }).then(result => {
            if (result.respCode === 1) {
                initNoGoZoneViewPage();
                $('.cancel-createZone').trigger('click')
            } else {
                $.alert('Create no go zone failed');
                console.error(result.respMessage)
            }
        })
    }
    const updateNoGoZone = async function (zone) {
        return await axios.post(`/zone/updateNogoZone`, { nogoZone: zone }).then(result => {
            if (result.respCode === 1) {
                initNoGoZoneViewPage();
                $('.cancel-createZone').trigger('click')
            } else {
                $.alert('Update no go zone failed');
                console.error(result.respMessage)
            }
        })
    }

    let zoneName = $('.zoneName').val();
    if (!zoneName) {
        $.alert(`Please Enter No Go Zone Name.`)
        return
    }
    let checkResult = await checkNoGoZoneName(zoneName)
    if (checkResult) {
        let zone = {
            zoneName,
            color: lineColor,
            polygon: markerPointList,
        }

        let alertType = $('.select-alert').prop('checked')
        if (alertType) {
            zone.alertType = 1
            let selectedWeeks = getSelectedWeeks();
            if (!selectedWeeks.length) {
                $.alert(`Please select week.`)
                return
            }
            zone.selectedWeeks = selectedWeeks

            let selectedTimes = getSelectedTimes();
            if (!selectedTimes.length) {
                $.alert(`Please select time.`)
                return
            }
            zone.selectedTimes = selectedTimes

            let startDate = $('#startDate').val()
            let endDate = $('#endDate').val()
            if (!startDate || !endDate) {
                $.alert(`Please select date.`)
                return
            }
            zone.startDate = moment(startDate, 'DD/MM/YYYY').format('YYYY-MM-DD')
            zone.endDate = moment(endDate, 'DD/MM/YYYY').format('YYYY-MM-DD')

        } else {
            zone.alertType = 0
            zone.selectedWeeks = null;
            zone.selectedTimes = null;
        }

        if (currentZone) {
            zone.id = currentZone.id
            await updateNoGoZone(zone)
        } else {
            await createNoGoZone(zone)
        }
    } else {
        $.alert(`Zone name already exist.`)
    }
}

const initAlertTime = function (zone) {
    $('.selectedWeeks .weeks').removeClass('active')
    $('.selectedTimes').empty();

    if (zone && zone.alertType) {
        $('.select-alert').prop('checked', zone.alertType ? true : false)
        $('.alert-container').addClass('expand')
        if (zone.selectedWeeks) {
            let weeks = zone.selectedWeeks.split(',')
            weeks.forEach(item => {
                $(`.selectedWeeks .weeks[data-value=${ item }]`).addClass('active')
            })
        }
        if (zone.selectedTimes) {
            zone.selectedTimes.split(',').forEach(item => {
                $('.selectedTimes').append(`
                    <div class="row mx-3 mt-1">
                        <div class="col-10 text-center timezone" style="background-color: #575757; border-radius: 5px;">${ item }</div>
                        <div class="col-2 text-center">
                            <button class="btn btn-success btn-sm minusTime" style="background-color: #1B9063; padding: 2px 8px;">-</button>
                        </div>
                    </div>
                `)
                initMinusBtn();
            })
        }

        if (zone.startDate) {
            $('#startDate').val(moment(zone.startDate).format('DD/MM/YYYY'))
        }
        if (zone.endDate) {
            $('#endDate').val(moment(zone.endDate).format('DD/MM/YYYY'))
        }
    } else {
        $('.select-alert').prop('checked', false)
        $('.alert-container').removeClass('expand')
    }
}

// ************* By left click menu

const startCreatePoint = function (zone) {
    MapUtil.initMapClickEvent(function(position) {
        // console.log(position)
        let wayPoint = position;

        if (polygon) MapUtil.removeMapObject(polygon)

        markerPointList.push(wayPoint);
        markerList.push(MapUtil.drawMarkerTop(position, { iconUrl: "../images/circle.png", iconSize: [15, 15] }))
        redrawZone()
    })
};
const endCreatePoint = function () {
    MapUtil.cancelMapClickEvent()
};

const redrawZone = function () {
    if (polygon) MapUtil.removeMapObject(polygon)
    polygon = MapUtil.drawPolygon(markerPointList, { color: lineColor, weight: 3 })
}

const clearZone = function () {
    if (polygon) MapUtil.removeMapObject(polygon)
    markerList.forEach(item => MapUtil.removeMapObject(item))
    markerList = []
    markerPointList = []
    polygon = null
    currentZone = null
}

const initMinusBtn = function () {
    $('.selectedTimes .minusTime').off('click').on('click', function () {
        $(this).closest('.row').remove();
    })
}

const getSelectedWeeks = function () {
    let weeks = []
    $(`.selectedWeeks .weeks.active`).each(function () {
        let week = $(this).data('value')
        weeks.push(week)
    })
    console.log(`Selected Weeks => `, weeks)
    return weeks
}

const getSelectedTimes = function () {
    let times = []
    $('.selectedTimes .timezone').each(function () {
        times.push($(this).html())
    })
    console.log(`Selected Times => `, times)
    return times
}
