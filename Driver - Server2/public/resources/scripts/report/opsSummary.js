let currentDay = moment().format('DD/MM/YYYY');
let preMonthDay = moment().add(-7, 'day').format('DD/MM/YYYY');
let defaultSelectedDateRange = preMonthDay + ' ~ ' + currentDay;

const arrowUp = `
    <svg t="1710322540477" class="icon" viewBox="0 0 1169 1024" version="1.1" p-id="2256" width="12" height="12">
        <path d="M1128.20097 1002.66625667H41.123004a40.959984 40.959984 0 0 1-35.635185-61.439975L549.026801-0.85334133a40.959984 40.959984 0 0 1 70.860772 0l543.538982 942.079623a40.959984 40.959984 0 0 1-35.225585 61.439975z" fill="#ffffff" p-id="2257"></path>
    </svg>
`
const arrowDown = `
    <svg t="1710322531253" class="icon" viewBox="0 0 1169 1024" version="1.1" p-id="2061" width="12" height="12">
        <path d="M63.49975474 16.0003972L1116.6065343 16.00039719a39.6799845 39.6799845 0 0 1 34.52158546 59.51997578L624.57473096 988.16000775a39.6799845 39.6799845 0 0 1-68.64637288 0l-526.55338881-912.63963478a39.6799845 39.6799845 0 0 1 34.12478547-59.51997577z" fill="#ffffff" p-id="2062"></path>
    </svg>
`
const rectangle = `
    <svg t="1710322796821" class="icon" viewBox="0 0 1024 1024" version="1.1" p-id="4203" width="12" height="12" >
        <path d="M85.333333 512a64 64 0 0 1 64-64h725.333334a64 64 0 0 1 0 128h-725.333334A64 64 0 0 1 85.333333 512z" fill="#ffffff" p-id="4204"></path>
    </svg>
`

$(() => {
    initData().then((data) => initDashboard(data));
    layui.use('laydate', function(){
        let laydate = layui.laydate;
        
        laydate.render({
            elem: '.selectedDate',
            format: 'dd/MM/yyyy',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            value: defaultSelectedDateRange,
            range: '~',
            btns: ['clear', 'confirm'],
            done: async (value) => {
                defaultSelectedDateRange = value
                await initData().then((data) => initDashboard(data))
            }
        });
    })
})

const initData = async function () {
    let startDate = null, endDate = null;
    if (defaultSelectedDateRange) {
        let dateRange = defaultSelectedDateRange.split('~').map(item => item.trim())
        startDate = moment(dateRange[0], 'DD/MM/YYYY').format('YYYY-MM-DD')
        endDate = moment(dateRange[1], 'DD/MM/YYYY').format('YYYY-MM-DD')
    }

    return await axios.post(`/opsSummaryReport`, { startDate, endDate })
        .then(res => {
            return res.respMessage ? res.respMessage : res.data.respMessage;
        })
}

const initDashboard = function (data) {
    if (!data) return

    initTotalDIV(data)
    initHubDIV(data)
}

const initTotalDIV = function (data) {

    $('.total-ops').html(data.selectedCycleResult.opsTaskWorkingDays)
    $('.total-trg').html(data.selectedCycleResult.trgTaskWorkingDays)
    $('.total-adm').html(data.selectedCycleResult.admTaskWorkingDays)

    $('.total').html(data.selectedCycleResult.taskWorkingDays)
    initRateHtml($('.total-range'), data.selectedCycleResult.taskWorkingDays, data.preCycleResult.taskWorkingDays)

    $('.toUnit').html(data.selectedCycleResult.toWorkingRate + '%')
    initRateHtml($('.toUnit-range'), data.selectedCycleResult.toWorkingRate, data.preCycleResult.toWorkingRate)

    $('.total-vel').html(`${ data.selectedCycleResult.vehicleActualWorkingDays }/${ data.selectedCycleResult.vehiclePlanWorkingDays }`)
    initRateHtml($('.total-vel-range'), 
        data.selectedCycleResult.vehicleActualWorkingDays / data.selectedCycleResult.vehiclePlanWorkingDays, 
        data.preCycleResult.vehicleActualWorkingDays / data.preCycleResult.vehiclePlanWorkingDays
    )
}

const initHubDIV = function (data) {
    const getHubHtml = function (data, preData) {
        const getHubTitleHtml = function (data) {
            return `
            <div class="content-top px-2" style="background-color: ${ data.color };">
                <div class="row mx-0">
                    <div class="col-2 hub">${ data.name }</div>
                    <div class="col-6">
                        <div class="row mx-0">
                            <div class="col-7">
                                <div class="row mx-0">
                                    <div class="col-4 mx-0 px-0">Ops</div>
                                    <div class="col-4 mx-0 px-0">Trg</div>
                                    <div class="col-4 mx-0 px-0">Adm</div>
                                </div>
                            </div>
                            <div class="col-5">
                                <div class="row mx-0">
                                    <div class="col-12 mx-0 px-0">Total</div>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="col-4">
                        <div class="row mx-0">
                            <div class="col-6 mx-0 px-0">TO Util</div>
                            <div class="col-6 mx-0 px-0">Veh(Avail/Holding)</div>
                        </div>
                    </div>
                </div>
            </div>
            `
        }
        const getHubTotalHtml = function (data, preData, color) {
            return `
                <div class="content-total text-center px-2 pt-2">
                    <div class="row mx-0">
                        <div class="col-2 fw-bold">
                            <label class="w-100 bg-1">Total</label>
                        </div>
                        <div class="col-6">
                            <div class="row bg-1 mx-0">
                                <div class="col-7">
                                    <div class="row mx-0">
                                        <div class="col-4 mx-0 px-0 fw-bold">${ data.opsTaskWorkingDays }</div>
                                        <div class="col-4 mx-0 px-0 fw-bold">${ data.trgTaskWorkingDays }</div>
                                        <div class="col-4 mx-0 px-0 fw-bold">${ data.admTaskWorkingDays }</div>
                                    </div>
                                </div>
                                <div class="col-5">
                                    <div class="row mx-0">
                                        <div class="col-12 mx-0 px-0">
                                            <label class="fw-bold" style="color: ${ color }; min-width: 30px;">${ data.taskWorkingDays }</label>
                                            ${ getRateHtml(data.taskWorkingDays, preData.taskWorkingDays) }
                                        </div>
                                    </div>
                                </div>
                                
                            </div>
                        </div>
                        <div class="col-4">
                            <div class="row bg-1 mx-0">
                                <div class="col-6 mx-0 px-0">
                                    <label class="fw-bold" style="min-width: 32px;">${ data.toWorkingRate }%</label>
                                    ${ getRateHtml(data.toWorkingRate, preData.toWorkingRate) }
                                </div>
                                <div class="col-6 mx-0 px-0">
                                    <label class="fw-bold" style="min-width: 60px;">${ data.vehicleActualWorkingDays + '/' + data.vehiclePlanWorkingDays }</label>
                                    ${ getRateHtml(data.vehicleActualWorkingDays/data.vehiclePlanWorkingDays, preData.vehicleActualWorkingDays/preData.vehiclePlanWorkingDays) }
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `
        }
        const getHubNodeHtml = function (data, preData, color) {
            const generateHubNodeHtml = function (data, preData, color) {
                return `
                    <div class="content-data text-center px-2 py-1">
                        <div class="row mx-0">
                            <div class="col-2 fw-bold">
                                <label class="w-100 bg-2" style="background-color: ${ getLightColor(color) }; color: ${ color }; ">${ data.name }</label>
                            </div>
                            <div class="col-6">
                                <div class="row bg-2 mx-0">
                                    <div class="col-7">
                                        <div class="row mx-0">
                                            <div class="col-4 mx-0 px-0 fw-bold">${ data.opsTaskWorkingDays }</div>
                                            <div class="col-4 mx-0 px-0 fw-bold">${ data.trgTaskWorkingDays }</div>
                                            <div class="col-4 mx-0 px-0 fw-bold">${ data.admTaskWorkingDays }</div>
                                        </div>
                                    </div>
                                    <div class="col-5">
                                        <div class="row mx-0">
                                            <div class="col-12 mx-0 px-0">
                                                <label class="fw-bold" style="color: ${ color }; min-width: 30px;">${ data.taskWorkingDays }</label>
                                                ${ getRateHtml(data.taskWorkingDays, preData.taskWorkingDays) }
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div class="col-4">
                                <div class="row bg-2 mx-0">
                                    <div class="col-6 mx-0 px-0">
                                        <label class="fw-bold" style="min-width: 32px;">${ data.toWorkingRate }%</label>
                                        ${ getRateHtml(data.toWorkingRate, preData.toWorkingRate) }
                                    </div>
                                    <div class="col-6 mx-0 px-0">
                                        <label class="fw-bold" style="min-width: 60px;">${ data.vehicleActualWorkingDays + '/' + data.vehiclePlanWorkingDays }</label>
                                        ${ getRateHtml(data.vehicleActualWorkingDays/data.vehiclePlanWorkingDays, preData.vehicleActualWorkingDays/preData.vehiclePlanWorkingDays) }
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                `
            }

            let html = ``
            let index = 0;
            for (let nodeData of data.nodeData) {
                html += generateHubNodeHtml(nodeData, preData.nodeData[index], color)
                index++
            }
            return html            
        }

        return `
            <div class="col-12 col-sm-12 col-md-12 col-lg-6">
                <div class="hub-content mt-2" style="height: calc(100% - 8px);">
                    ${ getHubTitleHtml(data) }
                    <div class="content-detail py-1" style="height: calc(100% - 40px);">
                        ${ getHubTotalHtml(data, preData, data.color) }
                        ${ getHubNodeHtml(data, preData, data.color) }
                    </div>
                </div>
            </div>
        `
    }

    $('.hub-container').empty()
    let index = 0;
    for (let hubData of data.selectedCycleResult.hubData) {
        $('.hub-container').append(getHubHtml(hubData, data.preCycleResult.hubData[index]))
        index++;
    }
}

const calculateRate = function (num1 = 0, num2 = 0) {
    if (num2 == 0 && num1 == 0) return 0
    if (num2 == 0 && num1 != 0) return 100
    if (!Number.isFinite(num2)) return 0

    return ((num1 - num2) / num2 * 100).toFixed(1)
}
const initRateHtml = function ($el, num1, num2) {
    let rate = calculateRate(num1, num2)
    $el.html(`${ rate }%`)
    if (rate > 0) {
        $el.parent().closest('label').removeClass().addClass('label-green')
        $el.parent().find('svg').remove()
        $el.parent().prepend(arrowUp)
    } else if (rate < 0) {
        $el.parent().closest('label').removeClass().addClass('label-red')
        $el.parent().find('svg').remove()
        $el.parent().prepend(arrowDown)
    } else {
        $el.parent().closest('label').removeClass().addClass('label-gray')
        $el.parent().find('svg').remove()
        $el.parent().prepend(rectangle)
    }
}
const getRateHtml = function (num1, num2) {
    let rate = calculateRate(num1, num2)
    let labelClass = '', arrowSvg = ''
    if (rate > 0) {
        labelClass = 'label-green'
        arrowSvg = arrowUp
    } else if (rate < 0) {
        labelClass = 'label-red'
        arrowSvg = arrowDown
    } else {
        labelClass = 'label-gray'
        arrowSvg = rectangle
    }

    return `
        <label class="${ labelClass }">
            ${ arrowSvg }
            <label class="data-range">${ rate }%</label>
        </label>
    `
}
const getLightColor = function (hexColor) {
    let r = parseInt(hexColor.slice(1, 3), 16),
    g = parseInt(hexColor.slice(3, 5), 16),
    b = parseInt(hexColor.slice(5, 7), 16);
   
    r = Math.max(0, Math.min(255, r - 10));
    g = Math.max(0, Math.min(255, g - 10));
    b = Math.max(0, Math.min(255, b - 10));
   
    return `rgba(${r}, ${g}, ${b}, 0.1)`; 
}