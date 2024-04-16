const InitExecutionDatePicker = function (elem) {
    let now = moment().format('YYYY-MM-DD')
    $(`#${elem}`).DatePicker({
        flat: true,
        format: 'Y-m-d',
        date: [now, now],
        current: now,
        calendars: 2,
        mode: 'range',
        starts: 0,
        onChange: function (formated) {
            $(`.${elem}-div>input`).val(formated.join(' ~ '))
            $(`#${elem}`).find('.layui-select-btn').removeClass('btn-success-number')
            // if (formated[0] != formated[1]) {
            //     FilterOnChange()
            // }
        }
    });

    $(`.${elem}-div>input`).bind('click', function () {
        $(`#${elem} div.datepicker`).css('display', 'block');
        $(`#${elem}`).focus()
        return false;
    });
    $(`#${elem} div.datepicker`).css('position', 'absolute');
    $(`#${elem} div.datepicker`).css('display', 'none');

    LayDateReady(elem)

    $(`#${elem} .datepickerCancel`).bind('click', function () {
        $(`.${elem}-div>input`).val("")
        $(`#${elem} div.datepicker`).css('display', 'none');
        FilterOnChange()
        return false;
    });
    $(`#${elem} .datepickerDone`).bind('click', function () {
        let d = $(`#${elem}`).DatePickerGetDate()
        let startDate = moment(d[0]).format("YYYY-MM-DD")
        let endDate = moment(d[1]).format("YYYY-MM-DD")
        $(`.${elem}-div>input`).val(`${startDate} ~ ${endDate}`)
        $(`#${elem} div.datepicker`).css('display', 'none');
        FilterOnChange()
        return false;
    });

    $(`#${elem}`).bind('click', function (e) {
        stopPropagation(e);
    });
}

const LayDateReady = function (elem) {
    $(`#${elem} .datepickerContainer`).append(`
        <div class="row align-items-center" style="margin: 12px 10px">
            <div class="col-auto">
            <label>or choose: </label>
            </div>
            <div class="col-auto">
            <button type="button" class="btn btn-secondary btn-sm layui-select-btn rounded-pill" onclick="changeLayDate(10,'${elem}',this)">Next 10 Days</button>
            </div>
            <div class="col-auto" >
            <button type="button" class="btn btn-secondary btn-sm layui-select-btn rounded-pill" onclick="changeLayDate(30,'${elem}',this)">Next 1 month</button>
            </div>
            <div class="col-auto" >
            <button type="button" class="btn btn-secondary btn-sm layui-select-btn rounded-pill" onclick="changeLayDate(365,'${elem}',this)">Next 1 year</button>
            </div>
        </div>

        <div class="datepickerBtn" style="margin-top: 10px"><button class="datepickerCancel">Cancel</button><button class="datepickerDone">Done</button></div>
    `)
}


const changeLayDate = function (nextDays, elem, e) {
    let start = moment().format("YYYY-MM-DD")
    let end = moment().add(nextDays, 'd').format("YYYY-MM-DD")
    $(`#${elem}`).DatePickerSetDate([start, end])
    $(`#${elem} .datepickerDone`).click()
    $(e).parent().parent().find('button').removeClass('btn-success-number')
    $(e).addClass('btn-success-number')
}

function stopPropagation(e) {
    if (e.stopPropagation)
        e.stopPropagation();
    else
        e.cancelBubble = true;
}

$(document).bind('click', function () {
    $(`#executionDate`).DatePickerHide();
    $(`#aviDate`).DatePickerHide();
    $(`#uqmDate`).DatePickerHide();
    $(`#wptDate`).DatePickerHide();
});



InitExecutionDatePicker('executionDate')
InitExecutionDatePicker('aviDate')
InitExecutionDatePicker('uqmDate')
InitExecutionDatePicker('wptDate')

$(".aviDate-div").parent().css("display", "none")
$(".uqmDate-div").parent().css("display", "none")
$(".wptDate-div").parent().css("display", "none")