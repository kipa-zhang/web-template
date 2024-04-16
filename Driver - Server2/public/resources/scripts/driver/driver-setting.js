$(function () {
    
})

export async function initDriverSettingPage () {
    const initLeaveTime = function () {
        layui.use('laydate', function() {
            let laydate = layui.laydate;
            laydate.render({
                elem: '#view-driver-setting .leave-time',
                type: 'datetime',
                format: 'dd/MM/yyyy HH:mm:ss',
                lang: 'en',
                show: false,
                done: function (time) {
                    console.log('Time Changed : ', time);
                    // check time
                    if (moment().valueOf() > moment(time, 'DD/MM/YYYY HH:mm:ss').add(1, 'm').valueOf()) {
                        console.log('Leave Time is not correct.');
                    }
                }
            });
        });
        // set default value
        $('#view-driver-setting .leave-time').html(moment().format('DD/MM/YYYY HH:mm:ss'));
    }

    const initSpeed = function () {
        $('#view-driver-setting .speed').val(60);
    }

    initLeaveTime();
    initSpeed()

    // show view driver setting module
    $('#view-driver-setting').modal('show');
}

