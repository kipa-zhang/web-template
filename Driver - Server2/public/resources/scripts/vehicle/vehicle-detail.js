
let currentVehicleNo = '';
let vehicleWptStartTime = null, vehicleWptEndTime = null, vehicleMptStartTime = null, vehicleMptEndTime = null, vehiclePmTime = null, vehicleAviTime = null;

let vehicleStatusColor = [{status: 'Deployed', color: '#FAD028'}, {status: 'Loan Out', color: 'blue'}, {status: 'Deployable', color: '#6EB825'}, 
{status: 'Under Maintenance', color: '#f705ce'}, {status: 'Out Of Service', color: '#3e3b3b'}, {status: 'On Hold', color: 'red'}]

$(() => {
    setTimeout(() => {
        $('.layui-vehicle-tab>ul>li:first').trigger('click')
    }, 500)

    currentVehicleNo = getParams("vehicleNo");
    $(".vehicleNo-label").html(currentVehicleNo);

    $(".edit-content-btn-div").on('click', function() {
        initVehicleEditPage('edit', $(".vehicle-no").text());
    })

    initBasicProfileHandler();
    layui.use('element', function(){
        let chatTabElement = layui.element;
        $('.user-select-none').on('click', function(title) {
            let layId = this.getAttribute('lay-id');
            if (layId === 'page-1') {
                initBasicProfileHandler();
            } else if (layId === 'page-2') {
                initVehicleIndentAssignedHandler();
            } else if (layId === 'page-3') {
                initMaintenanceHandler();
            } else if (layId === 'page-4') {
                initOddHandler();
            } else if (layId === 'page-5') {
                initVehicleScheduleHandler();
            }
        });
    });

    //fixme: layui tab default load tab1;
    setTimeout(function () {
        let defaultTab = getParams("defaultTab");
        if (defaultTab && defaultTab == 'indents') {
            $('.layui-tab-title li').removeClass('layui-this');
            $('.layui-tab-title .lay-indent').addClass('layui-this');
            $('.layui-tab-item').removeClass('layui-show');
            $('.layui-tab-item.layui-tab-item-indent').addClass('layui-show');

            initVehicleIndentAssignedHandler(1);
        }
    }, 500)
})

const initBasicProfileHandler = async function () {
    const initPageData = function(vehicleNo) {
        axios.post("/vehicle/getVehicleDetail", {vehicleNo: vehicleNo }).then(async res => {
            if (res.data.respCode == -100) {
                window.location = '../login'
            } else if (res.data.respCode == 0) {
                $.confirm({
                    title: 'WARN',
                    content: `Data is error, please refresh the page.`,
                    buttons: {
                        ok: function () {
                        },
                    }
                });
            }

            let vehicleDetail = res.data.respMessage.currentVehicle;

            vehicleWptEndTime = vehicleDetail.nextWpt1Time; 
            vehicleWptStartTime = vehicleWptEndTime ? moment(vehicleWptEndTime).add(-6, 'd').format("YYYY-MM-DD") : null;
            vehicleMptEndTime = vehicleDetail.nextMptTime; 
            vehicleMptStartTime = vehicleMptEndTime ? moment(vehicleMptEndTime).add(-20, 'd').format("YYYY-MM-DD") : null;
            vehiclePmTime = vehicleDetail.nextPmTime; 
            vehicleAviTime = vehicleDetail.nextAviTime; 

            let maintenanceOperation = vehicleDetail.maintenanceOperation
            let maintenanceOperationList = maintenanceOperation ? maintenanceOperation.split(',') : []
            if (vehicleDetail.onhold == 1 && maintenanceOperationList && maintenanceOperationList.includes('Release')) {
                $(".vehicle-release-btn").show();
            } else {
                $(".vehicle-release-btn").hide();
            }

            let assignedTaskNum = res.data.respMessage.assignedTaskNum;
            let vehicleOddNum = res.data.respMessage.vehicleOddNum;
            let vehicleSchedule = res.data.respMessage.vehicleSchedule;
            let scheduleCount = res.data.respMessage.scheduleCount;
            $(".vehicleType-label").text(vehicleDetail.vehicleType);
            $(".vehicle-subunit").text(vehicleDetail.subUnit ? vehicleDetail.subUnit : '-');
            $(".vehicle-unit").text(vehicleDetail.unit);
            $(".vehicle-mileage").text(vehicleDetail.totalMileage + " km");
            $("#vehicle-status").text(vehicleDetail.currentStatus);

            let statusColor = vehicleStatusColor.find(item => item.status == vehicleDetail.currentStatus)
            if (!statusColor) {
                statusColor = 'orange';
            } else {
                statusColor = statusColor.color;
            }
            $(".status-label").css('background-color', statusColor);
            
            $(".vehicle-no").text(vehicleDetail.vehicleNo);
            $(".vehicle-type").text(vehicleDetail.vehicleType);
            $(".vehicle-dimensions").text(vehicleDetail.dimensions ? vehicleDetail.dimensions : '-');
            $(".vehicle-speedlimit").text(vehicleDetail.limitSpeed ? vehicleDetail.limitSpeed + ' KM/hr' : '-');
            $(".avi-date").text(vehicleDetail.nextAviTime ? moment(vehicleDetail.nextAviTime, 'YYYY-MM-DD').format('DD/MM/YYYY') : '-');
            $(".vehicle-keyTagId").text(vehicleDetail.keyTagId ? vehicleDetail.keyTagId : '-');
            
            $(".maintenance-expire-warning").text(vehicleDetail.maintenanceWarningStr);

            $(".indentAssignedCount").text(assignedTaskNum);
            $(".oddCount").text(vehicleOddNum);
            
            $(".vehicleSchedule").text(vehicleSchedule);

            $('.vehicleSchedule').text(scheduleCount)

            if (Cookies.get('userType') == 'HQ') {
                $('.edit-driver').show();
                $('.hoto-info').hide()
            } else {
                if (vehicleDetail.hotoId) {
                    $('.edit-content-btn-div').hide();
                    $('.hoto-info').show()
                    $('.loan-info').hide()
                } else if (vehicleDetail.loanId) {
                    $('.edit-content-btn-div').hide();
                    $('.hoto-info').hide()
                    $('.loan-info').show()
                } else {
                    $('.edit-content-btn-div').show();
                    $('.loan-info').hide()
                    $('.hoto-info').hide()
                }
                // if(Cookies.get('userType').toLowerCase() == 'customer') { 
                //     $('.edit-content-btn-div').hide();
                // }
            }
        })
    };
    initPageData(currentVehicleNo);
}

const initDrivingRecordHandler = async function () {
    console.log('initDrivingRecordHandler...')
}

const initVehicleIndentAssignedHandler = async function (showMileageWarning) {
    console.log('initIndentAssignedHandler...')
    
    initVehicleAssignedIndent(showMileageWarning);
}

const initMaintenanceHandler = async function () {
    initMaintenance();
}

const initOddHandler = async function () {
    initOdd();
}

const initVehicleScheduleHandler = async function () {
    console.log('initVehicleScheduleHandler...')
    
    initVehicleSchedule();
}

const getParams = function(key) {
    let reg = new RegExp("(^|&)" + key + "=([^&]*)(&|$)");
    let r = window.location.search.substr(1).match(reg);
    if (r != null) {
        return unescape(r[2]);
    }
    return null;
};