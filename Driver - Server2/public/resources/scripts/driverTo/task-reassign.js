let currentReassignVehicleNo='';
let currentReassignDriverId='';
let currentTaskId = '';
let currentResourceType = 'driver'
$(function() {
    $('#driver-task-edit .close-img').on('click', function(){
        currentReassignVehicleNo = ''
        currentReassignDriverId = ''
        $('#driver-task-edit').modal('hide');
        $('.img-select-content').remove();
    });

    $('.driver-reasons .reassignReasonDiv').on('click', function(){
        $('.driver-reasons .reassignReasonDiv').removeClass('active');
        $(this).addClass('active');
    });
    $('.vehicle-reasons .reassignReasonDiv').on('click', function(){
        $('.vehicle-reasons .reassignReasonDiv').removeClass('active');
        $(this).addClass('active');
    });
});

const cancleEditTask = function() {
    currentReassignVehicleNo = ''
    currentReassignDriverId = ''
    $('#driver-task-edit').modal('hide');
    $('.img-select-content').remove();
}

const confirmEditTask = async function() {
    let newVehicleNo = $('.vehicleNo-img-select').attr('value');
    let newDriverId =  $('.driver-img-select').attr('value');
    if (!newVehicleNo) {
        $.alert('Please select Vehicle No.');
        return;
    }
    if (!newDriverId) {
        $.alert('Please select Driver.');
        return;
    }
    if (newVehicleNo == currentReassignVehicleNo && newDriverId == currentReassignDriverId) {
        $.alert('Please reselect Vehicle No. or Transport Operator!');
        return;
    }
    let reason =  '';
    if (currentResourceType == 'driver') {
        reason = $('.driver-reasons .reassignReasonDiv.active label').text();
    } else {
        reason = $('.vehicle-reasons .reassignReasonDiv.active label').text();
    }
    
    let remarks = $('#driver-task-edit #editDriverTaskRemarks').val();
    if(!remarks) {
        $.alert('Remarks is required!');
        return;
    }
    
    await axios.post('/driver/reassignDriverTask',{
        taskId: currentTaskId,
        vehicleNo: newVehicleNo,
        driverId: newDriverId,
        reason: reason,
        remarks: remarks
    }).then(function (res) {
        if (res.data.respCode === 1) {
            currentReassignVehicleNo = ''
            currentReassignDriverId = ''
            $('#driver-task-edit').modal('hide');
            if (currentResourceType == 'driver') {
                initWeekDaysHandler(currentWeek, currentDate.year(), currentDate.month())
            } else {
                initVehicleSchedule();
            }
        } else {
            $.alert(res.data.respMessage);
        }
    });
}

const initTaskEditPage = function(task, resourceType) {
    $('#driver-task-edit #editDriverTaskRemarks').val('');
    $('.vehicleNo-img-select').text('');
    $('.vehicleNo-img-select').attr('value', '');
    $('.driver-img-select').text('');
    $('.driver-img-select').attr('value', '');
    $('.img-select-content').remove();

    currentReassignVehicleNo = task.vehicleNumber
    currentReassignDriverId = task.driverId;
    currentTaskId = task.taskId;
    currentResourceType = resourceType

    $('#driver-task-edit .intentId-label').text('#' + task.indentId);
    $('#driver-task-edit .title-purpose-label').text(task.purpose);
    $('#driver-task-edit .taskExecutionDate').text(moment(task.indentStartTime).format("DD MMM YY"));
    $('#driver-task-edit .serviceModelLabel').text(task.serviceMode ? task.serviceMode : '' + ' Trip');

    $('#driver-task-edit .reporting-loc-label').text(task.pickupDestination);
    $('#driver-task-edit .indentStartTime-label').text(moment(task.indentStartTime).format("DD MMM YY, HH:mm"));
    $('#driver-task-edit .destionation-label').text(task.dropoffDestination);

    if (resourceType == 'vehicle') {
        $('.vehicle-reasons').show();
        $('.driver-reasons').hide();
    } else {
        $('.vehicle-reasons').hide();
        $('.driver-reasons').show();
    }
    initVehicleDriverSelect(task);

    $(".reassign-btn-div").show();
}

const initTaskViewPage = function(task, resourceType) {
    $('#driver-task-edit #editDriverTaskRemarks').val('');
    $('.img-select-content').remove();

    currentReassignVehicleNo = task.vehicleNumber
    currentReassignDriverId = task.driverId;
    currentTaskId = task.taskId;
    currentResourceType = resourceType

    $('#driver-task-edit .intentId-label').text('#' + task.indentId);
    $('#driver-task-edit .title-purpose-label').text(task.purpose);
    $('#driver-task-edit .taskExecutionDate').text(moment(task.indentStartTime).format("DD MMM YY"));
    $('#driver-task-edit .serviceModelLabel').text(task.serviceMode ? task.serviceMode : '' + ' Trip');

    $('#driver-task-edit .reporting-loc-label').text(task.pickupDestination);
    $('#driver-task-edit .indentStartTime-label').text(moment(task.indentStartTime).format("DD MMM YY, HH:mm"));
    $('#driver-task-edit .destionation-label').text(task.dropoffDestination);

    if (resourceType == 'vehicle') {
        $('.vehicle-reasons').show();
        $('.driver-reasons').hide();
    } else {
        $('.vehicle-reasons').hide();
        $('.driver-reasons').show();
    }
    //initVehicleDriverSelect(task);
    $('.vehicleNo-img-select').text(currentReassignVehicleNo);
    $('.vehicleNo-img-select').attr('value', currentReassignVehicleNo);
    $('.driver-img-select').text(task.driverName);
    $('.driver-img-select').attr('value', task.driverName);

    $(".reassign-btn-div").hide();
}

const initVehicleDriverSelect = async function(task) {
    let VehicleDriver = await getDriverAndVehicle(task.taskId, task.vehicleType);
    let driverList = VehicleDriver.driverList;
    let vehicleList = VehicleDriver.vehicleList;

    let driverSelect = $('.driver-img-select').imgSelect({
        searchable: true,
        dataKey: 'driverId',
        dataName: 'driverName',
        defaultValue: task.driverId ? task.driverId : '',
        data: driverList
    });

    let vehicleNoSelect = $('.vehicleNo-img-select').imgSelect({
        searchable: true,
        dataKey: 'vehicleNo',
        dataName: 'vehicleNo',
        defaultValue: task.vehicleNumber ? task.vehicleNumber : '',
        data: vehicleList,
        selectItemCallback: function($vehicleDiv) {
            //refreshDriver($vehicleDiv, driverSelect);
        }
    });
}

const refreshDriver = function($vehicleDiv, driverSelect) {
    let permitType = $vehicleDiv.attr('permitType');

    let $driverSelectEle = driverSelect.getSelectEle();
    $driverSelectEle.find('.img-select-item-div').each(function() {
        if ($(this).hasClass('premitType-class-' + permitType) == true) {
            $(this).show();
        } else {
            $(this).hide();
        }
    });
    $driverSelectEle.find('.img-selected-item-div').each(function() {
        $(this).children().remove();
    });
    $driverSelectEle.find('.img-select-item-div').each(function() {
        $(this).removeClass('active');
    });
    $('.driver-img-select').text('');
    $('.driver-img-select').attr('value', '');
}

const getDriverAndVehicle = async function (taskId, vehicleType) {
    return axios.post('/driver/getDriverAndVehicleForTask', { taskId, vehicleType }).then(function (res) {
        if (res.data.respCode === 1) {
            return res.data.respMessage;
        } else {
            console.error(res.data.respMessage);
            return null;
        }
    });
}