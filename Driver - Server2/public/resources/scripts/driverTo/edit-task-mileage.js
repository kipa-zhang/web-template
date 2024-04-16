
$(function () {
    $('#editTaskMileageCancel').on('click', function() {
        clearTaskMileageData();
        $('#edit-task-mileage-div').modal('hide');
    })

    $('#editTaskMileageConfirm').on('click', function() {
        updateTaskMileage();
    })
})

const editTaskMileageInfo = async function(taskId, startMileage, endMileage) {
    $('#editTaskId').val(taskId);
    $('.startMileage-input').val(startMileage)
    $('.endMileage-input').val(endMileage)

    $('#edit-task-mileage-div').modal('show');
}

window.updateTaskMileageStatus = async function (taskId, status) {
    axios.post("/driver/updateTaskMileageStatus", {
        taskId: taskId,
        status: status,
    }).then(res => {
        let respCode = res.respCode != null ? res.respCode : res.data.respCode
        let msg = res.respMessage != null ? res.respMessage : res.data.respMessage
        if (respCode == 1)  {
            $.alert({
                title: 'Info',
                content: msg,
            });
            if (typeof driverAssignedTaskTable != "undefined" && driverAssignedTaskTable) {
            driverAssignedTaskTable.ajax.reload(null, true)
            }
            if (typeof table != "undefined" && table) {
            table.ajax.reload(null, true)
            }
        } else {
            $.alert({
                title: 'Error',
                content: msg,
            });
        }
    })
}

const updateTaskMileage = async function() {
    let taskId = $('#editTaskId').val();
    let startMileage = $('.startMileage-input').val()
    let endMileage = $('.endMileage-input').val()

    axios.post("/driver/updateTaskMileageInfo", {
        taskId: taskId,
        startMileage: startMileage,
        endMileage: endMileage,
        resourceType: resourceType
    }).then(async res => {
        let respCode = res.respCode != null ? res.respCode : res.data.respCode
        if (respCode == 1)  {
            clearTaskMileageData();
            $('#edit-task-mileage-div').modal('hide');
            if (typeof driverAssignedTaskTable != "undefined" && driverAssignedTaskTable) {
            driverAssignedTaskTable.ajax.reload(null, true)
            }
            if (typeof table != "undefined" && table) {
            table.ajax.reload(null, true)
            }
        } else {
            let msg = res.respMessage != null ? res.respMessage : res.data.respMessage
            $.alert({
                title: 'Error',
                content: msg,
            });
        }
    });
}

const clearTaskMileageData = function () {
    $('.startMileage-input').val('')
    $('.endMileage-input').val('')
}