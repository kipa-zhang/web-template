
$(function () {
    $('#cancelDeleteVehicle').on('click', function() {
        clearVehicleRelDataList();
    })
    $('#confirmDeleteVehicle').on('click', function() {
        let deactivateVehicleNo = $("#deactivateVehicleNo").val();

        confirmDeleteVehicle(deactivateVehicleNo);
    })
})

const confirmDeleteVehicle = function(vehicleNo) {
    axios.post("/vehicle/deleteVehicle", { vehicleNo: vehicleNo }).then(async res => {
        $.confirm({
            title: res.respCode == 1 ? 'Success Info' : 'Fail Info',
            content: res.respMessage,
            buttons: {
                confirm: {
                    btnClass: 'btn-green',
                    action: function () {
                        clearVehicleRelDataList();
                        if (res.respCode == 1) {
                            table.ajax.reload(null, true)
                        }
                    },
                }
            }
        });
    })
}

const initVehicleTaskList = async function(vehicleNo, effectiveDataList) {
    let taskList = effectiveDataList ? effectiveDataList.taskList : [];
    let hotoList = effectiveDataList ? effectiveDataList.hotoList : [];
    let loanList = effectiveDataList ? effectiveDataList.loanList : [];
    $("#deactivateVehicleNo").val(vehicleNo);
    $(".unassigned-vehcileno-label").text(vehicleNo);
    $('.vehicleTaskContentDiv').empty();
    $('.vehicleTaskContentDiv').append(`
        <div class="row pt-2" style="display: flex; border-bottom: 1px solid #f5f5f5; ">
        <div class="col-3" style="text-align: center;">Task ID</div>
        <div class="col-5" style="text-align: center;">Date Time</div>
        <div class="col-4" style="text-align: center;">Purpose</div>
        </div>
    `);
    if (taskList && taskList.length > 0) {
        for(let temp of taskList) {
        $('.vehicleTaskContentDiv').append(`
            <div class="row py-1" style="display: flex; border-bottom: 1px solid #f5f5f5; font-size: 14px;">
            <div class="col-3" style="text-align: center;">${temp.taskId}</div>
            <div class="col-5" style="text-align: center;">${temp.indentStartTime ? moment(temp.indentStartTime).format("YYYY-MM-DD HH:mm:ss") : ''}</div>
            <div class="col-4" style="text-align: center;">${temp.purpose ?? '-'}</div>
            </div>
        `);
        }
    }
    if (hotoList && hotoList.length > 0) {
        $(".vehicleHotoList").show();
        $('.vehicleHotoContentDiv').empty();
        $('.vehicleHotoContentDiv').append(`
        <div class="row pt-2" style="display: flex; border-bottom: 1px solid #f5f5f5; ">
            <div class="col-3" style="text-align: center;">HOTO ID</div>
            <div class="col-5" style="text-align: center;">Date Time</div>
            <div class="col-4" style="text-align: center;">Purpose</div>
        </div>
        `);
        for(let temp of hotoList) {
        $('.vehicleHotoContentDiv').append(`
            <div class="row py-1" style="display: flex; border-bottom: 1px solid #f5f5f5; font-size: 14px;">
            <div class="col-3" style="text-align: center;">${temp.id}</div>
            <div class="col-5" style="text-align: center;">${temp.startDateTime ? moment(temp.startDateTime).format("YYYY-MM-DD HH:mm:ss") : ''}</div>
            <div class="col-4" style="text-align: center;">${temp.purpose ?? '-'}</div>
            </div>
        `);
        }
    }

    if (loanList && loanList.length > 0) {
        $(".vehicleLoanList").show();
        $('.vehicleLoanContentDiv').empty();
        $('.vehicleLoanContentDiv').append(`
        <div class="row pt-2" style="display: flex; border-bottom: 1px solid #f5f5f5; ">
            <div class="col-3" style="text-align: center;">LOAN ID</div>
            <div class="col-5" style="text-align: center;">Date Time</div>
            <div class="col-4" style="text-align: center;">Purpose</div>
        </div>
        `);
        for(let temp of loanList) {
        $('.vehicleLoanContentDiv').append(`
            <div class="row py-1" style="display: flex; border-bottom: 1px solid #f5f5f5; font-size: 14px;">
            <div class="col-3" style="text-align: center;">${temp.id}</div>
            <div class="col-5" style="text-align: center;">${temp.startDate ? moment(temp.startDate).format("YYYY-MM-DD HH:mm:ss") : ''}</div>
            <div class="col-4" style="text-align: center;">${temp.purpose ?? '-'}</div>
            </div>
        `);
        }
    }
}

const clearVehicleRelDataList = function() {
    $("#deactivateVehicleNo").val('');
    $('.vehicleTaskContentDiv').empty();

    $(".vehicleHotoList").hide();
    $('.vehicleHotoContentDiv').empty();

    $(".vehicleLoanList").hide();
    $('.vehicleLoanContentDiv').empty();

    $('#vehicle-task-info').modal('hide');
}