
$(function () {
  $('#cancelDeleteDriver').on('click', function() {
    clearDriverRelDataList();
  })
  $('#confirmDeleteDriver').on('click', function() {
    let deactivateDriverId = $("#deactivateDriverId").val();

    confirmDeleteDriver(deactivateDriverId);
  })
})

const confirmDeleteDriver = function(driverId) {
  axios.post("/driver/deleteDriver", { driverId: driverId }).then(async res => {
      $.confirm({
          title: res.respCode == 1 ? 'Success Info' : 'Fail Info',
          content: res.respMessage,
          buttons: {
              confirm: {
                btnClass: 'btn-green',
                action: function () {
                  clearDriverRelDataList();
                  if (res.respCode == 1) {
                      table.ajax.reload(null, true)
                  }
                }
              }
          }
      });
  })
}

const initDriverTaskList = async function(driverId, driverName, effectiveDataList) {
  let taskList = effectiveDataList ? effectiveDataList.taskList : [];
  let hotoList = effectiveDataList ? effectiveDataList.hotoList : [];
  let loanList = effectiveDataList ? effectiveDataList.loanList : [];
  $("#deactivateDriverId").val(driverId);
  $(".unassigned-driver-name-label").text(driverName);
  $('.driverTaskContentDiv').empty();
  $('.driverTaskContentDiv').append(`
    <div class="row pt-2" style="display: flex; border-bottom: 1px solid #f5f5f5; ">
      <div class="col-3" style="text-align: center;">Task ID</div>
      <div class="col-5" style="text-align: center;">Date Time</div>
      <div class="col-4" style="text-align: center;">Purpose</div>
    </div>
  `);
  if (taskList && taskList.length > 0) {
    for(let temp of taskList) {
      $('.driverTaskContentDiv').append(`
        <div class="row py-1" style="display: flex; border-bottom: 1px solid #f5f5f5; font-size: 14px;">
          <div class="col-3" style="text-align: center;">${temp.taskId}</div>
          <div class="col-5" style="text-align: center;">${temp.indentStartTime ? moment(temp.indentStartTime).format("DD/MM/YYYY HH:mm:ss") : ''}</div>
          <div class="col-4" style="text-align: center;">${temp.purpose ?? '-'}</div>
        </div>
      `);
    }
  }
  if (hotoList && hotoList.length > 0) {
    $(".driverHotoList").show();
    $('.driverHotoContentDiv').empty();
    $('.driverHotoContentDiv').append(`
      <div class="row pt-2" style="display: flex; border-bottom: 1px solid #f5f5f5; ">
        <div class="col-3" style="text-align: center;">HOTO ID</div>
        <div class="col-5" style="text-align: center;">Date Time</div>
        <div class="col-4" style="text-align: center;">Purpose</div>
      </div>
    `);
    for(let temp of hotoList) {
      $('.driverHotoContentDiv').append(`
        <div class="row py-1" style="display: flex; border-bottom: 1px solid #f5f5f5; font-size: 14px;">
          <div class="col-3" style="text-align: center;">${temp.id}</div>
          <div class="col-5" style="text-align: center;">${temp.startDateTime ? moment(temp.startDateTime).format("DD/MM/YYYY HH:mm:ss") : ''}</div>
          <div class="col-4" style="text-align: center;">${temp.purpose ?? '-'}</div>
        </div>
      `);
    }
  }

  if (loanList && loanList.length > 0) {
    $(".driverLoanList").show();
    $('.driverLoanContentDiv').empty();
    $('.driverLoanContentDiv').append(`
      <div class="row pt-2" style="display: flex; border-bottom: 1px solid #f5f5f5; ">
        <div class="col-3" style="text-align: center;">LOAN ID</div>
        <div class="col-5" style="text-align: center;">Date Time</div>
        <div class="col-4" style="text-align: center;">Purpose</div>
      </div>
    `);
    for(let temp of loanList) {
      $('.driverLoanContentDiv').append(`
        <div class="row py-1" style="display: flex; border-bottom: 1px solid #f5f5f5; font-size: 14px;">
          <div class="col-3" style="text-align: center;">${temp.id}</div>
          <div class="col-5" style="text-align: center;">${temp.startDate ? moment(temp.startDate).format("DD/MM/YYYY HH:mm:ss") : ''}</div>
          <div class="col-4" style="text-align: center;">${temp.purpose ?? '-'}</div>
        </div>
      `);
    }
  }
}

const clearDriverRelDataList = function() {
  $("#deactivateDriverId").val('');
  $('.driverTaskContentDiv').empty();

  $(".driverHotoList").hide();
  $('.driverHotoContentDiv').empty();

  $(".driverLoanList").hide();
  $('.driverLoanContentDiv').empty();

  $('#driver-task-info').modal('hide');
}