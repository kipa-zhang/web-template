
$(function () {
  $('#confirmCloseDriverPermitLog').off('click').on('click', function() {
    $('#driver-permit-log').modal('hide');
  });
})

const addPermitLog = function() {
  $(".last-log-div").hide();
  $(".logs-div").hide();
  $(".permit-remarks-input").val('');
  $(".permit-remarks-input").removeAttr("disabled");

  $('#confirmAddDriverPermitLog').show();
  $('#confirmCloseDriverPermitLog').hide();

  $('#confirmAddDriverPermitLog').off('click').on('click', function() {
    let remarks = $(".permit-remarks-input").val();
    let reason = $("#driver-permitInvalidReason-select").val();
    axios.post("/driver/addDriverPermitLog", { driverId: currentEditDriverId, remarks, reason }).then(async res => {
      if (res.data.respCode == 1) {
        $(".last-log-div").show();
        $(".logs-div").show();

        loadPermitRemarksList();

        $('#confirmAddDriverPermitLog').hide();
        $('#confirmCloseDriverPermitLog').show();
      }
    })
  });
}

const showPermitRemarks = async function() {
  $('#driver-permit-log').modal('show');
  $(".last-log-div").show();
  $(".logs-div").show();
  $(".permit-remarks-input").val('');
  $(".permit-remarks-input").attr("disabled", "disabled");

  $('#confirmAddDriverPermitLog').hide();
  $('#confirmCloseDriverPermitLog').show();

  let driverCurrentPermitStatus = $(".driverCurrentPermitStatus").text();
  if (driverCurrentPermitStatus == 'valid') {
    $(".addpermitlog-label").hide();
  } else {
    $(".addpermitlog-label").show();
  }

  loadPermitRemarksList();
}

const loadPermitRemarksList = function() {
  $(".permit-remarks-input").attr("disabled", "disabled");
  $('.driverRemarksLogContentDiv').empty();
  $('.driverRemarksLogContentDiv').append(`
    <div class="row pt-2" style="display: flex; border-bottom: 1px solid #f5f5f5; ">
      <div class="col-3" style="text-align: center;">Reason</div>
      <div class="col-3" style="text-align: center;">Entered By</div>
      <div class="col-3" style="text-align: center;">Date Time</div>
      <div class="col-3" style="text-align: center;">Remarks</div>
    </div>
  `);
  axios.post("/driver/getDriverPermitLogs", { driverId: currentEditDriverId }).then(async res => {
    if (res.data.respCode == 1) {
      let logs = res.data.respMessage;
      if (logs && logs.length > 0) {
        let lastLog = logs[0];
        $(".remark-by-label").text(lastLog.createName);
        $(".remark-time-label").text(lastLog.optTime ? moment(lastLog.optTime).format("DD/MM/YYYY HH:mm:ss") : '');
        $(".permit-remarks-input").val(lastLog.remarks);
        for(let temp of logs) {
          $('.driverRemarksLogContentDiv').append(`
            <div class="row py-1" style="display: flex; border-bottom: 1px solid #f5f5f5; font-size: 14px;">
              <div class="col-3" style="text-align: center; line-height: normal;">${temp.reason ?? '-'}</div>
              <div class="col-3" style="text-align: center; line-height: normal;">${temp.createName}</div>
              <div class="col-3" style="text-align: center; line-height: normal;">${temp.optTime ? moment(temp.optTime).format("DD/MM/YYYY HH:mm:ss") : ''}</div>
              <div class="col-3" style="text-align: center; line-height: normal;">${temp.remarks ?? '-'}</div>
            </div>
          `);
        }
      }
    }
  })
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