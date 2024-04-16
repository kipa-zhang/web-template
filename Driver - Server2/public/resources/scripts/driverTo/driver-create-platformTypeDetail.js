$(function () {
  initVehicleClassList();

  $('#driverPermitTypeDetailCancel').on('click', function() {
    clearVehicleClassFormData();
    $('#create-VehicleClass').modal('hide');
  })

  $('.btn-create-vehicleClass').on('click', function() {
    initVehicleClassPage();
    clearVehicleClassFormData();
    $('#create-VehicleClass').modal('show');

    if ($('.can-edit-permitType-mileage').text() != '1') {
      $('.basePermitTypeMileage-input').attr('disabled', 'disabled');
    } else {
      $('.basePermitTypeMileage-input').removeAttr('disabled');
    }
  });

  $("#deletePermitDetailConfirm").off('click').on('click', function() {
      let remarks = $(".delete-remarks-input").val();
      let currentPermitDetailId = $("#currentPermitDetailId").val();

      confirmDeletePermitTypeDetail(currentPermitDetailId, remarks);
  });
  $("#deletePermitDetailCancel").off('click').on('click', function() {
    $("#currentPermitDetailId").val('');
    $(".delete-remarks-input").val('');
    $(".delete-info-div").hide();
    $("#delete-driver-permitTypeDetail-modal").modal('hide');
  });

  $('#driverPermitTypeDetailConfirm').on('click', async function () {
    let driverPermitTypeDetailId = $('#driverPermitTypeDetailId').val();

    let permitType = $('#driver-permitType-select').val();
    let score = 0;
    let demeritPoint = 0;
    if (permitType.toUpperCase().startsWith('CL')) {
      demeritPoint = $('.demerit-point-input').val();
    } else {
      score = $('.score-input').val();
    }
    if (permitType.toUpperCase().endsWith('X')) {
      score = 0;
      demeritPoint = 0;
    }
    let driverPermitTypeDetail = {
          "detailId": driverPermitTypeDetailId,
          "driverId": currentEditDriverId,
          "permitType": $('#driver-permitType-select').val(),
          "passDate": $('.passTime-input').val() ? moment($('.passTime-input').val(), 'DD/MM/YYYY').format("YYYY-MM-DD") : null,
          "attemptNums": $('.attemptNums-input').val(),
          "testerCode": $('.testerCode-input').val(),
          "score": score,
          "demeritPoint": demeritPoint,
          "baseMileage": $('.basePermitTypeMileage-input').val()
      }
    axios.post("/driver/updatePermitTypeDetail", driverPermitTypeDetail).then(async res => {
      let respCode = res.respCode != null ? res.respCode : res.data.respCode
      if (respCode == 1)  {
        $('#create-VehicleClass').modal('hide');
        clearVehicleClassFormData();
        initVehicleClassList();
      } else {
        let msg = res.respMessage != null ? res.respMessage : res.data.respMessage
        $.alert({
            title: 'Error',
            content: msg,
        });
      }
    });
  });
})

const initVehicleClassList = async function() {
  axios.post("/driver/getPermitTypeDetailList", {driverId: currentEditDriverId}).then(async res => {
    let code = res.respCode != null ? res.respCode : res.data.respCode
    if (code == 1)  {
        $('.permitTypeDetailDiv').empty();
        let dataList = res.respMessage != null ? res.respMessage : res.data.respMessage
        if (dataList && dataList.length > 0) {
        for(let temp of dataList) {
          let operationList = temp.operation.split(',')
          let actionHtml = ``;
          if (temp.approveStatus == 'Deleted') {
            actionHtml = `<div style="cursor: pointer; color: blue; text-decoration: underline; " onclick="showDeleteInfo('${temp.deleteUser ? temp.deleteUser : '-'}', '${temp.deleteAt ? moment(temp.deleteAt).format("YYYY-MM-DD HH:mm:ss") : '-'}', '${temp.deleteReason ? temp.deleteReason : '-'}')" role="button" tabindex="0">Deleted</div>`
          } else if (operationList.includes('Edit') || operationList.includes('All')) {
            actionHtml += `
              <div style="color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="edit-driver-permitTypeDetail custom-btn-blue" onclick="editDriverPermitTypeDetail(${temp.id}, '${temp.permitType}', '${temp.passDate}', '${temp.baseMileage ? temp.baseMileage : 0}', '${temp.attemptNums}', '${temp.testerCode}', '${temp.score}', '${temp.demeritPoint}')" role="button" tabindex="0">
                Edit
              </div>
              <div style="color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="delete-driver-permitTypeDetail custom-btn-danger" onclick="deletePermitTypeDetail(${temp.id}, '${temp.permitType}')" role="button" tabindex="0" style="margin-left: 15px;">
                Delete
              </div>
            `;
          }

          // if (!actionHtml) {
          //   actionHtml = temp.approveStatus;
          // }
          $('.permitTypeDetailDiv').append(`
            <div class="py-3" style="display: flex; border-bottom: 1px solid #f5f5f5;">
              <div style="width: calc(100%/7);">${ temp.permitType.indexOf('CL ') > -1 ? `PRT ${ temp.permitType }` : temp.permitType}</div>
              <div style="width: calc(100%/7);">${temp.passDate ? moment(temp.passDate).format("DD/MM/YYYY") : ''}</div>
              <div style="width: calc(100%/7);">${temp.baseMileage ? temp.baseMileage : 0}</div>
              <div style="width: calc(100%/7);">${temp.attemptNums}</div>
              <div style="width: calc(100%/7);">${temp.testerCode}</div>
              <div style="width: calc(100%/7);">${temp.score ?? '-'}</div>
              <div style="width: calc(100%/7);">${temp.demeritPoint ?? '-'}</div>
              <div style="width: calc(100%/7);">
                <div style="display: flex;">
                  ${actionHtml}
                </div>
              </div>
            </div>
          `);
        }
      }
    } else {
        $.confirm({
            title: 'WARN',
            content: `Data is error, please refresh the page.`,
            buttons: {
              ok: {
                btnClass: 'btn-green',
                action: function () {
                }
              }
            }
        });
    }
  });
}

const initVehicleClassPage = async function () {
  layui.use('laydate', function(){
    let laydate = layui.laydate;
    laydate.render({
      elem: '.passTime-input',
      type: 'date',
      lang: 'en',
      format: 'dd/MM/yyyy',
      trigger: 'click',
      max: moment(new Date()).format('DD/MM/YYYY'),
      btns: ['clear', 'confirm'],
      done: (value) => {
      }
    });
  });

  initPermitTypeSelect();
}

const initPermitTypeSelect = async function() {
  await axios.post("/driver/getPermitTypeList").then(async res => {
      let permitTypeList = res.data.respMessage;
      $("#driver-permitType-select").empty();
      let optionHtml = `<option value=""></option>`;
      for (let item of permitTypeList) {
          optionHtml += `<option value="${item.permitType}">${item.permitType.indexOf('CL ') > -1 ? `PRT ${ item.permitType }` : item.permitType}</option>`
      }
      $("#driver-permitType-select").append(optionHtml);

      $("#driver-permitType-select").off('change').on('change', function() {
        let permitType = $("#driver-permitType-select").val();
        if (permitType.toUpperCase().startsWith('CL')) {
          $('.demerit-point-div').show();
          $('.score-div').hide();
        } else {
          $('.score-div').show();
          $('.demerit-point-div').hide();
        }

        if (permitType.toUpperCase().endsWith('X')) {
          $('.demerit-point-div').hide();
          $('.score-div').hide();
        } 
      });
  })
}

const deletePermitTypeDetail = function (id, permitType) {
  $("#delete-driver-permitTypeDetail-modal").modal("show");
  $("#currentPermitDetailId").val(id);
  $("#deletePermitDetailConfirm").show();

}

const showDeleteInfo = function(deleteUser, deleteAt, deleteReasong) {
  $("#delete-driver-permitTypeDetail-modal").modal("show");

  $(".delete-info-div").show();
  $(".delete-by-label").text(deleteUser);
  $(".delete-time-label").text(deleteAt);
  $(".delete-remarks-input").val(deleteReasong);

  $("#deletePermitDetailConfirm").hide();
}

const confirmDeletePermitTypeDetail = function(id, reason) {
axios.post("/driver/deletePermitTypeDetail", { detailId: id, deleteReason: reason }).then(async res => {
    $.confirm({
        title: res.data.respCode == 1 ? 'Success Info' : 'Fail Info',
        content: res.data.respMessage,
        buttons: {
            confirm: {
              btnClass: 'btn-green',
              action: function () {
                if (res.data.respCode == 1) {
                  $("#currentPermitDetailId").val('');
                  $(".delete-remarks-input").val('');
                  $(".delete-info-div").hide();
                  $("#delete-driver-permitTypeDetail-modal").modal('hide');
                  initVehicleClassList();
                }
              }
            }
        }
    });
})
}

const editDriverPermitTypeDetail = async function(id, permitType, passDate, baseMileage, attemptNums, testerCode, score, demeritPoiont) {
  $('#driverPermitTypeDetailId').val(id);
  await initPermitTypeSelect();
  $('#driver-permitType-select').attr('disabled', 'disabled');
  $('#driver-permitType-select').val(permitType)
  $(".attemptNums-input").val(attemptNums);
  $('.testerCode-input').val(testerCode);
  $('.basePermitTypeMileage-input').val(baseMileage);

  if ($('.can-edit-permitType-mileage').text() != '1') {
    $('.basePermitTypeMileage-input').attr('disabled', 'disabled');
  } else {
    $('.basePermitTypeMileage-input').removeAttr('disabled');
  }

  if (permitType.toUpperCase().startsWith('CL')) {
    $('.demerit-point-div').show();
    if (demeritPoiont) {
      $('.demerit-point-input').val(demeritPoiont);
    }
  } else {
    $('.score-div').show();
    if (score) {
      $('.score-input').val(score);
    }
  }
  if (permitType.toUpperCase().endsWith('X')) {
    $('.demerit-point-div').hide();
    $('.score-div').hide();
  }

  layui.use('laydate', function(){
    let laydate = layui.laydate;
    laydate.render({
      elem: '.passTime-input',
      type: 'date',
      format: 'dd/MM/yyyy',
      value: passDate ? moment(passDate).format('DD/MM/YYYY') : '',
      max: moment(new Date()).format('DD/MM/YYYY'),
      lang: 'en',
      trigger: 'click',
      btns: ['clear', 'confirm'],
      done: (value) => {
      }
    });
  });

  $('#create-VehicleClass').modal('show');
}

const clearVehicleClassFormData = function () {
  $('#driverPermitTypeDetailId').val('');
  $('#driver-permitType-select').val('')
  $(".attemptNums-input").val('')
  $('.passTime-input').val('')
  $('.testerCode-input').val('')
  $('.score-input').val('')
  $('.demerit-point-input').val('')
  $('.basePermitTypeMileage-input').val('');
  $('.basePermitTypeMileage-input').removeAttr('disabled');

  $('#driver-permitType-select').removeAttr('disabled');
}

const approvePermitTypeDetail = async function(recordId, newStatus) {
  axios.post("/driver/approvePermitTypeDetail", { recordId,newStatus }).then(async res => {
    $.confirm({
        title: res.data.respCode == 1 ? 'Success Info' : 'Fail Info',
        content: res.data.respMessage,
        buttons: {
            confirm: {
              btnClass: 'btn-green',
              action: function () {
                if (res.data.respCode == 1) {
                  initVehicleClassList();
                }
              }
            }
        }
    });
  })
}

const checkScore = function (el) {
    let score = $(el).val();
    score = Number.parseInt(score)
    if (score < 45) {
        $.alert({
          title: 'INFO',
          content: `Score should be in 45-50.`
        })
        $(el).val(45)
    } else if (score > 50) {
        $.alert({
          title: 'INFO',
          content: `Score should be in 45-50.`
        })
        $(el).val(50)
    }
}

const checkDemeritPoint = function (el) {
    let demeritPoint = $(el).val();
    demeritPoint = Number.parseInt(demeritPoint)
    if (demeritPoint < 0) {
        $.alert({
          title: 'INFO',
          content: `Demerit Point should be in 0-10.`
        })
        $(el).val(0)
    } else if (demeritPoint > 10) {
        $.alert({
          title: 'INFO',
          content: `Demerit Point should be in 0-10.`
        })
        $(el).val(10)
    }
}