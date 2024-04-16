$(function () {
  initPlatformConfList();

  $('#driverPlatformconfCancel').on('click', function() {
    clearPlatformconfFormData();
    $('#create-platformconf').modal('hide');
  })

  $('.btn-create-platformconf').on('click', function() {
    initPlatformConfPage();
    clearPlatformconfFormData();
    $('#create-platformconf').modal('show');
  })

  $('#driverPlatformconfConfirm').on('click', async function () {
    let driverPlatformConfId = $('#driverPlatformConfId').val();
    let driverPlatformConf = {
          "confId": driverPlatformConfId,
          "driverId": currentEditDriverId,
          "permitType": $('#driver-permitType-select').val(),
          "vehicleType": $('#driver-vehicleType-select').val(),
          "assessmentDate": $('.assessmentTime-input').val() ? moment($('.assessmentTime-input').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null,
          "lastDrivenDate": $('.lastDrivenTime-input').val() ? moment($('.lastDrivenTime-input').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null,
          //"baseMileage": $(".baseMileage-input").val()
      }
  
    axios.post("/driver/updatePlatformConf", driverPlatformConf).then(async res => {
      let respCode = res.respCode != null ? res.respCode : res.data.respCode
      if (respCode == 1)  {
        $('#create-platformconf').modal('hide');
        clearPlatformconfFormData();
        initPlatformConfList();
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

const initPlatformConfList = async function() {
  axios.post("/driver/getPlatformConfList", {driverId: currentEditDriverId}).then(async res => {
    let code = res.respCode != null ? res.respCode : res.data.respCode
    if (code == 1)  {
        $('.platformConfDiv').empty();
        let dataList = res.respMessage != null ? res.respMessage : res.data.respMessage
        if (dataList && dataList.length > 0) {
        for(let temp of dataList) {
          let diffNow = "-"
          if (temp.lastDrivenDate) {
            diffNow = moment().diff(moment(temp.lastDrivenDate), 'days');
            if (diffNow > 30) {
              diffNow = Math.floor(diffNow / 30) + ' month ago'
            } else {
              diffNow = diffNow + ' days ago';
            }
          }
          
          let operationList = temp.operation.split(',')

          let actionHtml = ``;
          if (operationList.includes('Edit') && temp.approveStatus != 'Edited') {
            actionHtml += `
              <div style="color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="edit-driver-assessment custom-btn-blue" onclick="editDriverPlatformConf(${temp.id}, '${temp.permitType}', '${temp.vehicleType}', '${temp.assessmentDate}', '${temp.lastDrivenDate ? temp.lastDrivenDate : ''}', ${temp.baseMileage})" role="button" tabindex="0">
                  Edit
              </div>
              <div style="color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="delete-driver-assessment custom-btn-danger" onclick="deleteDriverPlatformConf(${temp.id}, '${temp.vehicleType}')" role="button" tabindex="0" style="margin-left: 15px;">
                  Delete
              </div>
            `;
          }

          if (operationList.includes('Approve') && temp.approveStatus == 'Edited') {
            actionHtml += `
              <div style="border: solid 1px #1B9063; background-color: #1B9063;color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="approve-driver-platformconf" onclick="approvePlatformConf(${temp.id}, 'Approved')" role="button" tabindex="0">
                Approve
              </div>
              <div style="border: solid 1px #C4A548; background-color: #C4A548;color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="cancel-driver-platformconf" onclick="approvePlatformConf(${temp.id}, 'Rejected')" role="button" tabindex="0">
                Reject
              </div>
            `;
          }
          // if (!actionHtml) {
          //   actionHtml = temp.approveStatus;
          // }

          $('.platformConfDiv').append(`
            <div class="py-3" style="display: flex; border-bottom: 1px solid #f5f5f5;">
              <div style="width: calc(100%/6);">${temp.vehicleType}</div>
              <div style="width: calc(100%/6);">${temp.assessmentDate ? moment(temp.assessmentDate).format("DD/MM/YYYY") : ''}</div>
              <div style="width: calc(100%/6);">${diffNow}</div>
              <div style="width: calc(100%/6);">${temp.baseMileage}</div>
              <div style="width: calc(100%/6);">${temp.approveStatus == 'Edited' ? 'Pending Approval' : temp.approveStatus}</div>
              <div style="width: calc(100%/6);">
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

const initPlatformConfPage = async function () {
  layui.use('laydate', function(){
    let laydate = layui.laydate;
    laydate.render({
      elem: '.assessmentTime-input',
      type: 'date',
      lang: 'en',
      format: 'dd/MM/yyyy',
      trigger: 'click',
      max: moment().format("DD/MM/YYYY"),
      btns: ['clear', 'confirm'],
      done: (value) => {
      }
    });
    laydate.render({
      elem: '.lastDrivenTime-input',
      type: 'date',
      lang: 'en',
      format: 'dd/MM/yyyy',
      trigger: 'click',
      max: moment().format("DD/MM/YYYY"),
      btns: ['clear', 'confirm'],
      done: (value) => {
      }
    });
  });
  initVehicleTypeSelect();
}

const initVehicleTypeSelect = async function() {
  await axios.post("/driver/getVehicleTypeByDriverId", {driverId: currentEditDriverId}).then(async res => {
      let vehicleTypeArray = res.data.respMessage;
      if (vehicleTypeArray && vehicleTypeArray.length > 0) {
        $("#driver-vehicleType-select").empty();
          let optionHtml = `<option value=""></option>`;
          for(let vehicleType of vehicleTypeArray) {
            optionHtml += `<option value="${vehicleType.typeOfVehicle}">${vehicleType.typeOfVehicle}</option>`
          }

          $("#driver-vehicleType-select").append(optionHtml);
      }
  });
}

const deleteDriverPlatformConf = function (id, vehicleType) {
  $.confirm({
      title: 'Confirm Delete',
      content: 'Are you sure to delete platform:' + vehicleType,
      buttons: {
        cancel: function () {
              
        },
        confirm: {
          btnClass: 'btn-green',
          action: function () {
            confirmDeleteDriverPlatformConf(id);
          }
        }
      }
  });
}

const confirmDeleteDriverPlatformConf = function(id) {
axios.post("/driver/deleteDriverPlatformConf", { confId: id }).then(async res => {
    $.confirm({
        title: res.data.respCode == 1 ? 'Success Info' : 'Fail Info',
        content: res.data.respMessage,
        buttons: {
            confirm: {
              btnClass: 'btn-green',
              action: function () {
                if (res.data.respCode == 1) {
                  initPlatformConfList();
                }
              }
            }
        }
    });
})
}

const editDriverPlatformConf = async function(id, permitType, vehicleType, assessmentDate, lastDrivenDate, baseMileage) {
  $('#driverPlatformConfId').val(id);

await initPermitTypeSelect();
  await initVehicleTypeSelect();
  $('#driver-permitType-select').val(permitType)
  $('#driver-vehicleType-select').val(vehicleType)
  $(".baseMileage-input").val(baseMileage);

  $('#driver-vehicleType-select').attr('disabled', 'disabled');

  layui.use('laydate', function(){
    let laydate = layui.laydate;
    laydate.render({
      elem: '.assessmentTime-input',
      type: 'date',
      format: 'dd/MM/yyyy',
      value: assessmentDate ? moment(assessmentDate, 'YYYY-MM-DD').format('DD/MM/YYYY') : '',
      max: moment().format("DD/MM/YYYY"),
      lang: 'en',
      trigger: 'click',
      btns: ['clear', 'confirm'],
      done: (value) => {
      }
    });
    laydate.render({
      elem: '.lastDrivenTime-input',
      type: 'date',
      format: 'dd/MM/yyyy',
      value: lastDrivenDate ? moment(lastDrivenDate, 'YYYY-MM-DD').format('DD/MM/YYYY') : '',
      max: moment().format("DD/MM/YYYY"),
      lang: 'en',
      trigger: 'click',
      btns: ['clear', 'confirm'],
      done: (value) => {
      }
    });
  });

  $('#create-platformconf').modal('show');
}

const clearPlatformconfFormData = function () {
  $('#driverPlatformConfId').val('');
  $('#driver-permitType-select').val('')
  $('#driver-vehicleType-select').val('')
  $('.assessmentTime-input').val('')
  $('.lastDrivenTime-input').val('')
  $('.baseMileage-input').val('');

  $('#driver-vehicleType-select').removeAttr('disabled');
}

const approvePlatformConf = async function(recordId, newStatus) {
  axios.post("/driver/approvePlatformConf", { recordId,newStatus }).then(async res => {
    $.confirm({
        title: res.data.respCode == 1 ? 'Success Info' : 'Fail Info',
        content: res.data.respMessage,
        buttons: {
            confirm: {
              btnClass: 'btn-green',
              action: function () {
                if (res.data.respCode == 1) {
                  initPlatformConfList();
                }
              }
            }
        }
    });
  })
}