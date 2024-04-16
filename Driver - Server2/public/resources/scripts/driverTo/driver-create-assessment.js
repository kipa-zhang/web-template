
$(function () {
  initCategoryPage();

  initCategoryList();

  $('#driverAssessmentCancel').on('click', function() {
    clearAssessmentFormData();
    $('#create-assessment').modal('hide');
  })

  $('.btn-create-assessment').on('click', function() {
    clearAssessmentFormData();
    $('#create-assessment').modal('show');
  })
})

const initCategoryList = async function() {
  axios.post("/driver/getAssessmentRecord", {driverId: currentEditDriverId}).then(async res => {
    let code = res.respCode != null ? res.respCode : res.data.respCode
    if (code == 1)  {
      $('.driverAssessmentContentDiv').empty();
      $('.driverAssessmentContentDiv').append(`
        <div class="row pt-4" style="display: flex;">
          <div class="col-2">Assessment Date</div>
          <div class="col-4">Assessment</div>
          <div class="col-2">Status</div>
          <div class="col-2">Approve Status</div>
          <div class="col-2">Action</div>
        </div>
      `);
      let dataList = res.respMessage != null ? res.respMessage : res.data.respMessage
      if (dataList && dataList.length > 0) {
        for(let temp of dataList) {
          let operationList = temp.operation.split(',')

          let actionHtml = ``;
          if (operationList.includes('Edit') && temp.approveStatus != 'Edited') {
            actionHtml += `
              <div style="color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="edit-driver-assessment custom-btn-blue" onclick="editDriverAssessment(${temp.id}, '${temp.assessmentType}', '${temp.issueDate}', '${temp.status}')" role="button" tabindex="0">
                Edit
              </div>
              <div style="color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="delete-driver-assessment custom-btn-danger" onclick="deleteDriverAssessment(${temp.id}, '${temp.assessmentType}')" role="button" tabindex="0" style="margin-left: 15px;">
                Delete
              </div>
            `;
          }

          if (operationList.includes('Approve') && temp.approveStatus == 'Edited') {
            actionHtml += `
              <div style="border: solid 1px #1B9063; background-color: #1B9063;color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="approve-driver-assessment" onclick="approveAssessmentRecord(${temp.id}, 'Approved')" role="button" tabindex="0">
                Approve
              </div>
              <div style="border: solid 1px #1B9063; background-color: #1B9063;color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="cancel-driver-assessment" onclick="approveAssessmentRecord(${temp.id}, 'Rejected')" role="button" tabindex="0">
                Reject
              </div>
            `;
          }
          // if (!actionHtml) {
          //   actionHtml = temp.approveStatus;
          // }

          $('.driverAssessmentContentDiv').append(`
            <div class="row py-3" style="display: flex; border-bottom: 1px solid #f5f5f5;">
              <div class="col-2">${temp.issueDate ? moment(temp.issueDate).format("DD/MM/YYYY") : ''}</div>
              <div class="col-4">${temp.assessmentType}</div>
              <div class="col-2">
                ${temp.status == 'Pass' ? '<div style="text-align: center;width: 80px; height: 22px;font-size: 10px; background-color: #e7f3da; border-radius: 10px; color: #79bd9a;">PASS</div>' : '<div class="col-2"><div style="text-align: center;width: 80px; height: 22px;font-size: 10px; background-color: #fbe7d3; border-radius: 10px; color: #f3912e;">FAIL</div></div>'}
              </div>
              <div class="col-2">${temp.approveStatus == 'Edited' ? 'Pending Approval' : temp.approveStatus}</div>
              <div class="col-2" style="display: flex;">
                ${actionHtml}
              </div>
            </div>
          `);
        }
        let currentCategory = '-'
        dataList = dataList.filter(function(item) {return item.status == 'Pass'})
        if (dataList.length > 0) {
          dataList = dataList.sort(function(item1, item2) {
            return item1.assessmentType > item2.assessmentType ? 1 : -1;
          });
          let maxCateytoryLevel = dataList[0].assessmentType;
          currentCategory = maxCateytoryLevel == 'Category A Assessment' ? 'A' : maxCateytoryLevel == 'Category B Assessment' ? 'B' : maxCateytoryLevel == 'Category C Assessment' 
          ? 'C' : maxCateytoryLevel == 'Category D Assessment' ? 'D' : '-';
        }
        $('.driver-permitCategory').html(currentCategory);
        
      }
    } else {
      $.confirm({
            title: 'WARN',
            content: `Data is error, please refresh the page.`,
            buttons: {
              Ok: {
                btnClass: 'btn-green',
                action: function () {
                }
              }
            }
        });
    }
  });
}

const initCategoryPage = async function () {
  $('#driverAssessmentConfirm').on('click', async function () {
    let driverAssessmentRecordId = $('#driverAssessmentRecordId').val();
    let driverAssessment = {
          "recordId": driverAssessmentRecordId,
          "driverId": currentEditDriverId,
          "assessmentType": $('#driver-assessmentType-select').val(),
          "issueDate": $('.issueTime-input').val(),
          "status": $("input[name='assessmentStatus']:checked").val()
      }
    driverAssessment.issueDate = driverAssessment.issueDate ? moment(driverAssessment.issueDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
    axios.post("/driver/updateAssessmentRecord", driverAssessment).then(async res => {
      let respCode = res.respCode != null ? res.respCode : res.data.respCode
      if (respCode == 1)  {
        $('#create-assessment').modal('hide');
        clearFormData();
        initCategoryList();
      } else {
        let msg = res.respMessage != null ? res.respMessage : res.data.respMessage
        $.alert({
            title: 'Error',
            content: msg,
        });
      }
    });
  });

  layui.use('laydate', function(){
    let laydate = layui.laydate;
    laydate.render({
      elem: '.issueTime-input',
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
}

const editDriverAssessment = async function(id, assessmentType, issueDate, status) {
  $('#driverAssessmentRecordId').val(id);
  $('#driver-assessmentType-select').val(assessmentType)
  $("input[value='"+status+"']").attr('checked', 'checked');

  layui.use('laydate', function(){
    let laydate = layui.laydate;
    laydate.render({
      elem: '.issueTime-input',
      type: 'date', 
      value: issueDate ? moment(issueDate, 'YYYY-MM-DD').format('DD/MM/YYYY') : '',
      format: 'dd/MM/yyyy',
      max: moment().format("DD/MM/YYYY"),
      lang: 'en',
      trigger: 'click',
      btns: ['clear', 'confirm'],
      done: (value) => {
      }
    });
  });

  $('#create-assessment').modal('show');
}

const deleteDriverAssessment = function (id, assessmentType) {
    $.confirm({
        title: 'Confirm Delete',
        content: 'Are you sure to delete assessment:' + assessmentType,
        buttons: {
          cancel: function () {
                
          },
          confirm: {
            btnClass: 'btn-green',
            action: function () {
              confirmDeleteDriverAssessment(id);
            }
          }
        }
    });
}

const confirmDeleteDriverAssessment = function(id) {
  axios.post("/driver/deleteAssessmentRecord", { recordId: id }).then(async res => {
      $.confirm({
          title: res.data.respCode == 1 ? 'Success Info' : 'Fail Info',
          content: res.data.respMessage,
          buttons: {
              confirm: {
                btnClass: 'btn-green',
                action: function () {
                  if (res.data.respCode == 1) {
                    initCategoryList();
                  }
                }
              }
          }
      });
  })
}

const clearAssessmentFormData = function () {
    $('#driverAssessmentRecordId').val('');
    $('#driver-assessmentType-select').val('')
    $('.issueTime-input').val('')
    $("input[name='assessmentStatus']:first").attr('checked', 'checked');
}

const approveAssessmentRecord = async function(recordId, newStatus) {
  axios.post("/driver/approveAssessmentRecord", { recordId,newStatus }).then(async res => {
    $.confirm({
        title: res.data.respCode == 1 ? 'Success Info' : 'Fail Info',
        content: res.data.respMessage,
        buttons: {
          confirm: {
            btnClass: 'btn-green',
            action: function () {
              if (res.data.respCode == 1) {
                initCategoryList();
              }
            }
          }
        }
    });
  })
}