let cardSerialNo = null;
let editId = null;
$(function () {
    initPage()
    initCivilianLicenceList()
    $('.btn-create-civilian-licence').off('click').on('click', function() {       
        $('#driver-licence').val('')
        $('#driverLicenceId').val('')
        $('.lssueDate-input').val('')
        $('.cardSerialNo-input').val('')     
        $('#create-civilian-licence').modal('show');
        $('#create-civilian-licence .modal-title').text('Add Licence')
        if(!editId)  $('.cardSerialNo-div').show()
    })

    $('#driverLicenceCancel').off('click').on('click', function(){
        $('#driver-licence').val('')
        $('#driverLicenceId').val('')
        $('.lssueDate-input').val('')
        $('.cardSerialNo-input').val('')
        $('#create-civilian-licence').modal('hide');
        initCivilianLicenceList()
    })

    $('#driverLicenceConfirm').off('click').on('click', function(){
        addOrEditCivilianLicence()
    })
})

window.EditCivilianLicence = async function(id){
    if(id == editId) {
        $('.cardSerialNo-div').show()
    } else {
        $('.cardSerialNo-div').hide()
    }
    clearFormData()
    $('#create-civilian-licence').modal('show');
    $('#driverLicenceId').val(id)
    $('#create-civilian-licence .modal-title').text('Edit Licence')
    axios.post("/driver/getCivilianLicenceById", { id }).then(async res => {
        let data = res.data ? res.data.respMessage : res.respMessage
        $('#driver-licence').val(data.civilianLicence);
        $('.lssueDate-input').val(data.dateOfIssue ? moment(data.dateOfIssue, 'YYYY-MM-DD').format('DD/MM/YYYY') : '');
        $('.cardSerialNo-input').val(data.cardSerialNumber)
    })
    editId = null
}

window.deleteCivilianLicence = async function(id){
    axios.post("/driver/deleteCivilianLicenceById", { id }).then(async res => {
        $('#create-civilian-licence').modal('hide');
        clearFormData()
        let respCode = res.respCode ? res.respCode : res.data.respCode
        if (respCode != 1)  {
            $.alert({
                title: 'Warn',
                content: res.respMessage ? res.respMessage : res.data.respMessage,
            }); 
        } 
        editId = null
        initCivilianLicenceList()
    })
}

window.initCivilianLicenceList = async function() {
    axios.post("/driver/getCivilianLicence", {driverId: currentEditDriverId}).then(async res => {
        if (res.data.respCode == 1)  {
            $('.licenceDetailDiv').empty();
            let respMessage = res.data.respMessage
            let operationList = respMessage.operationList.split(',')
            let dataList = respMessage.civilianLicenceList
            if(dataList.length <= 0) return 
            editId = dataList[0].id
            for(let temp of dataList) { 
                if(temp.cardSerialNumber) cardSerialNo = temp.cardSerialNumber
                let actionHtml = ``;
                if (operationList.includes('Edit')) {
                actionHtml += `
                    <div style="color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="edit-driver-assessment custom-btn-blue" onclick="EditCivilianLicence(${ temp.id })" role="button" tabindex="0">
                        Edit
                    </div>
                `;
                }
                if (operationList.includes('Delete')) {
                actionHtml += `
                    <div style="color: white; margin-right: 10px; padding-left: 5px; padding-right: 5px; border-radius: 5px;" class="delete-driver-assessment custom-btn-danger" onclick="deleteCivilianLicence(${ temp.id })" role="button" tabindex="0" style="margin-left: 15px;">
                        Delete
                    </div>
                `;
                }
                $('.licenceDetailDiv').append(`
                <div class="py-3" style="display: flex; border-bottom: 1px solid #f5f5f5;">
                    <div style="width: calc(100%/4);">${ temp.civilianLicence }</div>
                    <div style="width: calc(100%/4);">${temp.dateOfIssue ? moment(temp.dateOfIssue).format("DD/MM/YYYY") : ''}</div>
                    <div style="width: calc(100%/4);">${ temp.cardSerialNumber ?? '-' }</div>
                    <div style="width: calc(100%/4);">
                    <div style="display: flex;">
                        ${ actionHtml }
                    </div>
                    </div>
                </div>
                `);
            }
            if(cardSerialNo){
                $('.cardSerialNo-div').hide()
            } 
        } 
    });
    
    
}

window.clearFormData = function(){
    $('#driver-licence').val('')
    $('#driverLicenceId').val('')
    $('.lssueDate-input').val('')
    $('.cardSerialNo-input').val('')
}

const initPage = async function(){
    const initDate = async function(){
        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: '.lssueDate-input',
                type: 'date',
                lang: 'en',
                format: 'dd/MM/yyyy',
                trigger: 'click',
                max: moment().format("DD/MM/YYYY"),
                btns: ['clear', 'confirm'],
            })
        })
    }

    const initClassList = function(){
        let data = ['CL 2B', 'CL 2A', 'CL 2', 'CL 3A', 'CL 3']
        $("#driver-licence").empty();
        let html = `<option></option>`
        for (let item of data) {
            html += `<option>${ item }</option>`
        }
        $("#driver-licence").append(html); 
    }
    initClassList()
    initDate()
}

const addOrEditCivilianLicence = async function(){
    const checkField = function(data) {
        let errorLabel = {
            civilianLicence: 'Civilian Licence', dateOfIssue: 'Date of Issue', cardSerialNumber: 'Card Serial No'
        }
        for (let key in data) {
            if(cardSerialNo) {
                if(key == 'cardSerialNumber') continue
            }
            if (data[key] == null || data[key] == "" || data[key].trim() == "") {
                $.alert({
                    title: 'Warn',
                    content: errorLabel[key] + " is required.",
                });
                return false
            }
        }
        return true;
    }
    let civilianLicenceObj = {
        civilianLicence: $('#driver-licence').val() ?? null,
        dateOfIssue: $('.lssueDate-input').val() ?? null,
        cardSerialNumber: $('.cardSerialNo-input').val() ? $('.cardSerialNo-input').val() : cardSerialNo ? cardSerialNo : null
    }
    let state = checkField(civilianLicenceObj);
    civilianLicenceObj.dateOfIssue = civilianLicenceObj.dateOfIssue ? moment(civilianLicenceObj.dateOfIssue, 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
    if(!state) return
    if(!currentEditDriverId) return
    civilianLicenceObj.driverId = currentEditDriverId
    if(!$('#driverLicenceId').val()) {
        axios.post("/driver/addCivilianLicence", civilianLicenceObj).then(async res => {
            let respCode = res.respCode ? res.respCode : res.data.respCode
            if (respCode != 1)  {
                $.alert({
                    title: 'Warn',
                    content: res.respMessage ? res.respMessage : res.data.respMessage,
                }); 
            } else {
                $('#create-civilian-licence').modal('hide');
                clearFormData()
                editId = null
            }
            initCivilianLicenceList()
        })
    } else {
        axios.post("/driver/editCivilianLicence", { civilianLicenceObj, id: $('#driverLicenceId').val() }).then(async res => {
            let respCode = res.respCode ? res.respCode : res.data.respCode
            if (respCode != 1)  {
                $.alert({
                    title: 'Warn',
                    content: res.respMessage ? res.respMessage : res.data.respMessage,
                }); 
            } else {
                $('#create-civilian-licence').modal('hide');
                clearFormData()
                editId = null
            }
            initCivilianLicenceList()
        })
    }
}