import { refreshTable } from '../scripts/user/user-management.js'

let dataFrom = null;
let userBaseId = null;
let systemUrl = null;
let hubTypeSelect = null;
let registerUserId = null;
let registerUserBaseId = null;

$(async function () {
    if(window.location.pathname == '/login') return
    initPage()  
    if(window.location.pathname == '/user/registerUser') {
        let url = window.location.href;
        let fromValue = url.indexOf('dataFrom=') != '-1' ? decodeURI(url.split("dataFrom=")[1]) : null;
        let dataList = null;
        if(!fromValue){
            dataList = await urlParameterDecode(url.split("registerFrom=")[1])
            registerUserId = dataList ? dataList.userId : null;
            if(dataList) fromValue = dataList.dataFrom ? dataList.dataFrom : null;
        }
       
        if(registerUserId) {
            $('.returnLogin').text('Return to Home menu')
        }
        setTimeout(() => {
            let dataType = dataList ? dataList.dataFrom == 'server' ? 'mv' : 'cv' : null;
            if(registerUserId) initPageByUserBase(null, registerUserId, dataType)
        }, 60) 
        dataFrom = fromValue ? fromValue: 'server';
        $('#registerAccount-modal').modal('show')
        $('#registerAccount-modal .modal-title').text('Register Account')
        document.addEventListener('keydown', function(event) {
            setTimeout(() => {
                if(event.key){
                    if ((event.key).toLowerCase() == 'escape') {
                        let url = null;
                        if(registerUserId) {
                            url = dataFrom == 'system' ? systemUrl + "/" : "/";
                        } else {
                            url = dataFrom == 'system' ? systemUrl + "/login" : "/login";
                        }
                        window.location.href = url; 
                    }
                }
            }, 30) 
        });
    }

    $('.create-registerAccountUser').off('click').on('click',  function(){
        initUser()
    })
    $('.cancel-create-user').off('click').on('click',  function(){
        clearData()
        if(!dataFrom) refreshTable()
    })
    $('.close-create-user').off('click').on('click',  function(){
        clearData()
        if(!dataFrom) refreshTable()
    })
});

const urlParameterDecode = async function (str) {
    return axios.post('/user/urlParameterDecode', { str }).then(function (res) {
        return res.respMessage ? res.respMessage : res.data.respMessage;
    });
}

const initHqUnit = function (userUsableId){
    let userStatus = true
    if(window.location.pathname == '/user/registerUser' && !registerUserId) userStatus = false
    userUsableId = userUsableId ? userUsableId : registerUserBaseId
    axios.post('/user/getHqTypeList', { userStatus, userUsableId }).then(function (res) {
        let hqTypeList = res.respMessage ? res.respMessage : res.data.respMessage;
        // let data = []
        // for(let item of hqTypeList){
        //     data.push({ id: item, name: item }) 
        // }
        // hubTypeSelect = $("#hubType").multipleSelect({
        //     dataKey: 'id',
        //     dataName: 'name',
        //     searchable: false,
        //     data: data
        // });
        $("#hubType").empty()
        let html =' <option></option>';
        for(let item of hqTypeList){
            html+= `<option>${ item }</option>`;
        }
        $("#hubType").append(html)
    });    
}

const initPage = async function () {
    const ininSysRole = async function (){
        const getSystemRole = async function () {
            return axios.get('/user/getSystemRole').then(function (res) {
                return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        let sysRoleList = await getSystemRole()
        $('.cv-accountType').empty()
        let html =' <option></option>';
        for(let item of sysRoleList){
            if(item.roleName.toLowerCase() != 'poc') html+= `<option data-id="${ item.id }">${ item.roleName }</option>`;
        }
        $('.cv-accountType').append(html)
    }
    ininSysRole()

    const getAccountUserData = async function () {
        let userStatus = true
        if(window.location.pathname == '/user/registerUser') userStatus = false
        return axios.post('/user/getAccountUserData', { userStatus }).then(function (res) {
          return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    let accountData = await getAccountUserData();
    const initGroup = async function (groupList) {
        $('.cv-unit').empty()
        let html =' <option></option>';
        for(let item of groupList){
            html+= `<option data-id="${ item.id }">${ item.groupName }</option>`;
        }
        $('.cv-unit').append(html)
        $('.mv-unit').empty()
        let html2 =' <option></option>';
        for(let item of groupList){
            html2+= `<option data-id="${ item.id }">${ item.groupName }</option>`;
        }
        $('.mv-unit').append(html2)
    }
    initGroup(accountData.groupList)

    const initUnitTypePage = async function (unitList) {
        $('#unitType').empty();
        $("#subUnitType").empty();
        let __unitList = unitList.map(unit => { return unit.unit });
        __unitList = Array.from(new Set(__unitList));
        let html = `<option name="unitType"></option>`
        for (let __unit of __unitList) {
            html += `<option name="unitType"  value="${ __unit }">${ __unit }</option>`
        }
        $('#unitType').append(html); 
        $("#unitType").off('change').on('change' , function () {
            let selectedUnit = $(this).val();
            if(selectedUnit) {
                $("#subUnitType").empty();
                let userType = $('#userType').val()
                for (let unit of unitList) {
                    if (unit.unit === selectedUnit) {
                        let html ;
                        if (userType == 'HUB' && unit.subUnit !== null) continue;
                        if (userType == 'NODE' && unit.subUnit == null) continue;

                        if(unit.subUnit === null){
                            html = `<option name="subUnitType" value="${ unit.id }"> - </option>`
                        }else{
                            html = `<option name="subUnitType" value="${ unit.id }">${ unit.subUnit }</option>`
                        }
                        $(" #subUnitType").append(html);
                    } else {
                        continue;
                    }
                }
            } else {
                $("#subUnitType").empty();
            }            
        })
    }
    initUnitTypePage(accountData.unitList)

    const initUserTypePage = async function (userTypeList) {
        $(' #userType').empty();
        $(' #userType').append(`<option></option>`)
        for (let userType of userTypeList) {
            if(userType.toUpperCase() != 'MOBILE') $(' #userType').append(`<option>${ userType }</option>`)
        }
        $('#userType').off('change').on('change', function () {
            if($(this).val()) {
                $('.role-div').show();
            } else {
                $('.role-div').hide();
            }
            $('#userRole').val('')
            $('.mv-unit').data('id', '')
            $('.mv-unit').val('')
            $('#unitType').val('')
            $("#unitType").trigger('change')
            let userType2 = $('#userType').val()
            userType2 = userType2 ? userType2.toUpperCase() : null;
            if (userType2 == 'UNIT') {
                $('.unitGroup-row').hide();
                $('.unit, .subUnit').show(); 
                $('.hq-hub').hide()
                $("#unitType").trigger('change')
            } else if(userType2 == 'CUSTOMER') {
                $('.unitGroup-row').show();
                $('.unit, .subUnit').hide(); 
                $('.hq-hub').hide()
            } else if(userType2 == 'HQ'){
                $('.hq-hub').show()
                initHqUnit(userBaseId)
                $('.unitGroup-row').hide();
                $('.unit, .subUnit').hide(); 
            } else {
                $('.unitGroup-row').hide();
                $('.unit, .subUnit').hide(); 
                $('.hq-hub').hide()
            } 
        })
        $('.hq-hub').hide()
        $('.unitGroup-row').hide();
        $('.unit, .subUnit').hide(); 
    }
    initUserTypePage(accountData.userTypeList)

    const initUserRole = async function (roleList) {
        $(' #userRole').empty();
        $(' #userRole').append(`<option></option>`)
        for (let role of roleList) {
            $(' #userRole').append(`<option id="${ role.id }">${ role.roleName }</option>`)
        }
    }
    initUserRole(accountData.roleList)

    const getServiceType = async function (groupId) {
        $(".cv-platformType").empty()
        await axios.post("/user/getServiceTypeBySelectedGroup", { selectedGroupId: groupId }).then(res => {
            let datas = res.respMessage ? res.respMessage : res.data.respMessage;
            if(!datas) return
            let checkBoxHtml = ""
            datas.forEach((data, index) => {
                let name = data.name
                let id = data.id
                checkBoxHtml += `<div class="form-check form-check-inline">
                                <input class="form-check-input" type="checkbox" data-value="${id}" id="service-type-${index}">
                                <label class="form-check-label checkbox-mt" for="service-type-${index}">
                                    ${name}
                                </label>
                            </div>`
            });
            $(".cv-platformType").append(`<div class="service-type-checkbox">${checkBoxHtml}</div>`)
        })
    }

    const getServiceProvider = async function (datas) {
        $(".cv-serviceProvider").empty()
        let checkBoxHtml = ""
        datas.forEach((data, index) => {
            let name = data.name
            let id = data.id
            checkBoxHtml += `<div class="form-check form-check-inline2">
                            <input class="form-check-input" type="checkbox" data-value="${id}" id="service-provider-${index}">
                            <label class="form-check-label checkbox-mt" for="service-provider-${index}">
                                ${name}
                            </label>
                        </div>`
        });
        $(".cv-serviceProvider").append(`<div class="service-provider-checkbox">${checkBoxHtml}</div>`)
    }
    
    $('.cv-unit').off('change').on('change', function() {
        if($('.cv-unit option:selected').data('id') 
        && ($('.cv-accountType option:selected').val().toLowerCase() == 'rf' || $('.cv-accountType option:selected').val().toLowerCase() == 'occ mgr'
        || $('.cv-accountType option:selected').val().toLowerCase() == 'ra' || $('.cv-accountType option:selected').val().toLowerCase() == 'cm')) {
            $('.cv-platformType-div').show()
            getServiceType($('.cv-unit option:selected').data('id'))
        } else {
            $('.cv-platformType-div').hide()
        }
    })
    $('.cv-accountType').off('change').on('change', function() {
        $('.cv-unit').val('')
        $('.cv-unit').data('id', '')
        $('.form-check-inline input[type="checkbox"]').prop('checked', false);
        $('.form-check-inline2 input[type="checkbox"]').prop('checked', false);
        let sysRole = $('.cv-accountType option:selected').val().toLowerCase();
        if(sysRole == 'tsp'){
            $('.cv-unit-div').hide();
            $('.cv-platformType-div').hide();
            $('.cv-serviceProvider-div').show();
            getServiceProvider(accountData.sysServiceProviderList)
        } else {
            $('.cv-unit-div').show();
            $('.cv-serviceProvider-div').hide();
            $('.cv-platformType-div').hide();
        }
    })
    const getSystemUrl = async function () {
        return axios.get('/user/getSystemUrl').then(function (res) {
          return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    systemUrl = await getSystemUrl();
    if(!dataFrom) {
        $('.cancel-create-user').show()
        $('.close-create-user').show()
        $('.returnLogin').hide()
    } else {
        $('.cancel-create-user').hide()
        $('.close-create-user').hide()
        $('.returnLogin').show()
        $('.returnLogin').off('click').on('click',  function(){
            let url = null;
            if(registerUserId) {
                url = dataFrom == 'system' ? systemUrl + "/" : "/";
            } else {
                url = dataFrom == 'system' ? systemUrl + "/login" : "/login";
            }
            window.location.href = url; 
            clearData()
        })
    }

    
    const initLayDate = function () {
        layui.use('layer', function(){
            layer = layui.layer;
        });
        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: '.ORD-Date',
                format: 'dd/MM/yyyy',
                type: 'date',
                lang: 'en',
                trigger: 'click',
                min: moment().format('YYYY-MM-DD'),
                btns: ['clear', 'confirm']
            });
        });
    }

    initLayDate();
}

export async function initPageByUserBase (id, registerUserId, dataType) {
    $('#registerAccount-modal').modal('show')
    if(!registerUserId && id) $('#registerAccount-modal .modal-title').text('Edit User')
    let userBase = null;
    let mvUnit = null;
    if(id) {
        userBaseId = id;
        const getUserBaseById = async function (userBaseId) {
            return axios.post('/user/getUserBaseById', { userBaseId: userBaseId }).then(function (res) {
            return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        let userData = await getUserBaseById(userBaseId)
        mvUnit = userData.mvUnit;
        userBase = userData.data
    }

    if(registerUserId) {
        const getUserBaseByUserId = async function (registerUserId, dataType) {
            return axios.post('/user/getUserBaseByUserId', { cvmvuserId: registerUserId, dataType: dataType }).then(function (res) {
              return res.respMessage ? res.respMessage : res.data.respMessage;
            });
        }
        let userdata = await getUserBaseByUserId(registerUserId, dataType)
        userBase = userdata.data;
        mvUnit = userdata.hubNode
        if(!userBase) {
            $('.returnLogin').trigger('click')
            return
        }
        if(userBase) registerUserBaseId = userBase.id
    }
    if(!userBase) return
    
    $('.ORD-Date').val(userBase.ord ? moment(userBase.ord).format('DD/MM/YYYY') : '')
    $("#user-nric").val(userBase.nric)
    $('#user-userName').val(userBase.fullName)
    $('.mobileNumber').val(userBase.contactNumber)
    $('.email').val(userBase.email)
    $(`.cv-accountType option[data-id='${ userBase.cvRole }']`).prop("selected", "selected").trigger("change")
    // $(`.cv-unit option[data-id='${ userBase.cvGroupId }']`).prop("selected", "selected").trigger("change")
    $('#userType').val(userBase.mvUserType)
    $('#userType').trigger('change')
    if($('#userType').val()){
        $(`.mv-unit option[data-id='${ userBase.mvGroupId }']`).prop("selected", "selected")
        if(mvUnit) {
            $('#unitType').val(mvUnit.hub)
            $("#unitType").trigger('change')
            $(`#subUnitType option[value='${ userBase.mvUnitId }']`).attr("selected", "selected")
        }
        // if($('#userType').val().toLowerCase() == 'hq' && userBase.mvUnitId) {
        //     setTimeout(function(){
        //         hubTypeSelect.setValue(userBase.mvUnitId)
        //     }, 400)
        // }
        if($('#userType').val().toLowerCase() == 'hq' && userBase.hq){
            setTimeout(function(){
                $('#hubType').val(userBase.hq)
            }, 300)
        }
        $('#userRole').val(userBase.mvRoleName)
    }
    if(userBase.cvServiceProviderId) {
        if(userBase.cvServiceProviderId != ''){
            $('.cv-serviceProvider-div').show();
            let cvServiceProviderIdList = (userBase.cvServiceProviderId).split(',')
            for(let item of cvServiceProviderIdList){
                $(`.form-check-inline2 input[type='checkbox'][data-value='${ item }']`).prop("checked", true)
            }
        }
    }
    setTimeout(function(){
        if(userBase.cvGroupId) $(`.cv-unit option[data-id='${ userBase.cvGroupId }']`).prop("selected", "selected").trigger("change")
    }, 150)
    setTimeout(function(){
        if(userBase.cvServiceTypeId) {
            if(userBase.cvServiceTypeId != ''){
                $('.cv-platformType-div').show();
                let cvServiceTypeIdList = (userBase.cvServiceTypeId).split(',')
                for(let item of cvServiceTypeIdList){
                    $(`.form-check-inline input[type='checkbox'][data-value='${ item }']`).prop("checked", true)
                }    
            }
        }
    }, 500)

    if(registerUserId){
        let mvUserState = userBase ? userBase.mvUserId ? true : false : false;
        let cvUserState = userBase ? userBase.cvUserId ? true : false : false;
        
        if(mvUserState) {
            $('#user-userName').prop('disabled', 'disabled')
            $('#user-userName').css('background-color', '#e9ecef')

            $('#userType').prop('disabled', 'disabled')
            $('#userType').css('background-color', '#e9ecef')
    
            $('#userRole').prop('disabled', 'disabled')
            $('#userRole').css('background-color', '#e9ecef')
    
            $('.mv-unit').prop('disabled', 'disabled')
            $('.mv-unit').css('background-color', '#e9ecef')
    
            $('#unitType').prop('disabled', 'disabled')
            $('#unitType').css('background-color', '#e9ecef')
    
            $('#subUnitType').prop('disabled', 'disabled')
            $('#subUnitType').css('background-color', '#e9ecef')

            $('#hubType').prop('disabled', 'disabled')
            $('#hubType').css('background-color', '#e9ecef')
        }   
        if(cvUserState) {
            $('#user-userName').prop('disabled', 'disabled')
            $('#user-userName').css('background-color', '#e9ecef')

            $('.cv-accountType').prop('disabled', 'disabled')
            $('.cv-accountType').css('background-color', '#e9ecef')
    
            $('.cv-unit').prop('disabled', 'disabled')
            $('.cv-unit').css('background-color', '#e9ecef')

            setTimeout(function(){
                for(let item of $('.form-check-inline2 input[type="checkbox"]')){
                    $(item).prop('disabled', 'disabled')
                }
                for(let item of $('.form-check-inline input[type="checkbox"]')){
                    $(item).prop('disabled', 'disabled')
                }
                $('.cv-platformType').css('background-color', '#e9ecef')
            }, 500)
        
        } 
    }
}

const initUser = async function () {
    let serviceTypeId = null;
    let serviceTypeArray = []
    $.each($('.form-check-inline input:checkbox:checked'), function () {
        serviceTypeArray.push($(this).data("value"))
    })
    if(serviceTypeArray.length > 0) serviceTypeId = serviceTypeArray.join(",")
    let serviceProviderId = null;
    let serviceProviderArray = []
    $.each($('.form-check-inline2 input:checkbox:checked'), function () {
        serviceProviderArray.push($(this).data("value"))
    })
    if(serviceProviderArray.length > 0) serviceProviderId = serviceProviderArray.join(",")
    let accountUserByValid = {
        fullName: $('#user-userName').val(),
        nric: $("#user-nric").val(),
        contactNumber: $('.mobileNumber').val(),
        email: $('.email').val(),
        ord: $('.ORD-Date').val() ? moment($('.ORD-Date').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null
    }
    let accountUser = {
        nric: $("#user-nric").val(),
        fullName: $('#user-userName').val(),
        contactNumber: $('.mobileNumber').val(),
        email: $('.email').val(),
        ord: $('.ORD-Date').val() ? moment($('.ORD-Date').val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : null,
        status: 'Pending Approval',
        cvRole: $('.cv-accountType option:selected').data('id') ?? null,
        cvGroupId: $('.cv-unit option:selected').data('id') ?? null,
        cvGroupName: $('.cv-unit option:selected').data('id') ? $('.cv-unit option:selected').val() : null,
        cvServiceProviderId: serviceProviderId,
        cvServiceTypeId: serviceTypeId,
        mvUserType: $('#userType').val() ? $('#userType').val() != '' ? $('#userType').val() : null : null,
        mvGroupId: $('.mv-unit option:selected').data('id') ?? null,
        mvGroupName: $('.mv-unit option:selected').data('id') ? $('.mv-unit option:selected').val() : null,
        mvRoleName: $('#userRole').val() && $('#userRole').val() != '' ?  $('#userRole').val() : null,
        dataFrom: dataFrom ? dataFrom == 'server' ? 'SERVER-REG' : 'SYSTEM-REG' : 'SERVER-USER',
        hq: $('#hubType').val() ?? null,
        mvUnitId: $('#subUnitType option:selected').val() ?? null
    }
    // let userTypeState = $('#userType').val() ? $('#userType').val() != '' ? $('#userType').val().toLowerCase() == 'hq' : true : false;
    // if(userTypeState) {
    //     accountUser.mvUnitId = hubTypeSelect ? hubTypeSelect.getValue() : null;
    // } else {
    //     accountUser.mvUnitId = $('#subUnitType option:selected').val() ? $('#subUnitType option:selected').val() : null;
    // }
    const ValidAssignTask = function (data, accountUser) {
        // let errorLabel = {
        //     nric: 'Nric',
        //     fullName: 'Name',
        //     contactNumber: 'Mobile Number',
        //     email: 'ddd@gmail.com',
        //     status: 'status',
        //     cvRole: 'CV Account Type Request',
        //     cvGroupId: 'CV Unit',
        //     cvGroupName: 'CV Unit',
        //     cvServiceProviderId: 'CV Service Provider',
        //     cvServiceTypeId: 'CV Platform Type',
        //     mvUserType: 'MV Account Type Request',
        //     mvUnitId: 'HUB / NODE',
        //     mvGroupId: 'Unit',
        //     mvGroupName: 'Unit',
        //     mvRoleName: 'Role',
        //     dataFrom: 'dataFrom',
        // }
        let errorLabel = {
            fullName: 'Name',
            nric: 'Nric',
            contactNumber: 'Mobile Number',
            email: 'E-Mail',
            ord: 'Operationally Ready Date (ORD)'
        }
        for (let key in data) {
            if(key == 'contactNumber') {
                let firstNumber = ($('.mobileNumber').val()).substring(0, 1)
                if (!(($('.mobileNumber').val()).length == 8 && (firstNumber == "8" || firstNumber == "9"))) {
                    $.alert({
                        title: 'Warn',
                        content: 'Mobile Number must be 8 number and start with 8 or 9.'
                    })
                    return false
                } 
            }
            if(key == 'nric') {   
                let regular = /^[S,T]\d{7}[A-Z]$/ ;     
                if ((regular).test($("#user-nric").val()) == false) {
                    $.alert({
                        title: 'Warn',
                        content: 'The nric format is incorrect.'
                    })
                    return false
                } 
            }
            if(key == 'email') {
                let regular = /^[^\s@]+@[^\s@]+\.[^\s@]+$/ ;     
                if ((regular).test($(".email").val()) == false) {
                    $.alert({
                        title: 'Warn',
                        content: 'The E-Mail format is incorrect.'
                    })
                    return false
                } 
            }
            if(!data[key]) {
                $.alert({
                    title: 'Warn',
                    content: `${ errorLabel[key] } is required.`,
                });
                return false;
            }
        }
        if(accountUser.mvUserType){
            if(accountUser.mvUserType.toLowerCase() == 'hq') {
                if(!accountUser.hq) {
                    $.alert({
                        title: 'Warn',
                        content: `HQ Unit is required.`,
                    });
                    return false
                }
            }
            if(!accountUser.mvRoleName){
                $.alert({
                    title: 'Warn',
                    content: `Role is required.`,
                });
                return false
            }
            if(accountUser.mvUserType.toLowerCase() == 'hub' || accountUser.mvUserType.toLowerCase() == 'node'){
                if(!accountUser.mvUnitId){
                    $.alert({
                        title: 'Warn',
                        content: `Hub is required.`,
                    });
                    return false
                }
            }
            if(accountUser.mvUserType.toLowerCase() == 'customer'){
                if(!accountUser.mvGroupId){
                    $.alert({
                        title: 'Warn',
                        content: `Unit is required.`,
                    });
                    return false
                }
            }
        }
        if(accountUser.cvRole){
            if($('.cv-accountType option:selected').val().toLowerCase() != 'tsp') {
                if(!accountUser.cvGroupId){
                    $.alert({
                        title: 'Warn',
                        content: `CV Unit is required.`,
                    });
                    return false
                }
            }
        } else {
            if(accountUser.cvGroupId){
                $.alert({
                    title: 'Warn',
                    content: `CV Account Type Request is required.`,
                });
                return false
            }
        }
        
        if(!accountUser.mvUserType && !accountUser.cvRole) {
            $.alert({
                title: 'Warn',
                content: `Either CV Account type request or MV Account type request must have a value.`
            });
            return false
        }
        if(registerUserId && dataFrom) {
            if(dataFrom == 'server') {
                if(!accountUser.cvRole){ 
                    $.alert({
                        title: 'Warn',
                        content: `CV Account Type Request is required.`,
                    });
                    return false
                }
            } else {
                if(dataFrom == 'system') {
                    if(!accountUser.mvUserType){
                       $.alert({
                            title: 'Warn',
                            content: `MV Account Type Request is required.`,
                        });
                        return false
                    }
                } 
            }
        }
        return true
    }
    let state = ValidAssignTask(accountUserByValid, accountUser)
    if(!state) return
    if(userBaseId || registerUserBaseId) {
        userBaseId = registerUserBaseId ? registerUserBaseId : userBaseId
        let dataUserType = null;
        if(registerUserBaseId) dataUserType = 'HomeRegistration'
        axios.post('/user/editAccountUser',{ accountUser, userBaseId, dataUserType })
        .then(function (res) {
            let respCode = res.respCode ?? res.data.respCode;
            if(respCode == 1){
                if(registerUserBaseId){
                    $.confirm({
                        title: 'Info',
                        content: 'Account Registration Succesful! Please wait for account to be approved by your system administrator.',
                        buttons: {
                            // no: function(){
                            //     clearData()
                            // },
                            Ok: {
								btnClass: 'btn-green',
								action: function () {
                                    let url = dataFrom == 'system' ? systemUrl + "/" : "/";
                                    window.location.href = url;    
                                    clearData()
								}
							}
                        }
                    });
                } else {
                    clearData()
                    $('#registerAccount-modal').modal('hide')
                    refreshTable()
                }
            } else {
                $.confirm({
                    title: 'Warn',
                    content: res.respMessage ?? res.data.respMessage,
                });
            }
            
        });
    } else {
        axios.post('/user/registerAccountUser',{ accountUser })
        .then(function (res) {
            let respCode = res.respCode ?? res.data.respCode;
            if(respCode == 1){
                if(dataFrom) {
                    $.confirm({
                        title: 'Info',
                        content: 'Account Registration Succesful! Please wait for account to be approved by your system administrator.',
                        buttons: {
                            // no: function(){
                            //     clearData()
                            // },
                            Ok: {
								btnClass: 'btn-green',
								action: function () {
                                    let url = dataFrom == 'system' ? systemUrl + "/login" : "/login";
                                    window.location.href = url;    
                                    clearData()
								}
							}
                        }
                    });
                } else {
                    $('#registerAccount-modal').modal('hide')
                    refreshTable()
                    clearData()
                }
            } else {
                $.confirm({
                    title: 'Warn',
                    content: res.respMessage ?? res.data.respMessage,
                });
            }
        });
    }
}

const clearData = function() {
    userBaseId = null;
    systemUrl = null;
    registerUserBaseId = null;
    registerUserId = null;
    if(window.location.pathname != '/user/registerUser') dataFrom = null;
    $("#user-nric").val('')
    $('#user-userName').val('')
    $('.mobileNumber').val('')
    $('.email').val('')
    $('.cv-accountType').val('')
    $('.cv-accountType').data('id', '')
    $('.cv-accountType').trigger('change')
    $('.cv-unit').data('id', '')
    $('.cv-unit').val('')
    $('.cv-unit').trigger('change')
    $('#userType').val('')
    $('#userType').trigger('change')
    $('#subUnitType').val('')
    $('.mv-unit').data('id', '')
    $('.mv-unit').val('')
    $('#unitType').val('')
    $("#unitType").trigger('change')
    $('#userRole').val('')
    $('.form-check-inline input[type="checkbox"]').prop('checked', false);
    $('.form-check-inline2 input[type="checkbox"]').prop('checked', false);
    $('.cv-platformType-div').hide();
    $('.cv-serviceProvider-div').hide();
    $('.ORD-Date').val('')
    // if(hubTypeSelect) hubTypeSelect.clearAll()
    $("#hubType").val('')

    $('#user-userName').removeAttr('disabled')
    $('#user-userName').css('background-color', '')

    $('#userType').removeAttr('disabled')
    $('#userType').css('background-color', '')

    $('#userRole').removeAttr('disabled')
    $('#userRole').css('background-color', '')

    $('.mv-unit').removeAttr('disabled')
    $('.mv-unit').css('background-color', '')

    $('#unitType').removeAttr('disabled')
    $('#unitType').css('background-color', '')

    $('#subUnitType').removeAttr('disabled')
    $('#subUnitType').css('background-color', '')

    $("#hubType").removeAttr('disabled')
    $("#hubType").css('background-color', '')

    $('.cv-accountType').removeAttr('disabled')
    $('.cv-accountType').css('background-color', '')

    $('.cv-unit').removeAttr('disabled')
    $('.cv-unit').css('background-color', '')

    
    setTimeout(function(){
        for(let item of $('.form-check-inline2 input[type="checkbox"]')){
            $(item).removeAttr('disabled')
        }
        for(let item of $('.form-check-inline input[type="checkbox"]')){
            $(item).removeAttr('disabled')
        }
        $('.cv-platformType').css('background-color', '')
    }, 500)
}