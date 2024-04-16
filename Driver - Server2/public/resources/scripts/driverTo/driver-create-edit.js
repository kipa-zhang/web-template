let userId = Cookies.get('userId');
$(function () {
    $('.opt-btn-div-driver-cancel').on('click', function () {
        $('#view-driver-edit').modal('hide');
        clearFormData()
    })
    $('.driverCancel').on('click', function () {
        $('#view-driver-edit').modal('hide');
        clearFormData()
    })
    initCreateEditPage();
    initClickEditDriver();
})


const initCreateEditPage = async function () {
    const getUnitList = async function (userId) {
        return axios.post('/driver/getUnitList', { userId })
                .then(function (res) {
                    return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    const initUnitTypePage = function (unitList) {
        unitList = unitList.hubNodeList
        $('.driver-hub-select').empty();
        $(".driver-node-select").empty();
        let __unitList = unitList.map(unit => { return unit.hub });
        __unitList = Array.from(new Set(__unitList));
        
        let html = `<option></option>`
        for (let __unit of __unitList) {
            html += `<option name="unitType"  value="${ __unit }">${ __unit }</option>`
        }
        $('.driver-hub-select').append(html); 

        $('.driver-hub-select').off('change').on('change' , function () {
            $('.driver-unit-select').val('')
            let selectedUnit = $(this).val();
            $(".driver-node-select").empty();
            for (let unit of unitList) {
                if (unit.hub === selectedUnit) {
                    for(let node of unit.nodeList) {
                        let html2 = ``;
                        if(node.node){ 
                           html2 = `<option name="subUnitType" value="${ node.node }">${ node.node }</option>`
                        }else{
                            html2 = `<option name="subUnitType" value="${ node.node }">-</option>`
                        }
                        // if(node.node) html2 = `<option name="subUnitType" value="${ node.node }">${ node.node }</option>`
                        $(".driver-node-select").append(html2);
                    }
                } else {
                    continue;
                }
            }
            
        })
    }

    initUnitTypePage(await getUnitList(userId))
    $('.driver-hub-select').trigger('change')

    const initLayDate = function () {
        layui.use('layer', function(){
            layer = layui.layer;
        });
        layui.use('laydate', function(){
            let laydate = layui.laydate;
    
            laydate.render({
                elem: '.enlistment-date-input',
                type: 'date',
                lang: 'en',
                format: 'dd/MM/yyyy',
                trigger: 'click',
                btns: ['clear', 'confirm'],
                done: (value) => {
                    if (value) {
                        if (moment(value).isSameOrAfter(moment($('.ORD-input').val()))) {
                            $.alert({
                                title: 'Warn!',
                                content: 'Operationally Ready Date (ORD) is greater than Enlistment Date.',
                            });
                            $('.ORD-input').val(null)
                        }
                    }
                    
                }
            });
    
            laydate.render({
                elem: '.ORD-input',
                type: 'date',
                lang: 'en',
                format: 'dd/MM/yyyy',
                trigger: 'click',
                btns: ['clear', 'confirm'],
                done: (value) => {
                    if ($('.enlistment-date-input').val()) {
                        if (moment($('.enlistment-date-input').val()).isSameOrAfter(moment(value))) {
                            $.alert({
                                title: 'Warn!',
                                content: 'Operationally Ready Date (ORD) is greater than Enlistment Date.',
                            });
                            $('.ORD-input').val(null)
                        }
                    }
                }
            });
    
            laydate.render({
                elem: '.birth-date-input',
                type: 'date',
                lang: 'en',
                format: 'dd/MM/yyyy',
                trigger: 'click',
                max: moment(new Date()).format('dd/MM/yyyy'),
                btns: ['clear', 'confirm']
            });
        });
    }

    initLayDate();

    $('.edit-driver').off('click').on("click", function () {
        $('#view-driver-edit').modal('show');
        $('.opt-btn-div-edit').val('edit')
        const initDriver = async function () {
            const getDriver = async function () {
                return axios.post('/driver/getDriverByDriverId', { driverId: currentEditDriverId })
                .then(function (res) {
                    if (res.data.respCode === 1) {
                        return res.data.respMessage;
                    } else {
                        console.error(res.data.respMessage);
                        return null;
                    }
                });
            }
           
            let driver = await getDriver();

            $('.layui-tab-content').val(driver.driverId)
            $('.driver-name-input').val(driver.driverName)
            // $('.rank-input').val(driver.rank)
            $('.driver-role-select').val(driver.role)
            $('.driver-role-select').trigger('change')
            $('.driver-vocation-select').val(driver.vocation)
            if(driver.unit) $('.driver-unit-select').val(driver.unit)
            if(driver.hub) {
                $('.driver-hub-select').val(driver.hub)
                $('.driver-hub-select').trigger('change')
                $('.driver-node-select').val(`${ driver.node }`)
            }
            $('.enlistment-date-input').val(driver.enlistmentDate ? moment(driver.enlistmentDate, 'YYYY-MM-DD').format('DD/MM/YYYY') : '')
            $('.ORD-input').val(driver.operationallyReadyDate ? moment(driver.operationallyReadyDate, 'YYYY-MM-DD').format('DD/MM/YYYY') : '')
            // if(driver.nric) driver.nric = ((driver.nric).toString()).substr(0, 1) + ((driver.nric).toString()).substr(((driver.nric).toString()).length-4, 4)
            $('.NRIC-input').val(driver.nric ? driver.nric : '')
            $('.birth-date-input').val(driver.birthday ? moment(driver.birthday, 'YYYY-MM-DD').format('DD/MM/YYYY') : '')
            $('.contact-no-input').val(driver.contactNumber)
        }
        initDriver()
    })

    const getRoleVocation = async function () {
        return axios.post('/getRoleVocation')
                .then(function (res) {
                    return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }

    let roleVocation = await getRoleVocation();

    let roleData = []
    Object.keys(roleVocation).forEach(function(key){
        roleData.push(key)
    })

    const initRoleSelect = function (data) {
        $('.driver-role-select').empty();
        let html = `<option></option>`;
        for(let item of data){
            html += `<option>${ item }</option>`;
        }
        $('.driver-role-select').append(html)
    }


    initRoleSelect(roleData)

    const initVocationSelect = function (data) {
        $('.driver-vocation-select').empty();
        let html = '<option></option>';
        for(let item of data){
            html += `<option>${ item ? item : '-' }</option>`;
        }
        $('.driver-vocation-select').append(html)
    }

    $('.driver-role-select').off('change').on('change', function () {
        if($('.driver-role-select').val()){
            if(($('.driver-role-select').val()).toLowerCase() == 'to' || ($('.driver-role-select').val()).toLowerCase() == 'tl') {
                $('.driver-unit-select').val('')
                $('.driver-unit-select').prop('disabled', 'disabled')
                $('.driver-unit-select').css('background-color', '#e9ecef')

                $('.driver-hub-select').removeAttr('disabled')
                $('.driver-hub-select').css('background-color', 'white')
                $('.driver-node-select').removeAttr('disabled')
                $('.driver-node-select').css('background-color', 'white')
                
            } else if(($('.driver-role-select').val()).toLowerCase() == 'dv' || ($('.driver-role-select').val()).toLowerCase() == 'loa'){
                $('.driver-unit-select').removeAttr('disabled')
                $('.driver-unit-select').removeAttr("style");

                $('.driver-hub-select').val('')
                $('.driver-hub-select').trigger('change')
                // $('.driver-hub-select').prop('disabled', 'disabled')
                // $('.driver-hub-select').css('background-color', '#e9ecef')
                $('.driver-node-select').val('')
                // $('.driver-node-select').prop('disabled', 'disabled')
                // $('.driver-node-select').css('background-color', '#e9ecef')
            }
            initVocationSelect(roleVocation[$('.driver-role-select').val()])
        } else {
            $('.driver-unit-select').removeAttr('disabled')
            $('.driver-unit-select').removeAttr("style");

            $('.driver-hub-select').removeAttr('disabled')
            $('.driver-hub-select').css('background-color', 'white')
            $('.driver-node-select').removeAttr('disabled')
            $('.driver-node-select').css('background-color', 'white')
        }
    });

    const getSystemGroup = async function () {
        return axios.post('/driver/getSystemGroup')
                .then(function (res) {
                    return res.respMessage ? res.respMessage : res.data.respMessage;
        });
    }
    const initUnitGroup = function (data) {
        $(".driver-unit-select").empty();
        let html = `<option></option>`
        for (let item of data) {
            html += `<option name="unitGroup" value="${ item.id }">${ item.groupName }</option>`
        }
        $('.driver-unit-select').append(html); 
        $('.driver-unit-select').off('change').on('change', function(){
            $('.driver-hub-select').val('')
            $('.driver-node-select').val('')
        })
    }
    initUnitGroup(await getSystemGroup())

    // const getPermitTypeList = async function () {
    //     return axios.post('/driver/getPermitTypeList')
    //             .then(function (res) {
    //                 return res.respMessage ? res.respMessage : res.data.respMessage;
    //     });
    // }
    const initDriverClassData = async function (driverClassList) {
        $('#driver-class-div').empty();
        let html = '';
        for(let item of driverClassList){
            html += `<div class="col-6"><input class="driver-class-input driver-class-input-${ (item).replaceAll(" ","_") }" type="checkbox" value="${ item }">${ item }</input></div>`;
        }
        $('#driver-class-div').append(html)
    }
    initDriverClassData(['CL 2B', 'CL 2A', 'CL 2', 'CL 3A', 'CL 3'])
}

const initClickEditDriver = function () {
    const confirmCreateOrSave = function() {
        $('.opt-btn-div-edit').off('click').on('click', function () {
                let classList = [];
                for (let i = 0; i < $('.driver-class-input').length; i++) {
                    if($('.driver-class-input')[i].checked) {
                        if($('.driver-class-input')[i].value && ($('.driver-class-input')[i].value).toUpperCase() != 'NULL') classList.push($('.driver-class-input')[i].value)
                    }
                }
                let driver = {
                    "action": $('.opt-btn-div-edit').val(),
                    "driverId": $('.layui-tab-content').val(),
                    // 'rank': $('.rank-input').val(),
                    'role': $('.driver-role-select').val(),
                    'vocation': $('.driver-vocation-select').val(),
                    'unit': $('.driver-unit-select').val(),
                    'hub':  $('.driver-hub-select').val(),
                    'node': $('.driver-node-select').val(),
                    'enlistmentDate': $('.enlistment-date-input').val() ? $('.enlistment-date-input').val() : null,
                    'operationallyReadyDate': $('.ORD-input').val() ? $('.ORD-input').val() : null,
                    'nric': $('.NRIC-input').val(),
                    'driverName': $('.driver-name-input').val(),
                    'birthday': $('.birth-date-input').val() ? $('.birth-date-input').val() : null,
                    'contactNumber': $('.contact-no-input').val(),
                    // 'class': classList.length > 0 ? classList.join(',') : null,
                    // 'civilianLicence': classList.length > 0 ? classList.join(',') : null,
                    // 'cardSerialNumber': $('.cardSerialNo-input').val() ? $('.cardSerialNo-input').val() : null,
                    // 'dateOfIssue': $('.lssue-date-input').val() ? $('.lssue-date-input').val() : null,
                }
                let checkResult = checkField(driver);
                driver.enlistmentDate = driver.enlistmentDate ? moment(driver.enlistmentDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
                driver.operationallyReadyDate = driver.operationallyReadyDate ? moment(driver.operationallyReadyDate, 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
                driver.birthday = driver.birthday ? moment(driver.birthday, 'DD/MM/YYYY').format('YYYY-MM-DD') : null;
                if (checkResult) {
                    // driver.nric = ((driver.nric).toString()).substr(0, 1) + ((driver.nric).toString()).substr(((driver.nric).toString()).length-4, 4)
                    
                    $(this).addClass('btn-disabled')
                    axios.post("/driver/createDriverFromResource", driver).then(async res => {
                        let start = res.respCode != null ? res.respCode : res.data.respCode
                        if (start == 1)  {
                            $('#view-driver-edit').modal('hide');
                            if(driver.action == 'create'){
                                table.ajax.reload(null, true)
                            } else {
                                initBasicProfileHandler()
                            }
                            clearFormData()
                        } else {
                            $.alert({
                                title: 'Warn',
                                content: res.respMessage ? res.respMessage : res.data.respMessage,
                            });
                        }
                    }).finally(() => {
                        $(this).removeClass('btn-disabled')
                    })
                }
        })       
    }

    const checkField = function(data) {
        let errorLabel = {
            action: 'action', driverId: 'driverId', role: 'Role', vocation: 'Vocation',
            unit: 'Unit', hub: 'Hub',
            node: 'Node', enlistmentDate: 'Enlistment Date', operationallyReadyDate: 'Operationally Ready Date (ORD)', nric: 'NRIC', driverName: 'Driver Name', 
            birthday: 'Date of Birth', contactNumber: 'Contact No.',
        }

        for (let key in data) {
            if(key == 'contactNumber') {
                let firstNumber = ($('.contact-no-input').val()).substring(0, 1)
                if (!(($('.contact-no-input').val()).length == 8 && (firstNumber == "8" || firstNumber == "9"))) {
                    $.alert('Mobile Number must be 8 number and start with 8 or 9.')
                    return false
                } 
            }
            if(key == 'nric') {   
                let regular = /^[S,T]\d{7}[A-Z]$/;     
                // let regular = /^[S,T]\d{3}[A-Z]$/ ;     
                if ((regular).test($('.NRIC-input').val()) == false) {
                    $.alert('The nric format is incorrect.')
                    return false
                } 
            }

            if(key == 'action' || key == 'driverId'){
                continue 
            }

            if(key == 'unit'){
                if($('.driver-role-select').val().toLowerCase() == 'to' || $('.driver-role-select').val().toLowerCase() == 'tl'){
                    continue
                } else {
                    if($('.driver-unit-select').val() || $('.driver-hub-select').val()){
                        continue
                    }
                }
            }

            if(key == 'hub'){
                if($('.driver-unit-select').val() || $('.driver-hub-select').val()){
                    continue
                }
            }

            if(key == 'node'){
                if($('.driver-unit-select').val() || $('.driver-hub-select').val()){
                    continue
                }
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

    confirmCreateOrSave()
}

const clearFormData = function () {
    $('.driver-name-input').val('')
    $('.driver-role-select').val('')
    $('.driver-role-select').trigger('change')
    $('.driver-hub-select').val('')
    $('.driver-hub-select').trigger('change')
    $('.driver-node-select').val('')
    $('.driver-unit-select').val('')
    $('.enlistment-date-input').val('')
    $('.driver-vocation-select').val('')
    $('.ORD-input').val('')
    $('.NRIC-input').val('')
    $('.birth-date-input').val('')
    $('.contact-no-input').val('')
    // $("#driver-class-div").find('input:checkbox').each(function () {
    //     $(this).prop('checked',false);
    // });
    // $('.cardSerialNo-input').val('')
    // $('.lssue-date-input').val('')
}