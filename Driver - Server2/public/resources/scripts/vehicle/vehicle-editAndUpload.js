
let currentUploadVehicleFile = null;
let editPageAction = 'create';
let sysSupportVehicleTypeInfo = [];

$(function () {
    $(".vehicle-batch-upload-div").on('click', function() {
        initVehicleUploadPage();
    });

    $(".download-div").on('click', function() {
        window.location.href = "../template/vehicle-template.xlsx";
    });

    $(".upload-label-div").on('click', function() {
        $("#click-upload-file").trigger("click");
    });

    $("#click-upload-file").on('change', function(event) {
        let files = event.target.files;
        parseFile(files);
    });

    $(".opt-btn-div-upload").on('click', function() {
        confirmUploadFile();
    })

    if('draggable' in document.createElement('span')){
        let holder = document.getElementById('upload-file-div');
        if (holder) {
            holder.ondragover = function () { 
                return false; 
            };
            holder.ondragend = function () { 
                return false; 
            };
            holder.ondrop = function (event) {
                event.preventDefault();
                let files = event.dataTransfer.files;
                parseFile(files);
            };
        }
        
    }

    $(".opt-btn-div-cancel").on('click', function() {
        cancleUploadFile();
    })
})

const parseFile = function(files) {
    if (files && files.length > 0) {
        let fileName = files[0].name
        let fileNameArray = fileName.split('.');
        let fileType = fileNameArray[fileNameArray.length - 1]
        if (fileType && fileType.toLowerCase() != 'xls' && fileType.toLowerCase() != 'xlsx') {
            $.alert({
                title: 'Warn',
                content: 'Just support .xls or .xlsx file!',
            });
            return;
        }

        currentUploadVehicleFile = files[0];
        $(".upload-label-div").text(currentUploadVehicleFile.name);
    }
}

const initVehicleUploadPage = async function() {
    $('#view-vehicle-resource-upload').modal('show');
}

const initVehicleEditPage = async function(action, vehicleNo) {

    $('#view-vehicle-edit').modal('show');
    $(".edit-content-div").show();
    $(".success-div").hide();
    editPageAction = action;

    let defaultVehicleType = '';
    let defaultPermitType = '';
    let defaultVehicleCategory = '';

    $("input[name='pmType']").off('click').on('click', function() {
        let currentPmType = $("input[name='pmType']:checked").val();
        if (currentPmType == 1) {
            $('.pm-by-distance-div').show();
            $('.pm-by-months-div').hide();
        } else if (currentPmType == 2) {
            $('.pm-by-distance-div').hide();
            $('.pm-by-months-div').show();
        }
    })

    $('.vehicle-hub-select').empty();
    $(".vehicle-node-select").empty();
    await axios.post('/getUnitList').then(function (res) {
        let unitList = res.respMessage ? res.respMessage : res.data.respMessage;
        if (unitList && unitList.length > 0) {
            let __unitList = unitList.map(unit => { return unit.unit });
            __unitList = Array.from(new Set(__unitList));
            
            let html = `<option></option>`
            for (let __unit of __unitList) {
                html += `<option name="unitType"  value="${ __unit }">${ __unit }</option>`
            }
            $('.vehicle-hub-select').append(html); 

            $('.vehicle-hub-select').off('change').on('change' , function () {
                let selectedUnit = $(this).val();
                if (selectedUnit) {
                    $(".vehicle-group-select").val("");
                }
                $(".vehicle-node-select").empty();
                for (let unit of unitList) {
                    if (unit.unit === selectedUnit) {
                        let html2 = `<option></option>`;
                        if(unit.subUnit){
                            html2 = `<option name="subUnitType" value="${ unit.subUnit }">${ unit.subUnit }</option>`
                        }else{
                            html2 = `<option name="subUnitType" value="-">-</option>`
                        }
                        $(".vehicle-node-select").append(html2);
                    } else {
                        continue;
                    }
                }
                
            })
        }
    });

    await axios.post('/driver/getSystemGroup').then(function (res) {
        let sysGroupList =  res.respMessage ? res.respMessage : res.data.respMessage;
        $(".vehicle-group-select").empty();
        let html = `<option></option>`
        for (let item of sysGroupList) {
            html += `<option name="unitGroup" value="${ item.id }">${ item.groupName }</option>`
        }
        $('.vehicle-group-select').append(html); 
    });

    $(".pmType1").val("1");
    $(".pmType2").val("2");
    if (action == 'create') {
        $('#view-vehicle-edit .modal-title').text('Create Vehicle');
        $(".opt-btn-div-create").show();
        $(".opt-btn-div-edit").hide();
    } else {
        $('#view-vehicle-edit .modal-title').text('Edit Vehicle');
        $(".opt-btn-div-create").hide();
        $(".opt-btn-div-edit").show();

        $("input[name='vehicleNo']").attr('readonly', 'readonly');
        await axios.post("/vehicle/getVehicleDetail", {vehicleNo: vehicleNo }).then(async res => {

            let respMessage = res.respMessage ? res.respMessage : res.data.respMessage;
            let vehicleDetail = respMessage.currentVehicle;
            //$("input[name='vehicleType']").val(vehicleDetail.vehicleType);

            $("select[name='vehicleHub']").val(vehicleDetail.unit);
            $('.vehicle-hub-select').trigger('change');

            $("select[name='vehicleNode']").val(vehicleDetail.subUnit ? vehicleDetail.subUnit : '-');
            $("input[name='vehicleMileage']").val(vehicleDetail.totalMileage);
            $("input[name='vehicleKeyTagId']").val(vehicleDetail.keyTagId);
            $("input[name='vehicleNo']").val(vehicleDetail.vehicleNo);
            $("select[name='vehicleGroup']").val(vehicleDetail.groupId);
            $("input[name='vehicleDimensions']").val(vehicleDetail.dimensions);
            $("input[name='vehicleSpeedlimit']").val(vehicleDetail.limitSpeed);
            $("input[name='aviDate']").val(vehicleDetail.nextAviTime ? moment(vehicleDetail.nextAviTime).format("YYYY-MM-DD")    : "");

            defaultVehicleType = vehicleDetail.vehicleType
            defaultPermitType = vehicleDetail.permitType
            defaultVehicleCategory = vehicleDetail.vehicleCategory
            $("input[name='vehicleCategory']").val(defaultVehicleCategory);

            $(".permit-type-input").show();
            $(".permit-type-select").hide();
            $(".permit-type-input").val(defaultPermitType);
            
            $(".vehicle-pmMaxMileage-input").val('');
            $(".vehicle-pmMonths-input").val('');
            if (vehicleDetail.pmMaxMileage && vehicleDetail.pmMaxMileage > 0) {
                $(".pmType1").attr("checked", true);
                $(".pmType2").attr("checked", false);
                $('.pm-by-distance-div').show();
                $('.pm-by-months-div').hide();

                $(".vehicle-pmMaxMileage-input").val(vehicleDetail.pmMaxMileage);
            }
            if (vehicleDetail.pmCycleMonth && vehicleDetail.pmCycleMonth > 0) {
                $(".pmType1").attr("checked", false);
                $(".pmType2").attr("checked", true);
                $('.pm-by-distance-div').hide();
                $('.pm-by-months-div').show();

                $(".vehicle-pmMonths-input").val(vehicleDetail.pmCycleMonth);
            }
        })
    }

    await axios.post("/vehicle/getTypeOfVehicle", {}).then(async res => {
        let vehicleTypeArray = res.data;
        if (!vehicleTypeArray) {
            vehicleTypeArray = res;
        }
        sysSupportVehicleTypeInfo = vehicleTypeArray;
        if (vehicleTypeArray && vehicleTypeArray.length > 0) {
            $('.vehicle-type-select').empty();
            let optionHtml = `<option value=""></option>`;
            for(let vehicleType of vehicleTypeArray) {
                if (defaultVehicleType && defaultVehicleType == vehicleType.vehicleName) {
                    optionHtml += `<option selected vehiclecategory="${vehicleType.category}" value="${vehicleType.vehicleName}">${vehicleType.vehicleName}</option>`
                } else {
                    optionHtml += `<option vehiclecategory="${vehicleType.category}" value="${vehicleType.vehicleName}">${vehicleType.vehicleName}</option>`
                }
            }

            $('.vehicle-type-select').append(optionHtml);
        }
    })

    $('.vehicle-type-select').on('change', function() {
        let vehicleType = $(this).val();
        if (vehicleType) {
            let vehicleCategory = $('.vehicle-type-select option:selected').attr('vehiclecategory');
            $("input[name='vehicleCategory']").val(vehicleCategory);

            let vehicleTypeObj = sysSupportVehicleTypeInfo.find(item => item.vehicleName == vehicleType);
            if (vehicleTypeObj) {
                $(".vehicle-dimensions-input").val(vehicleTypeObj.description);
                let permitType = vehicleTypeObj.vehicleClass;
                if (permitType) {
                    $(".permit-type-input").val(permitType);
                    $(".permit-type-input").show();
                    $(".permit-type-select").hide();
                } else {
                    initPermitTypeByVehicleType(permitType);
                }
            }
        } else {
            $("input[name='vehicleCategory']").val('');
            $(".vehicle-dimensions-input").val('');
        }
    });

    $(".vehicle-group-select").on('change', function() {
        let selectedGroup = $(this).val();
        if (selectedGroup) {
            $('.vehicle-hub-select').val("");
            $('.vehicle-hub-select').trigger('change');
        }
    });

    let defaultNextAviTime = $("input[name='aviDate']").val();
    defaultNextAviTime = defaultNextAviTime ? moment(defaultNextAviTime, 'YYYY-MM-DD').format('DD/MM/YYYY') : '';
    layui.use('laydate', function() {
        let laydate = layui.laydate;
        laydate.render({
            elem: '.avi-date-input',
            type: 'date',
            lang: 'en',
            format: 'dd/MM/yyyy',
            trigger: 'click',
            value: defaultNextAviTime,
            done: function(value){
                $(".avi-date-input").val(value);
            },
        });
    });
}

const initPermitTypeByVehicleType = async function(defaultPermitType) {
    $(".permit-type-input").val('');
    $(".permit-type-input").hide();
    $(".permit-type-select").show();
    await axios.post("/vehicle/getPermitTypeByVehicleType", {}).then(async res => {
        let permitTypeArray = res.data;
        if (!permitTypeArray) {
            permitTypeArray = res;
        }
        if (permitTypeArray && permitTypeArray.length > 0) {
            $('.permit-type-select').empty();
            let optionHtml = `<option value=""></option>`;
            for(let permitType of permitTypeArray) {
                if (defaultPermitType && defaultPermitType == permitType.permitType) {
                    optionHtml += `<option selected value="${permitType.permitType}">${permitType.permitType}</option>`
                } else {
                    optionHtml += `<option value="${permitType.permitType}">${permitType.permitType}</option>`
                }
            }

            $('.permit-type-select').append(optionHtml);
        }
    })
}

const cancleUploadFile = async function() {
    $(".upload-label-div").text('Drag & drop files to upload or click here to browse');
    currentUploadVehicleFile = null;
    $('#view-vehicle-resource-upload').modal('hide');
}

const confirmUploadFile = async function() {
    if (currentUploadVehicleFile == null) {
        $.alert({
            title: 'Warn',
            content: 'Please select excel file first!',
        });
        return;
    }
    let formData = new FormData();
    formData.append("file", currentUploadVehicleFile);

    let file = formData.get('file');

    
    axios.post("/upload/uploadVehicle ", formData, { "Content-Type": "multipart/form-data;"}).then(function (response) {
        if (response.respCode == 1) {
            $.alert({
                title: 'Info',
                content: response.respMessage,
            });
            cancleUploadFile();
            table.ajax.reload(null, true)
        } else {
            $.alert({
                title: 'Error',
                content: response.respMessage,
            });
        }
    }).catch(function (error) {
        console.log(error);
    });
}

const cancleEditVehicle = function() {
    editPageAction = '';
    $("#vehicle-info-form input").val('');
    $("#vehicle-info-form select").val('');

    $('#view-vehicle-edit').modal('hide');
    $(".edit-content-div").show();
    $(".success-div").hide();
}

const confirmCreateOrSave = async function(el) {
    let vehicleInfo = serializeToJson($("#vehicle-info-form").serializeArray())
    vehicleInfo.vehicleCategory = $("#vehicleCategory").val();

    let permitType = $(".permit-type-input").val();
    if (!permitType) {
        permitType = $(".permit-type-select").val();
    }
    vehicleInfo.permitType = permitType;
    vehicleInfo.vehicleDimensions = $(".vehicle-dimensions-input").val();
    if (!vehicleInfo.vehicleNode || vehicleInfo.vehicleNode == '-') {
        vehicleInfo.vehicleNode = "";
    }
    let checkResult = checkField(vehicleInfo);
    if (vehicleInfo.aviDate) {
        vehicleInfo.aviDate = moment(vehicleInfo.aviDate, 'DD/MM/YYYY').format('YYYY-MM-DD');
    }
    vehicleInfo.action = editPageAction
    if (checkResult) {
        $(el).addClass('btn-disabled')
        await axios.post("/vehicle/editVehicle", vehicleInfo).then(async res => {
            let respCode = (res.respCode != undefined) ? res.respCode : res.data.respCode;
            let respMessage = res.respMessage ? res.respMessage : res.data.respMessage;
            if (respCode == 1) {
                $(".edit-content-div input").val("");
                $(".edit-content-div select").val("");
                if (editPageAction == 'create') {
                    $(".edit-content-div").hide();
                    $(".success-div").show();
                }else {
                    $('#view-vehicle-edit').modal('hide');
                    initBasicProfileHandler();
                }
            } else {
                $.alert({
                    title: 'Error',
                    content: respMessage
                });
            }
        }).finally(() => {
            $(el).removeClass('btn-disabled')
        })
    }
}

const checkField = function(data) {
    let errorLabel = {
        vehicleNode: 'Node', vehicleHub: 'Hub', vehicleMileage: 'Odometer', vehicleNo: 'Vehicle No', vehicleSpeedlimit: 'Speed Limit',
        vehicleType: 'Vehicle Name', vehicleDimensions: 'Description', permitType: 'Permit Type'
    }

    for (let key in data) {
        if(key == 'vehicleNode' || key == 'vehicleKeyTagId' || key == 'pmType' || key == 'vehiclePmMaxMileage' || key == 'vehiclePmMonths' || key == 'permitTypeInput' || key == 'permitTypeSelect') continue
        if (key == 'vehicleHub' || key == 'vehicleGroup') {
            let hub = data['vehicleHub']
            let group = data['vehicleGroup']
            if (!hub && !group) {
                $.alert({
                    title: 'Warn',
                    content: "Hub or Unit is required.",
                });
                return false
            } else {
                continue;
            }
        }
        if (key != 'vehicleDimensions' && key != 'aviDate' && (data[key] == null || data[key] == "" || data[key].trim() == "")) {
            $.alert({
                title: 'Warn',
                content: errorLabel[key] + " is required.",
            });
            return false
        }
    }
    return true;
}

const backToList = function() {
    cancleEditVehicle();
}
const createContinue = function() {
    $(".edit-content-div").show();
    $(".success-div").hide();
}

const serializeToJson = function(data) {
    let s = {};
    data.forEach(a => {
        s[a["name"]] = a["value"]
    })
    return s
}