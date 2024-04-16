
let currentEditDriverId;
let permitTypeColor = ['#1b9063', '#194a37', '#fad028', '#1c8aea', '#21517b', '#133553'];
let permitInvalidReasons = ['Superseded', 'Revoke', 'Suspended', 'Disqualified', 'Deceased', 'Change Vocation', 'Pending Investigation'];
let driverStatusColor = [{status: 'Deployed', color: '#FAD028'}, {status: 'Loan Out', color: 'blue'}, {status: 'Deployable', color: '#6EB825'}, 
    {status: 'On Leave', color: '#3e3b3b'}, {status: 'Permit Invalid', color: 'red'}]

$(() => {
    currentEditDriverId = getParams("driverId");

    setTimeout(() => {
        $('.layui-driver-tab>ul>li:first').trigger('click')
    }, 500)
    
    initBasicProfileHandler();
    layui.use('element', function(){
        let chatTabElement = layui.element;
        chatTabElement.on('tab(docDemoTabBrief1)', function() {
            console.log(this.getAttribute('lay-id'));
            let layId = this.getAttribute('lay-id');
            if (layId === 'driver-1') {
                initBasicProfileHandler();
            } else if (layId === 'driver-2') {
                initDrivingRecordHandler();
            } else if (layId === 'driver-3') {
                initDriverIndentAssignedHandler();
            } else if (layId === 'driver-4') {
                initAchievementContentHandler();
            }
        });
    });

    $(".content-hide-show").on('click', function() {
        let divType = $(this).attr("divType");
        if ($(this).hasClass('active')) {
            $(this).removeClass('active')
            $(this).css('background-image', 'url(../images/show-green.svg)');
            if (divType == 'category') {
                $(".driverAssessmentContentDiv").hide();
            } else if (divType == 'platformConf') {
                $(".driverPlatformConfContentDiv").hide();
            } else if (divType == 'incident') {
                $(".incidentListDiv").hide();
            } else if (divType == 'permitTypeDetail') {
                $(".driverPermitTypeDetailContentDiv").hide();
            } else if (divType == 'licence') {
                $('.licenceListDiv').hide()
            }
        } else {
            $(this).addClass('active')
            $(this).css('background-image', 'url(../images/hide-green.svg)');
            if (divType == 'category') {
                $(".driverAssessmentContentDiv").show();
            } else if (divType == 'platformConf') {
                $(".driverPlatformConfContentDiv").show();
            } else if (divType == 'incident') {
                $(".incidentListDiv").show();
            } else if (divType == 'permitTypeDetail') {
                $(".driverPermitTypeDetailContentDiv").show();
            } else if (divType == 'licence') {
                $('.licenceListDiv').show()
            }
        }
    });
    
    $("#driverBaseinfoEditImg").on('click', function() {
        $("#driverBaseinfoEditImg").hide();
        $(".driver-permitNo").hide();
        $(".driver-permitDateOfIssue").hide();

        $("#driverBaseinfoSaveImg").show();
        $(".driver-permitNo-input").show();
        $(".driver-permitDateOfIssue-input").show();

        $("#driver-permitStatus-select").removeAttr('disabled')
        $("#driver-permitInvalidReason-select").removeAttr('disabled')
    });
    $("#driverBaseinfoSaveImg").on('click', async function() {
        await axios.post('/driver/updateDriverBaseinfo', { 
            driverId: currentEditDriverId, 
            permitNo: $(".driver-permitNo-input").val(), 
            permitIssueDate:  $(".driver-permitDateOfIssue-input").val() ? moment($(".driver-permitDateOfIssue-input").val(), 'DD/MM/YYYY').format('YYYY-MM-DD') : '',
            permitStatus: $("#driver-permitStatus-select").val(),
            permitInvalidReason: $("#driver-permitInvalidReason-select").val()
        }).then(res => {
            if (res.data.respCode == 1) {
                $("#driverBaseinfoEditImg").show();
                $(".driver-permitNo").show();
                $(".driver-permitDateOfIssue").show();

                $("#driverBaseinfoSaveImg").hide();
                $(".driver-permitNo-input").hide();
                $(".driver-permitDateOfIssue-input").hide();

                $('.driver-permitNo').html($(".driver-permitNo-input").val());
                $('.driver-permitDateOfIssue').html($(".driver-permitDateOfIssue-input").val());

                $("#driver-permitStatus-select").prop('disabled', 'disabled')
                $("#driver-permitInvalidReason-select").prop('disabled', 'disabled')
            } else {
                $.alert({
                    title: 'Warn',
                    content: res.data.respMessage,
                });
            }
        });
    });

    $("#driver-permitStatus-select").on('change', async function() {
        let permitStatus = $("#driver-permitStatus-select").val();
        $(".driverCurrentPermitStatus").text(permitStatus);
        $("#driver-permitInvalidReason-select").empty();
        if (permitStatus == 'invalid') {
            let optionsHtml = '';
            for (let temp of permitInvalidReasons) {
                optionsHtml += `<option value="${temp}">${temp}</option>`;
            }
            $("#driver-permitInvalidReason-select").append(optionsHtml);
        } else {
            $("#driver-permitInvalidReason-select").append('<option value="NA">NA</option>');
        }
    });

    //fixme: layui tab default load tab1;
    setTimeout(function () {
        let defaultTab = getParams("defaultTab");
        if (defaultTab && defaultTab == 'indents') {
            $('.layui-tab-title li').removeClass('layui-this');
            $('.layui-tab-title .driver-lay-indent').addClass('layui-this');
            $('.layui-tab-item').removeClass('layui-show');
            $('.layui-tab-item.layui-tab-item-indent').addClass('layui-show');

            initDriverIndentAssignedHandler(1);
        }
    }, 500)
})

const initBasicProfileHandler = async function () {
    const initBasicProfile = function (driver) {
        $('.driver-username').html(driver.driverName);
        $('.driverName-input').val(driver.driverName)

        $('.driver-status').html(driver.currentStatus);
        let statusColor = driverStatusColor.find(item => item.status == driver.currentStatus)
        if (!statusColor) {
            statusColor = 'orange';
        } else {
            statusColor = statusColor.color;
        }
        $('.driver-statusColor').css('background-color', statusColor);

        $('.driver-vocation').html(driver.vocation ? driver.vocation : '-');
        $('.driver-role').html(driver.role ? driver.role : '-');
        // $('.driver-unit').html(driver.unit ? driver.unit : '-');
        $('.driver-unit').html(driver.group ? driver.group : '-');
        $('.driver-nodeHub').html((driver.node ? driver.node : '-') + ', ' + (driver.hub ? driver.hub : '-'));
        $('.driver-enlistmentDate').html(driver.enlistmentDate ? moment(driver.enlistmentDate).format('DD/MM/YYYY') : '-');
        $('.driver-operationallyReadyDate').html(driver.operationallyReadyDate ? moment(driver.operationallyReadyDate).format('DD/MM/YYYY') : '-');
        $('.driver-nric').html(driver.nric ? ((driver.nric).toString()).substr(0, 1) + '****' + ((driver.nric).toString()).substr(((driver.nric).toString()).length-4, 4) : '-');
        $('.driver-birthday').html(driver.birthday ? moment(driver.birthday).format('DD/MM/YYYY') : '-');
        $('.driver-contactNo').html(driver.contactNumber ? driver.contactNumber : '-');
        // $('.driver-bloodType').html(driver.bloodType ? driver.bloodType : '-');
        // $('.driver-emergency-username').html(driver.emergencyList ? (driver.emergencyList.username ? driver.emergencyList.username : '-') : '-' );
        // $('.driver-emergency-nric').html(driver.emergencyList ? (driver.emergencyList.nric ? driver.emergencyList.nric : '-') : '-' );
        // $('.driver-emergency-relationship').html(driver.emergencyList ? (driver.emergencyList.relationship ? driver.emergencyList.relationship : '-') : '-' );
        // $('.driver-emergency-contactNo').html(driver.emergencyList ? (driver.emergencyList.contactNumber ? driver.emergencyList.contactNumber : '-') : '-');

        $('.driver-permitType').html(driver.permitType ? driver.permitType : '-');
        $('.driver-permitNo').html(driver.permitNo ? driver.permitNo : '-');
        $('.driver-permitDateOfIssue').html(driver.permitIssueDate ? moment(driver.permitIssueDate).format("DD/MM/YYYY") : '-');
        $('.driver-permitNo-input').val(driver.permitNo ? driver.permitNo : '');
        $('.driver-permitCategory').html(driver.category ? driver.category : '-');
        $('.driver-demeritPoints').html(driver.demeritPoints ? driver.demeritPoints : '0');

        $(".driverCurrentPermitStatus").text(driver.permitStatus);
        if (driver.permitStatus && driver.permitStatus == 'invalid') {
            $("#driver-permitStatus-select").val('invalid');
            $("#driver-permitInvalidReason-select").empty();
            let permitInvalidReason = driver.permitInvalidReason;
            let optionsHtml = '';
            for (let temp of permitInvalidReasons) {
                optionsHtml += `<option value="${temp}" ${temp == permitInvalidReason ? ' selected ' : ''}>${temp}</option>`;
            }
            $("#driver-permitInvalidReason-select").append(optionsHtml);
        }

        layui.use('laydate', function(){
            let laydate = layui.laydate;
            laydate.render({
                elem: '.driver-permitDateOfIssue-input',
                type: 'date',
                lang: 'en',
                format: 'dd/MM/yyyy',
                value: driver.permitIssueDate ? moment(driver.permitIssueDate).format("DD/MM/YYYY") : '',
                max: moment().format("DD/MM/YYYY"),
                trigger: 'click',
                btns: ['clear', 'confirm'],
                done: (value) => {
                }
            });
        });

        if (Cookies.get('userType') == 'HQ') {
            $('.edit-driver').show();
            $('.hoto-info').hide()
        } else {
            if (driver.hotoId) {
                $('.driver-info-container .edit-driver').hide();
                $('.hoto-info').show()
                $('.loan-info').hide()
            } else if (driver.loanId) {
                $('.driver-info-container .edit-driver').hide();
                $('.hoto-info').hide()
                $('.loan-info').show()
            } else {
                $('.driver-info-container .edit-driver').show();
                $('.loan-info').hide()
                $('.hoto-info').hide()
            }
            // if(Cookies.get('userType').toLowerCase() == 'customer') { 
            //     $('.driver-info-container .edit-driver').hide()
            //     $('#driverBaseinfoEditImg').hide()
            //     $('.btn-create-assessment').hide()
            //     $('.btn-create-platformconf').hide()
            //     $('.edit-driver-assessment').hide()
            //     $('.delete-driver-assessment').hide()
            //     $('.btn-create-vehicleClass').hide()
            //     $('.edit-driver-permitTypeDetail').hide()
            //     $('.delete-driver-permitTypeDetail').hide()
            // }
        }

        if(driver.nricShow) {
            $('.img-showNRIC').off('click').on('click', function(){  
                $('.img-noShowNRIC').show()
                $('.img-showNRIC').hide()
                $('.driver-nric').html(driver.nric ? driver.nric : '-');
            })
            $('.img-noShowNRIC').off('click').on('click', function(){
                $('.img-noShowNRIC').hide()
                $('.img-showNRIC').show()
                $('.driver-nric').html(driver.nric ? ((driver.nric).toString()).substr(0, 1) + '****' + ((driver.nric).toString()).substr(((driver.nric).toString()).length-4, 4) : '-');
            })
            $('.img-noShowNRIC').trigger('click')
        } else {
            $('.img-showNRIC').hide();
            $('.img-noShowNRIC').hide();
        }
    }
    const getBasicProfile = async function (driverId) {
        return await axios.post('/driver/getTODriverDetailInfo', { driverId }).then(result => { 
            if (result.data.respCode == -100) {
                window.location = '../login'
            } else if (result.data.respCode == 0) {
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
            } else {
                return result.data.respMessage
            }
        })
    }

    console.log('initBasicProfileHandler...')
    let driverBasicProfile = await getBasicProfile(currentEditDriverId);
    initBasicProfile(driverBasicProfile);

    initDriverAssignedIndent();
}

const initDrivingRecordHandler = async function () {
    initMileageStatInfo(currentEditDriverId);
}

const initDriverIndentAssignedHandler = async function (showMileageWarning) {
    console.log('initDriverIndentAssignedHandler...')
    
    initDriverAssignedIndent(showMileageWarning);
}

const initAchievementContentHandler = async function () {
    console.log('initAchievementContent...')
    initAchievementContent();
}

const initMileageStatInfo = function(driverId) {
    axios.post('/driver/getDriverMileageStatInfo', {
        driverId: driverId
    }).then(res=>{
        let driverMileageStatInfo=[];
        let driverMileageStat = res.data.respMessage;
        let currentTaskMileage = driverMileageStat.currentTaskMileage;
        let totalMileage = driverMileageStat.driverTotalMileage

        let permitTypeStat = driverMileageStat.statResult;
        
        $(".driverTotalMileageLabel").text(formatNumber(totalMileage));
        $("#cardTotalMileageLabel").text(formatNumber(totalMileage));

        //let newPermitTypeStat = permitTypeStat.filter(item => (item.totalMileage != 0 && item.totalMileage != null));
        buildSimplePermitTypeMileageHtml(totalMileage, permitTypeStat);
        buildDetailPermitTypeMileageHtml(totalMileage, permitTypeStat);
    })
}

const buildSimplePermitTypeMileageHtml = function(totalMileage, permitTypeStat) {
    let $percentageDiv = $(".percentage-div");
    let $allPercentageDiv = $(".all-percentage-div");

    let index = 0;
    let maxIndex = permitTypeStat.length - 1;
    let allPerHtml = ``;
    let perItemHtml = ``;
    let perItemRowNum = Math.ceil(permitTypeStat.length / 4);
    let rowItem = 1;
    for (let permitType of permitTypeStat) {
        let percent = permitType.totalMileage * 100 / totalMileage;
        if (index == 0) {
            allPerHtml += `
                <div style="width: ${percent}%;height: 100%;height: 10px;border-radius: 10px 0 0 10px;background-color: ${permitTypeColor[index]};"></div>
            `;
        } else if (index == maxIndex) {
            allPerHtml += `
                <div style="width:  ${percent}%;height: 100%;height: 10px;border-radius: 0 10px 10px 0;background-color: ${permitTypeColor[index]};"></div>
            `;
        } else {
            allPerHtml += `
                <div style="width: ${percent}%;height: 100%;height: 10px;background-color: ${permitTypeColor[index]};"></div>
            `;
        }

        if (rowItem == 1) {
            perItemHtml += `<div class="mx-3 mt-2 color-templete-div" style="display: flex;">`;
        }
        perItemHtml += `
            <div class="color-templete-item fs-6" style="width: 25%;height: 30px;display: flex;justify-content: start;align-items: center;">
                <div style="width: 10%;border: 1px solid #f2f8f6;border-radius: 10px;width: 10px;height: 10px;background-color: ${permitTypeColor[index]};"></div>
                <div class="px-2" style="width: 90%;display: flex;">
                    <label style="color: #3f3f3f;font-weight:600;">${formatNumber(permitType.totalMileage)}</label>
                    <label style="color: #838383;margin-left: 10px;">${permitType.permitType}</label>
                </div>
            </div>
        `;
        
        if (rowItem == 4){
            perItemHtml += `</div>`;
            rowItem = 1;
        } else {
            rowItem++;
        }
        index++;
    }
    $allPercentageDiv.empty();
    $allPercentageDiv.append(allPerHtml);
    $percentageDiv.find(".color-templete-div").remove();
    $percentageDiv.append(perItemHtml);
}

const buildDetailPermitTypeMileageHtml = function(totalMileage, permitTypeStat) {
    let $permitTypeDiv = $(".permit-type-div");
    let baseHtml = ``;
    let index = 0;
    let columnIndex = 1;
    for (let permitType of permitTypeStat) {
        let percent = permitType.totalMileage * 100 / permitType.eligibilityMileage;
        if (columnIndex == 1) {
            baseHtml += `<div class="permit-type-line" style="width: 100%; display: flex;">`;
        }
        let moreMileage = permitType.totalMileage < permitType.eligibilityMileage ? permitType.eligibilityMileage - permitType.totalMileage : 0;
        baseHtml += `
            <div class="permit-type-stat-info fs-6" style="margin-top: 15px;">
                <div class="permit-type-info">
                    <div class="px-3" style="width: 100%;color: #1b7c5b;width: 100%;display: flex;margin-top: 20px;">
                        <div style="width: 50%;display: flex;">
                            <label class="permit-type-label">${permitType.permitType} Eligibility</label>
                        </div>
                    </div>
                    <div class="px-3" style="width: 100%;height: 30px;margin-top: 15px;">
                        <div style="display: flex;height: 10px;border-radius: 10px;background-color: #e5e5e5;">
                            <div tooltip="${percent}%" style="width: ${percent}%;height: 100%;height: 10px;border-radius: 10px;background-color: ${permitTypeColor[index]};"></div>
                        </div>
                    </div>
                    <div class="px-3" style="width: 100%;color: #1b7c5b;width: 100%;display: flex;">
                        <div style="color: white;width: 60%;height: 100%;display: flex;justify-content: start;align-items: center;">
                            <label style="font-weight: 600;color: #262626;">${formatNumber(moreMileage)} km</label>
                            <label style="color: #a2a2a2;">&nbsp more on ${permitType.permitType}</label>
                        </div>
                        <div style="width: 40%;display: flex;justify-content: flex-end;align-items: center;">
                            <label>${formatNumber(permitType.totalMileage)}</label>
                            <label class="driverTotalMileageLabel" style="color: #c5c5c5;">/${formatNumber(permitType.eligibilityMileage)} km</label>
                        </div>
                    </div>
                </div>
            </div>
        `;
        if (columnIndex == 1) {
            baseHtml += `<div style="width: 2%;"></div>`;
        }
        index++;
        if (columnIndex != 1 || permitTypeStat.length == index) {
            baseHtml += `</div>`;
        }
        
        if (columnIndex == 1) {
            columnIndex++;
        } else {
            columnIndex = 1;
        }
    }

    $permitTypeDiv.empty();
    $permitTypeDiv.append(baseHtml);
}

const formatNumber = function(str, splitStr = ',') {
    if (!str || str == 0) return '0.0';
    str += '';
    let pointNumArray = str.split('.');
    let pointStr = '';
    if (pointNumArray.length > 1) {
        str = pointNumArray[0];
        pointStr = '.' + pointNumArray[1];
    }
    str = str.split("").reverse().reduce((prev, next, index) => {
        return ((index % 3) ? next: (next + splitStr)) + prev;
    });
    return str + pointStr;
}

const getParams = function(key) {
    let reg = new RegExp("(^|&)" + key + "=([^&]*)(&|$)");
    let r = window.location.search.substr(1).match(reg);
    if (r != null) {
        return unescape(r[2]);
    }
    return null;
};