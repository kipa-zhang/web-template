$(function () {
    initOverview( Cookies.get('selectedUnit') ? Cookies.get('selectedUnit') : Cookies.get('hub'), Cookies.get('selectedSubUnit') ? Cookies.get('selectedSubUnit') : Cookies.get('node'));

    $(".overview-task-tooltip").tooltip();
});

const initOverview = async function (hub, node) {
    let overviewData = await getOverview(hub, node);
    if (overviewData) {
        let vehicleData = overviewData.vehicleData;
        let tsOperatorData = overviewData.tsOperatorData;
            
        initData('#vehicleCancelled', vehicleData.vehicleTodayCancelled, vehicleData.vehicleTodayAssigned, '#vehicleCancelledPercentage');
        initData('#vehicleEnded', vehicleData.vehicleTodayCompleted, vehicleData.vehicleTodayAssigned, '#vehicleTodayCheckedPercentage');
        initData('#vehicleYetToStartTask', vehicleData.vehicleTodayYetToStart, vehicleData.vehicleTodayAssigned, '#vehicleYetToStartTaskPercentage');
        initData('#vehicleOnGoing', vehicleData.vehicleTodayStarted, vehicleData.vehicleTodayAssigned, '#vehicleOnGoingPercentage');
        // initData('#vehicleOwnedWithMe', vehicleData.vehicleOwnedWithMe, vehicleData.vehicleOwned, '#vehicleOwnedWithMePercentage');
        initData('#vehicleAssigned', vehicleData.vehicleDeployed, vehicleData.vehicleOwned, '#vehicleAssignedPercentage');
        initData('#vehicleUnAssigned', vehicleData.vehicleDeployable, vehicleData.vehicleOwned, '#vehicleUnAssignedPercentage');
        initData('#vehicleMaintenance1', vehicleData.vehicleMaintenance, vehicleData.vehicleOwned, '#vehicleMaintenancePercentage');
        initData('#vehicleOutOfService1', vehicleData.vehicleOutOfService, vehicleData.vehicleOwned, '#vehicleOutOfServicePercentage');
        
        initData('#vehicleOwned', vehicleData.vehicleOwned);
        initData('#vehicleTodayAssigned', vehicleData.vehicleTodayAssigned + " / "  + vehicleData.vehicleTodayAssignedEffective + " / " + vehicleData.vehicleDeployed);
        // initData('#vehicleAvailable', vehicleData.vehicleAvailable);
        // initData('#vehicleOwned-available', '/' + vehicleData.vehicleOwned);
        initData('#vehicleDeployed', vehicleData.vehicleDeployed);
        initData('#vehicleDeployable', vehicleData.vehicleDeployable);
        initData('#vehicleMaintenance', vehicleData.vehicleMaintenance);
        initData('#vehicleOutOfService', vehicleData.vehicleOutOfService);
        // initData('#vehicleOnhold', vehicleData.vehicleOnhold);

        initData('#tsOperatorYetToStartTask', tsOperatorData.tsOperatorTodayYetToStart, tsOperatorData.tsOperatorTodayAssigned, '#tsOperatorYetToStartTaskPercentage');
        initData('#tsOperatorOnGoing', tsOperatorData.tsOperatorTodayStarted, tsOperatorData.tsOperatorTodayAssigned, '#tsOperatorOnGoingPercentage');
        initData('#tsOperatorEnded', tsOperatorData.tsOperatorTodayCompleted, tsOperatorData.tsOperatorTodayAssigned, '#tsOperatorEndedPercentage');
        initData('#tsOperatorCancelled', tsOperatorData.tsOperatorTodayCancelled, tsOperatorData.tsOperatorTodayAssigned, '#tsOperatorCancelledPercentage');
        initData('#tsOperatorTodayAssigned', tsOperatorData.tsOperatorTodayAssigned + " / " + tsOperatorData.tsOperatorTodayAssignedEffective + " / " + tsOperatorData.tsOperatoreDeployed);
        initData('#tsOperatorPermitInvalid', tsOperatorData.tsOperatorInvalid ? tsOperatorData.tsOperatorInvalid : 0, tsOperatorData.tsOperatorOwned, '#tsOperatorPermitInvalidPercentage');

        initData('#tsOperatorOwned', tsOperatorData.tsOperatorOwned);
        initData('#tsOperatorAssigned', tsOperatorData.tsOperatoreDeployed, tsOperatorData.tsOperatorOwned, '#tsOperatorAssignedPercentage');
        initData('#tsOperatorUnAssigned', tsOperatorData.tsOperatorDeployable, tsOperatorData.tsOperatorOwned, '#tsOperatorUnAssignedPercentage');
        initData('#tsOperatorOnleave1', tsOperatorData.tsOperatorOnleave, tsOperatorData.tsOperatorOwned, '#tsOperatorOnleavePercentage');
        initData('#tsOperatorDeployable', tsOperatorData.tsOperatorDeployable);
        initData('#tsOperatorOnleave', tsOperatorData.tsOperatorOnleave);
        initData('#tsOperatorDeployed', tsOperatorData.tsOperatoreDeployed);
        initData('#tsOperatorInvalid', tsOperatorData.tsOperatorInvalid ? tsOperatorData.tsOperatorInvalid : 0);

        if (Cookies.get('userType') != 'CUSTOMER') {
            $(".loanOutDiv").show();
            initData('#vehicleLoanOut', vehicleData.vehicleLoanout ? vehicleData.vehicleLoanout : 0);
            initData('#vehicleLoanOut1', vehicleData.vehicleLoanout ? vehicleData.vehicleLoanout : 0, vehicleData.vehicleOwned, '#vehicleLoanOutPercentage');

            initData('#driverLoanOut', tsOperatorData.tsOperatoreLoanOut ? tsOperatorData.tsOperatoreLoanOut : 0);
            initData('#tsOperatorLoanOut', tsOperatorData.tsOperatoreLoanOut ? tsOperatorData.tsOperatoreLoanOut : 0, tsOperatorData.tsOperatorOwned, '#tsOperatorLoanOutPercentage');
        }
    } 
}

const roundNumbers = function (total, start) {
    return ((total/start)*100);
}

const initData = function (vehicleId, idData, total, idPercentage) {
    $(vehicleId).empty();
    $(vehicleId).html(idData);
    if(idPercentage && total) {
        if(idData == 0) {
            $(idPercentage).css('width', '0%');
            return
        }
        let idPercentageData = roundNumbers(idData, total);
        $(idPercentage).css('width', idPercentageData + '%');
    }else {
        if(total == 0 || idData == 0) {
            $(idPercentage).css('width', '0%');
        }
    }
}

const getOverview = async function (hub, node) {
    // debugger
    return axios.post("/getResourcesStatData", { unit: hub, subunit: node }).then(function (res) {
        let resultCode = res.data ? res.data.respCode : res.respCode
        let resultData = res.respMessage ? res.respMessage : res.data.respMessage

        if (resultCode == -100) {
            window.location = '../login'
        }

        if (resultCode == '1') {
            return resultData;
        } else {
            $.confirm({
                title: 'Error',
                content: resultData,
                buttons: {
                    cancel: function () {
                    }
                }
            });
        }
        return null;
    });
}

window.reloadHtml = function (hub, node) {
    // console.log(Cookies.get('selectedUnit'))
    // console.log(Cookies.get('selectedSubUnit'))
    let unit = Cookies.get('selectedUnit') ? Cookies.get('selectedUnit') : Cookies.get('hub')
    let subUnit = Cookies.get('selectedSubUnit') ? Cookies.get('selectedSubUnit') : Cookies.get('node')
    initOverview(unit ? unit : hub, subUnit ? subUnit : node)
    // window.location.reload();
}