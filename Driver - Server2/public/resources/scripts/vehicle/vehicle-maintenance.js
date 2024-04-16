let maintenancetable

$(function () {
    $(".search-input-odd").on("keyup", function() {
        maintenanceFilterOnChange();
    });

    $("#aviDateConfirm").on('click', function() {
        let newAviDate =$("#aviDateInput1").val();

        axios.post("/vehicle/updateVehicleAviDate", {currentVehicleNo: currentVehicleNo, newAviDate: newAviDate }).then(async res => {
            if (res.data.respCode == 1) {
                $("#aviDateInput1").val('');
                $("#avi-date-update").modal('hide');

                maintenancetable.ajax.reload(null, true);
            } else {
                $.alert({
                    title: 'Error',
                    content: res.data.respMessage,
                });
            }
        });

    });
    $("#aviDateCancel").on('click', function() {
        $("#aviDateInput1").val('');
        $("#avi-date-update").modal('hide');
    });
})

const initMaintenance = function() {
    maintenancetable = $('.vehicle-maintenance-table').DataTable({
        "ordering": false,
        "searching": false,
        "paging": false,
        "autoWidth": false,
        "fixedHeader": true,
        "scrollCollapse": true,
        "language": PageHelper.language(),
        "lengthMenu": PageHelper.lengthMenu(),
        "dom": '',
        "processing": true,
        "serverSide": true,
        "destroy": true,
        "sAjaxDataProp": "respMessage",
        "ajax": {
            url: "/vehicle/getVehicleMaintenance",
            type: "POST",
            data: function (d) {
                let params = {vehicleNo: currentVehicleNo}
                return params
            },
        },
        "columns": [
            {
                "data": "type", "title": "Type",
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `<label style='font-weight: 600;'>${data}</label>`;
                    }
                    return '-'
                }
            },
            {
                "data": "dueTime", "title": "Due On",
                "render": function (data, type, full, meta) {
                    let html = '';
                    if (data) {
                        let currentDateStr = moment().format("YYYY-MM-DD");
                        if (full.type != 'PM') {
                            if (data < currentDateStr && !full.exeTime) {
                                html += `<img alt="" style="width: 20px; cursor: pointer; margin-top: -2px; padding-right: 4px;" src="../images/warn-mileage.svg">`;
                            }
                        }
                        html += `<span class="vehicle-avi-time-span" style="color: #6c757d;">${moment(data).format("DD/MM/YYYY")}</span>`;
                    }
                    if (full.type == 'AVI' && full.maintenanceOperationList && full.maintenanceOperationList.includes('Edit AVI Date')) {
                        html += `<img alt="" class="vehicle-avi-time" onclick="configAviTime('${data ? moment(data).format("YYYY-MM-DD") : ''}')" role="button" style="width: 20px; cursor: pointer; margin-top: -4px; padding-left: 4px;" src="../images/edit.svg">`;
                    }
                    return html ? html : '-';
                }
            },
            {
                "data": "remarks", "title": "Remarks"
            },
            {
                "data": "exeTime", "title": "Status",
                "render": function (data, type, full, meta) {
                    if (data) {
                        return `Completed on: ${moment(data).format("DD/MM/YYYY HH:mm:ss")}`;
                    }
                    return '-'
                }
            },
        ]
    });
}

const maintenanceFilterOnChange = async function () {
    await maintenancetable.ajax.reload(null, true)
}

const configAviTime = async function (currentAviDate) {
    $("#avi-date-update").modal('show');
    layui.use('laydate', function() {
        let laydate = layui.laydate;
        laydate.render({
            elem: '#aviDateInput1',
            type: 'date',
            lang: 'en',
            trigger: 'click',
            value: currentAviDate,
            done: function(value){
                $("#aviDateInput1").val(value);
            },
        });
    });
}