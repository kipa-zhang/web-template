let vehicleOddTable

$(function () {
    $("#oddRectifyConfirm").on('click', function() {
        let remarks = $(".odd-remarks-input").val();
        let oddId = $("#oddId").val();

        axios.post("/vehicle/oddRectify", {oddId: oddId, remarks: remarks }).then(async res => {
            if (res.data.respCode == 1) {
                $("#oddId").val('');
                $(".odd-remarks-input").val('');
                $("#odd-rectify").modal('hide');
            } else {
                $.alert({
                    title: 'Error',
                    content: res.data.respMessage,
                });
            }

            vehicleOddTable.ajax.reload(null, true);
        });

    });
    $("#oddRectifyCancel").on('click', function() {
        $("#oddId").val('');
        $(".odd-remarks-input").val('');
        $("#odd-rectify").modal('hide');
    });
})

const initOdd = function() {

    let columns = [
        {
            "data": "creatorName", "title": "Submitted By"
        },
        {
            "data": "content", "title": "Description",
            "render": function (data, type, full, meta) {
                let dataStr = data;
                if (dataStr && dataStr.length > 20) {
                    dataStr = dataStr.substring(0, 20) + '...';
                    return `<a style="color: #0d6efd;text-decoration: underline;" onclick="alertDescription('Description', '${data}')" title="Rectify">${dataStr}</a>`
                } else {
                    return data;
                }
            }
        },
        {
            "data": "vehicleNo", "title": "Vehicle Number"
        },
        {
            "data": "vehicleType", "title": "Vehicle Type"
        },
        {
            "data": "createdAt", "title": "DateTime",
            "render": function (data, type, full, meta) {
                if (data) {
                    return moment(data).format("DD/MM/YYYY HH:mm:ss");
                }
                return "-"
            }
        },
        {
            "data": "retifyByName", "title": "Rectify By",
            "render": function (data, type, full, meta) {
                if (data) {
                    return data;
                }
                return "-"
            }
        },
        {
            "data": "remarks", "title": "Remarks",
            "render": function (data, type, full, meta) {
                if (full.rectifyBy) {
                    let dataStr = data;
                    if (dataStr && dataStr.length > 20) {
                        dataStr = dataStr.substring(0, 20) + '...';
                        return `<a style="color: #0d6efd;text-decoration: underline;" onclick="alertDescription('Remarks', '${data}')" title="Rectify">${dataStr}</a>`
                    } else {
                        return data;
                    }
                } else {
                    // if (Cookies.get('userType') == 'CUSTOMER') {
                    //     return '-';
                    // }
                    return `<button class="btn btn-sm" style="border: solid 1px grey;" onclick="recfify('${full.id}')" title="Rectify">Rectify</button>`
                }
                
            }
        }
    ];

    if (!Cookies.get('node')) {
        columns.unshift({
            "data": "subUnit", "title": "Node",
            "render": function (data, type, full, meta) {
                if (data) {
                    return data;
                }
                return '-'
            }
        },);
        columns.unshift({
            "data": "unit", "title": "Hub",
            "render": function (data, type, full, meta) {
                if (data) {
                    return data;
                }
                return '-'
            }
        });
    }

    vehicleOddTable = $('.vehicle-odd-table').DataTable({
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
            url: "/vehicle/getVehicleOdd",
            type: "POST",
            data: function (d) {
                let params = {vehicleNo: currentVehicleNo}
                return params
            },
        },
        "columns": columns
    });
}

const recfify = function(oddId) {
    $("#oddId").val(oddId);
    $("#odd-rectify").modal('show');
}

const alertDescription = function(title, content) {
    $.alert({
        title: title,
        content: content,
    });
}