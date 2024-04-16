import { customPopupInfo, customConfirm } from '../common-script.js'
import { initRouteCreatePage } from './route-create.js'

$(function () {
    $('#view-route .route-search').on('keyup', _.debounce(() => {
        filterOfRouteName = $('#view-route .route-search').val();
        initDataTables();
    }, 500))
})

let filterOfRouteName;
let routeList = [];
let routeViewDataTable = null;

export async function initRouteList () {
    const getRouteList = function () {
        return axios.post('/route/getRouteList', { routeName: filterOfRouteName })
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    routeList = await getRouteList();
    return routeList;
};

const initDataTables = async function () {
    await initRouteList();
    routeViewDataTable = $('#route-list').DataTable({
        destroy: true,
        data: routeList,
        columns: [
            // { title: 'Selected', data: null, render: function (data, type, row, meta) {
            //     return `
            //         <div class="form-check">
            //             <input class="form-check-input" type="checkbox" value="">
            //         </div>
            //     `
            // } },
            { title: 'Route No', data: 'routeNo', sortable: true },
            { title: 'Route Name', data: 'routeName', defaultContent: '-', sortable: true },
            { title: 'From Address', data: 'fromAddress', defaultContent: '-', sortable: false },
            { title: 'To Address', data: 'toAddress', defaultContent: '-', sortable: false },
            { title: 'Action', data: null, width: '30%', render: function (data, type, row, meta) {
                let html = ``
                let operationList = row.operation.split(',')
                if (operationList.includes('Edit')) {
                    html += ` <button type="button" class="btn btn-primary btn-sm custom-btn-green" onclick="editRoute('${ row.routeNo }')">Edit</button> `
                }
                if (operationList.includes('Delete')) {
                    html += ` <button type="button" class="btn btn-primary btn-sm custom-btn-danger" onclick="deleteRoute('${ row.routeNo }')">Delete</button> `
                }
                if (operationList.includes('Duplicate')) {
                    html += ` <button type="button" class="btn btn-primary btn-sm custom-btn-blue px-1" onclick="copyRoute('${ row.routeNo }')">Duplicate</button> `
                }

                return html
            } },
        ],
        bFilter: false,
        bInfo: false,
        autoWidth: false,
        lengthChange: false,
        // searching: true,
        pageLength: 8,
        // scrollY: 200,
        // scrollCollapse: true,
        // stateSave: true, // keep page, searching, filter
    });
}

export async function initRouteViewPage () {
    initDataTables();

    // show view route module
    $('#view-route').modal('show');
    $('.offcanvas').offcanvas('hide')
}

window.editRoute = function (routeNo) {
    initRouteCreatePage({ routeNo }, 1)
    $('#view-route').modal('hide');
    $('#createRouteLabel').html('Edit Route')
}

window.deleteRoute = function (routeNo) {
    customConfirm('Confirm', 'Are you sure to delete this route ?', async function () { 
        axios.post('/route/deleteRoute', { routeNo })
            .then(function (res) {
                if (res.respCode === 1) {
                    initRouteViewPage();
                } else {
                    // alert(res.respMessage)
                    customPopupInfo('Attention', res.respMessage)
                }
            });
    })
    
}

window.copyRoute = function (routeNo) {
    axios.post('/route/copyRoute', { routeNo })
        .then(function (res) {
            if (res.respCode === 1) {
                initRouteViewPage();
            } else {
                // alert(res.respMessage)
                customPopupInfo('Attention', res.respMessage)
            }
        });
}


