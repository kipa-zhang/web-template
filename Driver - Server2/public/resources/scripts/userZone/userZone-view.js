

$(function () {
    
})

let userZoneList = [];
let userZoneViewDataTable = null;

export async function initUserZoneList () {
    const getUserZoneList = function () {
        return axios.post('/zone/getUserZoneList')
            .then(function (res) {
                if (res.respCode === 1) {
                    return res.respMessage
                } else {
                    console.error(res.respMessage)
                    return []
                }
            });
    } 
    
    userZoneList = await getUserZoneList();
    return userZoneList;
};

export async function initUserZoneViewPage () {
    await initUserZoneList();
    userZoneViewDataTable = $('#userZone-list').DataTable({
        destroy: true,
        data: userZoneList,
        columns: [
            { title: 'Selected', data: null, render: function (data, type, row, meta) {
                return `
                    <div class="form-check">
                        <input class="form-check-input" type="checkbox" value="">
                    </div>
                `
            } },
            { title: 'Zone ID', data: 'id', sortable: true },
            { title: 'Zone Name', data: 'zoneName', defaultContent: '-', sortable: true },
            { title: 'User Name', data: 'owner', defaultContent: '-', sortable: false },
            { title: 'Edit', data: null, render: function (data, type, row, meta) {
                return `
                    <button type="button" class="btn btn-primary custom-btn">Edit</button>
                    <button type="button" class="btn btn-primary custom-btn">Delete</button>
                `;
            } },
        ],
        bFilter: false,
        bInfo: false,
        lengthChange: false,
        // searching: true,
        pageLength: 10,
        // scrollY: 200,
        // scrollCollapse: true,
        stateSave: true, // keep page, searching, filter
    });

    // show view userzone module
    $('#view-userZone').modal('show');
}

