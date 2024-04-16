$(function () {

})

let mileageList = []

const initMileageList = function () {
	return axios.post('/getMileageList').then(function (res) {
		if (res.respCode === 1) {
			mileageList = res.respMessage;
			return res.respMessage;
		} else {
			console.error(res.respMessage);
			return [];
		}
	});
}

const initMileageDataTable = function (mileageList) {
	let dataTable = $('#mileage-list').DataTable({
		destroy: true,
		data: mileageList,
		columns: [
			{ title: 'Date', data: 'date' },
			{
				title: 'Start Time', data: 'startTime', render: function (data, type, row, meta) {
					if (data) {
						return moment(data).format("HH:mm:ss");
					} else {
						return '-'
					}
				}
			},
			{
				title: 'End Time', data: 'endTime', render: function (data, type, row, meta) {
					if (data) {
						return moment(data).format("HH:mm:ss");
					} else {
						return '-'
					}
				}
			},
			{ title: 'Vehicle No', data: 'vehicleNo', 'defaultContent': '-' },
			{ title: 'Device Id', data: 'deviceId', 'defaultContent': '-' },
			{ title: 'Driver Name', data: 'driverName', 'defaultContent': '-' },
			// {
			// 	title: 'Mileage Start', data: 'mileageStart', render: function (data, type, row, meta) {
			// 		return data / 1000;
			// 	}
			// },
			// {
			// 	title: 'Mileage End', data: 'mileageEnd', render: function (data, type, row, meta) {
			// 		return data / 1000;
			// 	}
			// },
			{
				title: 'Mileage Traveled', data: 'mileageTraveled', render: function (data, type, row, meta) {
					return data / 1000;
				}
			},
		],
		bFilter: false,
		bInfo: false,
		bLengthChange: false
	});
	dataTable.page.len(8).draw();

	$('.tb_mileage_info').find('th[class="sorting_asc"]').removeClass('sorting_asc');
}

export async function initMileageViewPage () {
	await initMileageList();
	initMileageDataTable(mileageList);
	$('#view-mileage').modal('show')
	initMileageSearchEventHandler();
}

const initMileageSearchEventHandler = async function () {
    const searchMileageList = function () {
        let newList = [];
        mileageList.forEach(function (mileage) {
            if (mileage.driverName.indexOf($('.mileage-search').val()) >= 0) {
                newList.push(mileage);
            }
        });
		initMileageDataTable(newList)
    }

    $('.mileage-search').on("keyup", _.debounce(searchMileageList, 500));
};