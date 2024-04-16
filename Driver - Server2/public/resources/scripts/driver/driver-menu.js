import { initUserZoneViewPage } from '../userZone/userZone-view.js'
import { initUserZoneCreatePage } from '../userZone/userZone-create.js'
import { initRouteViewPage } from '../route/route-view.js'
import { initRouteCreatePage } from '../route/route-create.js'
import { initDriverTaskViewPage } from '../driver/driver-view.js'
import { initDriverTaskCreatePage } from '../driver/driver-create.js'
import { initDriverUploadPage } from '../driver/driver-upload.js'
import { initVehicleUploadPage } from '../vehicle/vehicle-upload.js'
import { initWaypointUploadPage } from '../waypoint/waypoint-upload.js'
import { initMobileSettingPage } from '../mobile/mobile-setting.js'
import { initIncidentViewPage } from '../incident/incident-view.js'
import { initMileageViewPage } from '../mileage/mileage-view.js'
import { initLocationViewPage } from '../location/location-view.js'

$(function () {
    initDriverMenuEventHandler();
})

const initDriverMenuEventHandler = function () {
    $('#offcanvasRight .list-group-item, .menu2-item, .menu2-item2').on('click', function () {
        let action = $(this).data('action')


        switch (action) {
            case 'userZone-create':
                initUserZoneCreatePage();
                break;
            case 'userZone-view':
                initUserZoneViewPage();
                break;
            case 'route-create':
                $('title').html('Create Route')
                initRouteCreatePage();
                break;
            case 'route-view':
                $('title').html('Edit Route')
                initRouteViewPage();
                break;
            case 'driverTask-create':
                initDriverTaskCreatePage();
                break;
            case 'driverTask-view':
                initDriverTaskViewPage();
                break;
            case 'driver-upload':
                initDriverUploadPage();
                break;
            case 'vehicle-upload':
                initVehicleUploadPage();
                break;
            case 'waypoint-upload':
                initWaypointUploadPage();
                break;
            case 'location-create':
                // initLocationViewPage();
                break;
            case 'location-view':
                initLocationViewPage();
                break;
            case 'incident-view':
                initIncidentViewPage();
                break;
            case 'set-mobile':
                initMobileSettingPage();
                break;
            case 'mileage-view':
                initMileageViewPage();
                break;
        }

    })
}