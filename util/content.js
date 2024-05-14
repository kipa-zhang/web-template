module.exports = {
    USER_TYPE: {
        ADMINISTRATOR: 'ADMINISTRATOR',
        HQ: 'HQ',
        UNIT: 'UNIT',
        LICENSING_OFFICER: 'LICENSING OFFICER',
        MOBILE: 'MOBILE',
        CUSTOMER: 'CUSTOMER'
    },
    ViolationType: {
		HardBraking: 'Hard Braking',
		RapidAcc: 'Rapid Acc',
		Speeding: 'Speeding',
		IDLETime: 'IDLE Time',
		Missing: 'Missing',
        NoGoZoneAlert: 'No Go Zone Alert'
	},
    INCIDENT_STATUS: {
        OPEN: 'OPEN',
        RE_ROUTED: 'RE-ROUTED',
        DISMISSED: 'DISMISSED'
    },
    ROUTE_STATUS: {
        NEW: 'NEW',
        ASSIGNING: 'ASSIGNING',
        ASSIGNED: 'ASSIGNED',
    },
    CONVOY_STATUS: {
        NEW: 'NEW',
        ASSIGNING: 'ASSIGNING',
        ASSIGNED: 'ASSIGNED',
    },
    CONVOY_STATE: {
        YET_TO_START: 'YET TO START',
        ON_TIME: 'ON-TIME',
        LATE: 'LATE',
        UNCONTACTABLE: 'UNCONTACTABLE',
        ARRIVED: 'ARRIVED',
        YET_TO_START_DELAYED: 'YET TO START (DELAYED)',
        STOPPED: 'STOPPED',
    },
    RECORD_STATE: {
        ASSIGNED: 'assigned',
        UN_ASSIGNED: 'un_assigned',
        REJECTED: 'rejected',
        APPROVED: 'approved',
        INCIDENT_MSG: 'incident_msg',
        INCIDENT_UPDATE: 'incident_update',
        INCIDENT_ROUTE: 'incident_route',
        INCIDENT_NEW: 'incident_new',
        INCIDENT_DELETE: 'incident_delete',
    }
};