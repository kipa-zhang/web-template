module.exports = {
    ViolationType: {
		HardBraking: 'Hard Braking',
		RapidAcc: 'Rapid Acc',
		Speeding: 'Speeding',
		IDLETime: 'IDLE Time',
        Missing: 'Missing',
        NoGoZoneAlert: 'No Go Zone Alert'
	},
    INCIDENT_STATUS: {
        NEW: 'NEW',
        OPEN: 'OPEN',
        RE_ROUTED: 'RE-ROUTED',
        DISMISSED: 'DISMISSED'
    },
    DEVICE_STATE: {
        PARKED: 'Parked',
        ON_ROAD: 'On Road',
    },
    USER_TYPE: {
        ADMINISTRATOR: 'ADMINISTRATOR',
        HQ: 'HQ',
        UNIT: 'UNIT',
        LICENSING_OFFICER: 'LICENSING OFFICER',
        MOBILE: 'MOBILE',
        CUSTOMER: 'CUSTOMER'
    },
    ROUTE_STATUS: {
        UNASSIGNED: 'UNASSIGNED',
        ASSIGNED: 'ASSIGNED',
    },
    DRIVER_STATUS: {
        UNASSIGNED: 'UNASSIGNED',
        ASSIGNED: 'ASSIGNED',
    },
    USER_APPOINT: {
        OIC: 'OIC',
        DY: 'DY',
        TCP: 'TCP',
    },
    BROADCAST_EVENT: {
        INCIDENT_UPDATE: 'incident_update',
        ROUTE_UPDATE: 'route_update',
        ETA_NOTICE: 'ETA_NOTICE',
        USER_ZONE_NOTICE: 'USER_ZONE_NOTICE',
        WAY_POINT_UPDATE: 'WAY_POINT_UPDATE',
        MOVEMENT_NOTICE: 'MOVEMENT_NOTICE',
    },
    WAY_POINT_STATE: {
        OFF_ROAD: 'Off Road',
        CROSSING_SITE: 'Crossing Site',
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
        CONVOY_STOP: 'convoy_stop',
        CONVOY_RESUME: 'convoy_resume',
        CONVOY_DELAY: 'convoy_delay',
        CONVOY_DELETED: 'convoy_deleted',
        UPLOAD_POSITION_FREQ: 'upload_position_freq',
        UPLOAD_INCIDENT_FREQ: 'upload_incident_freq',
        UPLOAD_PEER_UNIT_UPDATE: 'upload_peer_unit_update',
        NEW_NO_GO_ZONE: 'new_no_go_zone',
        UPDATE_NO_GO_ZONE: 'update_no_go_zone',
        DELETE_NO_GO_ZONE: 'delete_no_go_zone',
        SYS_CONF_UPDATE: 'sys_conf_update',
    },
    TO_VEHICLE_STATUS: {
        Deployed: "Deployed",
        Deployable: "Deployable",
        Under_Maintenance: "Under Maintenance",
        Out_Of_Service: "Out Of Service",
        LOAN_OUT: "Loan Out",
        // ON_HOLE: "On Hold"
        PENDING_WPT: "Pending WPT",
        PENDING_MPT: "Pending MPT",
    },
    TO_DRIVER_STATUS: {
        Deployed: "Deployed",
        Deployable: "Deployable",
        On_Leave: "On Leave",
        LOAN_OUT: "Loan Out",
        Invalid: "Permit Invalid",
    },
    USER_STATUS: {
        DISABLE: 0,
        ENABLE: 1,
        LOCK_OUT_PWD: 2,
        LOCK_OUT_90: 3,
        LOCK_OUT_180: 4
    },
    CV_USER_STATUS: {
        Active: 'Active',
        LockOut: 'Lock Out',
        Deactivated: 'Deactivated',
    },
}

