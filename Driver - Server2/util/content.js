module.exports = {
    INCIDENT_STATUS: {
        NEW: 'NEW',
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
    DEVICE_STATE: {
        PARKED: 'Parked',
        ON_ROAD: 'On Road',
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
        SYS_CONF_UPDATE: 'sys_conf_update'
    },
    USER_TYPE: {
        CA: 'CA',
        ACECOM: 'ACECOM',
        MOBILE: 'MOBILE'
    },
    USER_APPOINT: {
        OIC: 'OIC',
        DY: 'DY',
    },
    DRIVER_STATUS: {
        EMPTY: "",
        UNASSIGNED: "Unassigned",
        ASSIGNED: "Assigned",
        COMPLETED: "Completed",
        NOSHOW: "No Show",
        NOSHOWSYSTEM: "No Show (System)",
        ARRIVED: "Arrived",
        DEPARTED: "Started",
        LATE: "Late Trip",
    },
};