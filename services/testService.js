
const axios = require('axios');
const test = function () {
    try {
        axios.post('http://localhost:10000/publicFirebaseNotification', {
            "targetList": [
                {
                    "token": "AAAAAAAAAA",
                    "taskId": "T1",
                    "driverId": 1,
                    "vehicleNo": "V1"
                },
                {
                    "token": "BBBBBBB",
                    "taskId": "T2",
                    "driverId": 2,
                    "vehicleNo": "V2"
                }
            ],
            "title": "Test",
            "content": "Hello, world!"
        }).then(result => {
            console.log(result.data)
        })
    } catch (error) {
        console.log(11)
        console.error(error)
    }
}
test();