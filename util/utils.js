
module.exports.validateNRIC = function (nric) {
    if(typeof nric === "undefined" || nric === '' || nric === 'NULL' || null === nric) return false;
    let weights = [2,7,6,5,4,3,2];
    let alphabet = ["A","B","C","D","E","F","G","H","I","Z","J"];
    let strNric = nric.split('');
    if (typeof nric !== "string") return false;
    if(strNric.length !== 9) return false;
    if(strNric[0] !== "S" && strNric[0] !== "T" && strNric[0] !== "F" && strNric[0] !== "G") return false;
    let weightVal = weights[0]*strNric[1]+weights[1]*strNric[2]+weights[2]*strNric[3]+weights[3]*strNric[4]
        +weights[4]*strNric[5]+weights[5]*strNric[6]+weights[6]*strNric[7];
    let offset = (strNric[0] === "T" || strNric[0] === "G") ? 4:0;
    let val = 11 - (offset + weightVal)%11;
    return strNric[8] === alphabet[val - 1];
};

module.exports.pointDistance = function (lat, lng, x, y) {
    let dx = lng - x;
    let dy = lat - y;
    return dx*dx + dy*dy;
};

module.exports.wait = async function (time) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve();
        }, time);
    });
};

module.exports.isTimeOverlap = function (startTime1, endTime1, startTime2, endTime2) {
    if (startTime1 >= endTime2 || startTime2 >= endTime1) {
        return false;
    } else {
        return true;
    }
}

module.exports.isPointInPolygon = function (point, polygon) {
    let x = point[0], y = point[1];

    let inside = false;
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
        let xi = polygon[i][0], yi = polygon[i][1];
        let xj = polygon[j][0], yj = polygon[j][1];

        let intersect = (( yi > y ) != ( yj > y )) &&
            (x < ( xj - xi ) * ( y - yi ) / ( yj - yi ) + xi);
        if (intersect) inside = !inside;
    }

    return inside;
}
