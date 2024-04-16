/**
 * @param data:  response data
 * @param recordsTotal:  total entries
 * @param res:  response
 */
module.exports.success = function(res, data, recordsTotal=null){
    let success = {
        "code": 1,
        "msg": 'success',
        "data": data
    };
    /**
     * recordsTotal: total entries
     * recordsFiltered: filtered from _MAX_ total entries
     */
    if(recordsTotal != null){
        success.recordsTotal = recordsTotal;
        success.recordsFiltered = recordsTotal;
    }
    return res.json(success);
}

module.exports.error = function(res, msg, code=null){
    return res.json({
        "code": code? code: 0,
        "msg": msg,
        "data": ""
    });
}