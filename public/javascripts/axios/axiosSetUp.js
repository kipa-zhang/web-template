/**
 * axios global error handle
 */
axios.interceptors.response.use(
    response => {
        console.log(response)
        
        return response
    }, err => {
        console.error(err);
        if (err.response.status == 500) {
            simplyAlert(err.message, 'red');
        } else {
            return Promise.reject(err);
        }
    }
)