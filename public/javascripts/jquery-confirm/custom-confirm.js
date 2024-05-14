function simplyAlert(content, type) {
    $.alert({
        title: 'Alert',
        content: content,
        type: type ? type : 'green',
    });
}

function simplyError(content) {
    $.alert({
        title: 'Encountered an error',
        content: content,
        type: 'red',
    });
}

function simplyConfirm(content, callback) {
    $.confirm({
        title: 'Confirm',
        content: content,
        type: 'dark',
        buttons: {
            cancel: function () {
                //close
            },
            confirm: {
                btnClass: 'btn-system',
                action: function () {
                    return callback()
                },
            },
        },
    });
}

function simplyForm1(content, callback) {
    $.confirm({
        title: 'Confirm',
        content: content,
        type: 'dark',
        buttons: {
            cancel: function () {
                //close
            },
            confirm: {
                btnClass: 'btn-system',
                action: function () {
                    return callback(this)
                },
            },
        },
        onContentReady: function () {
            // bind to events
            var jc = this;
            this.$content.find('form').on('submit', function (e) {
                jc.$confirm.trigger('click'); // reference the button and click it
            });
        }
    });
}

function simplyRemarks(title, content, onContentReady, callback) {
    $.confirm({
        title: title,
        content: content,
        type: 'dark',
        buttons: {
            cancel: function () {
                //close
            },
            confirm: {
                btnClass: 'btn-system',
                action: function () {
                    return callback(this)
                },
            },
        },
        onContentReady: function () {
            return onContentReady(this)
        }
    });
}

function simplyForm(content, onContentReady, callback) {
    $.confirm({
        title: 'Confirm',
        content: content,
        type: 'dark',
        buttons: {
            cancel: function () {
                //close
            },
            confirm: {
                btnClass: 'btn-system',
                action: function () {
                    return callback(this)
                },
            },
        },
        onContentReady: function () {
            return onContentReady(this)
        }
    });
}


const changeNav = function (target) {
    let topWindow = $(window.parent.document);
    topWindow.find('.nav-item').removeClass('active')
    topWindow.find('.navbar-nav>.nav-item').each(function () {
        if ($(this).data('target') === target) {
            $(this).addClass('active')
        }
    })
}

function continueNewTrip(title, content, onContentReady, callback) {
    $.confirm({
        title: title,
        content: content,
        type: 'dark',
        buttons: {
            "No": function () {
                //close
            },

            "Add New Trip": {
                btnClass: 'btn-system ms-4',
                action: function () {
                    return callback(this)
                },
            },
        },
        onContentReady: function () {
            return onContentReady(this)
        }
    });
}