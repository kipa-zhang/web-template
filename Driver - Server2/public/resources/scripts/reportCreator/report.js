let clickType = null;
$(() => {
    for(let item of $('.report-menu')){
        let data = $(item).attr('data-link')
        if(data != 'task') $(`.${ data }-image-white`).hide()
    }

    $('.report-menu').on('click', function () {
        $('.report-menu').removeClass('active')
        $(this).addClass('active')

        let link = `/reportCreator/${ $(this).attr('data-link') }`
        $('.report-iframe').attr('src', link)
        clickType = $(this).attr('data-link')
        $(`.${ $(this).attr('data-link') }-image`).hide()
        $(`.${ $(this).attr('data-link') }-image-white`).show()
        for(let item of $('.report-menu')){
            let data = $(item).attr('data-link')
            if(clickType != data) {
                $(`.${ data }-image`).show()
                $(`.${ data }-image-white`).hide()
            } 
        }
    })
   
    $('.report-menu').on("mouseover", function() {
        let data = $(this).attr('data-link');
        $(`.${ data }-image`).hide()
        $(`.${ data }-image-white`).show()
      }).on("mouseout", function() {
        let data = $(this).attr('data-link');
        if(data != clickType) {
            $(`.${ data }-image`).show()
            $(`.${ data }-image-white`).hide()
        }
      });
})

function showLoading () {
    $('.custom-loading').show()
}
function closeLoading () {
    $('.custom-loading').hide()
}