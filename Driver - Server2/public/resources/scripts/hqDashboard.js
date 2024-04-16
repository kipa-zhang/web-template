$(() => {
	let iframe = document.getElementById('resource-iframe');
	iframe.onload = function() {
		console.log(` Iframe will jump to => ` + iframe.contentWindow.location.pathname)
		if (iframe.contentWindow.location.pathname == '/login') {
			window.parent.location = '/login'
		}
	}
	
	$('.tab-label').off('click').on('click', function () {
		$('.tab-label').removeClass('active');
		$('.action-button').hide();
		$(this).addClass('active');

		let tab = $(this).data('tab');
		if (tab == 1) {
			$('iframe').attr('src', './dashboard/overview');
		} else if (tab == 2) {
			$('iframe').attr('src', './dashboard/credit');
		} else {
			console.log(`Tab ${ tab } does not exist now.`)
		}
	})
})