const axios = require('axios');
const fetch = require('node-fetch');

/**
 * API: https://api.surveymonkey.net/v3/docs
 * Private APP: https://developer.surveymonkey.com/
 */

const accessToken = 'gwIkzT39rNtoaYdrzi-0v.8p2LupakUS0bHfUhe1catQ8y55MxxEjMCjhEauvgT5zk-Caww.36EN8m5mNYiO-v8mJgDKZyT2WwKjXl9HgbuLcSdL6iu1tsV5-rrqnJws'

const testGetAllSurvey = async function () {
	axios.get('https://api.surveymonkey.com/v3/surveys', {
		'headers': {
			'Accept': 'application/json',
			'Authorization': `Bearer ${ accessToken }`
		  }
	}).then(response => {
		let data = response.data
		console.log(data);
	}).catch(err => {
		console.error(err);
	});
}
testGetAllSurvey();

const testGetSurveyResponse = async function (surveyId) {
	axios.get(`https://api.surveymonkey.com/v3/surveys/${ surveyId }/responses/bulk`, {
		'headers': {
			'Accept': 'application/json',
			'Authorization': `Bearer ${ accessToken }`
		  }
	}).then(response => {
		let resultList = [];

		// Every response
		for (let data of response.data.data) {
			// Every answer
			for (let page of data.pages) {
				let result = {}
				console.log('***************************');
				for (let question of page.questions) {
					console.log(question);
					let answers = question.answers;
					console.log('------ start ------');
					switch (question.id) {
						case '84667557': 
							result.username = answers[0].text;
							break;
						case '84676831': 
							result.likeColor = answers[0].text;
							break;
						case '84677025': 
							result.rich = answers[0].text;
							break;
						case '84677170': 
							// Here may have many things
							result.thingsHave = answers;
							break;
						case '84677241': 
							result.birthday = answers[0].text;
							break;
					}
					console.log('------  end  ------');
				}
				resultList.push(result)
			}
		}
	}).catch(err => {
		console.error(err);
	});
}
// testGetSurveyResponse(507618636);


const testGetSurveyDetail = async function (surveyId) {
	axios.get(`https://api.surveymonkey.com/v3/surveys/${ surveyId }/details`, {
		'headers': {
			'Accept': 'application/json',
			'Authorization': `Bearer ${ accessToken }`
		  }
	}).then(response => {
		// console.log(response.data)
		let questionIdList = [];
		for (let page of response.data.pages) {
			// console.log(page.questions)
			// Every response url
			for (let question of page.questions) {
				console.log(question)
				let question = { id: question.id }

				switch (question.family) {
					case 'single_choice':

						break;
				}


				questionIdList.push(question)
				console.log('*********************')
			}
		}
	}).catch(err => {
		console.error(err);
	});

}
// testGetSurveyDetail(507618636);

const testCreateSurvey = async function () {
	const body = {
		"title": "Example Survey",
		"pages": [
		  {
			"title": "My First Page",
			"description": "Page description",
			"position": 1,
			"questions": [
			  {
				"family": "single_choice",
				"subtype": "vertical",
				"answers": {
				  "choices": [
					{
					  "text": "Apple",
					  "position": 1
					},
					{
					  "text": "Orange",
					  "position": 2
					},
					{
					  "text": "Banana",
					  "position": 3
					}
				  ]
				},
				"headings": [
				  {
					"heading": "What is your favourite fruit?"
				  }
				],
				"position": 1
			  }
			]
		  }
		]
	  }
	fetch("https://api.surveymonkey.com/v3/surveys", {
		"method": "POST",
		"headers": {
		  "Content-Type": "application/json",
		  "Accept": "application/json",
		  "Authorization": `Bearer ${ accessToken }`
		},
		"body": JSON.stringify(body)
	  })
	  .then(response => {
		console.log(response);
	  })
	  .catch(err => {
		console.error(err);
	  });
}
// testCreateSurvey()







const testGetSurveyResponse2 = async function (surveyId) {
	axios.get(`https://api.surveymonkey.com/v3/surveys/${ surveyId }/responses`, {
		'headers': {
			'Accept': 'application/json',
			'Authorization': `Bearer ${ accessToken }`
		  }
	}).then(response => {
		// Every response
		for (let data of response.data.data) {
			console.log(response.data)
			// Every response url
			// for (let responseUrl of data.data) {
			// 	console.log(JSON.stringify(responseUrl))
			// }
		}
	}).catch(err => {
		console.error(err);
	});

}
// testGetSurveyResponse2(507618636);

const testGetSurveyResponseDetail = async function (surveyId, responseId) {
	axios.get(`https://api.surveymonkey.com/v3/surveys/${ surveyId }/responses/${ responseId }/details`, {
		'headers': {
			'Accept': 'application/json',
			'Authorization': `Bearer ${ accessToken }`
		  }
	}).then(response => {
		console.log(response.data)
		// Every response
		for (let page of response.data.pages) {
			console.log(page.questions)
			// Every response url
			for (let question of page.questions) {
				console.log(question)
			}
		}
	}).catch(err => {
		console.error(err);
	});

}
// testGetSurveyResponseDetail(507618636, 118096986520);